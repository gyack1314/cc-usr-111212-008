"""主体撤回的行级影响规划、重建、屏障与清除账测试。

覆盖任务要求的四个验证入口：双主体行、共享聚合、缺边、撤回中并发读取，
另加环/未知转换阻塞、filter/union/group-sum 语义、幂等与过期计划、
副本清除回执与保留例外。
"""
import copy
import json
import unittest
from pathlib import Path

from lake_data.io import canonical, digest, load
from lake_data.withdraw import BarrierStale, Lakehouse, WithdrawBlocked

DATA = Path(__file__).resolve().parent.parent / 'data'


def lake_of(name):
    return Lakehouse(load(DATA / name)[0])


def by_id(rows):
    return {r['row_id']: r for r in rows}


class DoubleSubjectRowTests(unittest.TestCase):
    """双主体行：命中任一主体即整行撤除，不拆出另一主体的部分。"""

    def test_shared_subject_row_removed_whole(self):
        lake = lake_of('shared-raw-row.json')
        plan = lake.plan_withdraw('w-person3', ['person3'])
        # c 同时属于 person1/person3，person3 撤回 -> c 整行撤除
        self.assertEqual(plan['hit_raw_rows'], ['c'])
        actions = {a['row_id']: a for a in plan['affected_rows']}
        self.assertIn('c', actions)
        self.assertEqual(actions['c']['action'], 'removed')
        self.assertIn('整行撤除', actions['c']['reason'])
        # 下游 pc 撤除；south 组失去唯一父贡献 -> 空组消失
        self.assertEqual(actions['pc']['action'], 'removed')
        self.assertEqual(actions['gs']['action'], 'removed')
        # 北部聚合与 person1/person2 的行完全不在影响闭包内
        self.assertNotIn('gn', plan['affected_row_ids'])
        self.assertTrue(plan['unaffected_unchanged'])
        self.assertEqual(set(plan['unaffected_row_ids']),
                         {'a', 'b', 'pa', 'pb', 'gn'})
        result = lake.commit(plan)
        new_ids = {r['row_id'] for r in lake.manifest['rows']}
        self.assertEqual(new_ids, {'a', 'b', 'pa', 'pb', 'gn'})
        # person1 的贡献仍然完整存在，没有被连带撤除
        self.assertEqual(by_id(lake.manifest['rows'])['gn']['values']['amount'], 30)
        self.assertEqual(result['committed']['withdraw_epoch_after'], 1)


class SharedAggregationTests(unittest.TestCase):
    """共享聚合：撤除部分贡献后重算，组仍有其他父行则保留。"""

    def test_group_sum_recomputed_with_before_after(self):
        lake = lake_of('normal.json')
        plan = lake.plan_withdraw('withdraw1', ['person1'])
        # person1 命中 a 与双主体行 c
        self.assertEqual(plan['hit_raw_rows'], ['a', 'c'])
        actions = {a['row_id']: a for a in plan['affected_rows']}
        # gn 保留但排除 pa 的贡献：30 -> 20
        gn = actions['gn']
        self.assertEqual(gn['action'], 'recomputed')
        self.assertEqual(gn['before_values']['amount'], 30)
        self.assertEqual(gn['after_values']['amount'], 20)
        self.assertEqual(gn['parents_before'], ['pa', 'pb'])
        self.assertEqual(gn['parents_after'], ['pb'])
        self.assertEqual(gn['group_key'], {'team': 'north'})
        self.assertEqual(gn['removed_contributions'],
                         [{'parent': 'pa', 'value': 10}])
        self.assertEqual(gn['contributions_after'],
                         [{'parent': 'pb', 'value': 20}])
        # 受影响行都带完整血缘路径
        self.assertIn(['gn', 'pa', 'a'], gn['lineage_paths'])
        self.assertIn(['gn', 'pb', 'b'], gn['lineage_paths'])
        # south 组唯一贡献被撤除 -> 组消失
        self.assertEqual(actions['gs']['action'], 'removed')
        result = lake.commit(plan)
        rows = by_id(lake.manifest['rows'])
        # 重建后的来源边只指向仍有效父行
        self.assertEqual(rows['gn']['parents'], ['pb'])
        self.assertNotIn('pa', rows)
        self.assertNotIn('gs', rows)
        # 未受影响行逐字节保持不变
        old = by_id(load(DATA / 'normal.json')[0]['rows'])
        for rid in plan['unaffected_row_ids']:
            self.assertEqual(canonical(old[rid]), canonical(rows[rid]))
        self.assertEqual(result['new_release'], 'er0-wd')
        self.assertEqual(lake.manifest['withdraw_epoch'], 1)


class MissingEdgeTests(unittest.TestCase):
    """缺边/环/未知转换：必须阻塞，不能只删可见几层就报完成。"""

    def test_missing_parent_blocks(self):
        lake = lake_of('lineage-gap.json')
        with self.assertRaises(WithdrawBlocked) as ctx:
            lake.plan_withdraw('withdraw1', ['person1'])
        codes = {b['code'] for b in ctx.exception.blockers}
        self.assertIn('missing-parent', codes)
        detail = ' '.join(b['detail'] for b in ctx.exception.blockers
                          if b['code'] == 'missing-parent')
        self.assertIn('missing', detail)
        # 阻塞时清单原封不动
        self.assertEqual(lake.manifest['release'], 'er0')
        self.assertEqual(len(lake.manifest['rows']), 8)

    def test_unsupported_transform_blocks(self):
        lake = lake_of('invalid-config.json')
        with self.assertRaises(WithdrawBlocked) as ctx:
            lake.plan_withdraw('withdraw1', ['person1'])
        self.assertIn('unsupported-transform',
                      {b['code'] for b in ctx.exception.blockers})

    def test_cycle_blocks(self):
        v = copy.deepcopy(load(DATA / 'normal.json')[0])
        v['rows'][3]['parents'] = ['gn']  # pa -> gn -> pa 成环
        lake = Lakehouse(v)
        with self.assertRaises(WithdrawBlocked) as ctx:
            lake.plan_withdraw('w', ['person1'])
        self.assertIn('lineage-cycle',
                      {b['code'] for b in ctx.exception.blockers})

    def test_unknown_parent_transform_blocks(self):
        v = copy.deepcopy(load(DATA / 'normal.json')[0])
        v['rows'][3]['transform'] = 'ghost'
        lake = Lakehouse(v)
        with self.assertRaises(WithdrawBlocked) as ctx:
            lake.plan_withdraw('w', ['person1'])
        self.assertIn('unknown-transform',
                      {b['code'] for b in ctx.exception.blockers})


class ConcurrentReadTests(unittest.TestCase):
    """撤回屏障：先校验再读、返回前重验，旧句柄不得返回含主体的旧行。"""

    def test_reader_barrier_lifecycle(self):
        lake = lake_of('normal.json')
        old_reader = lake.open_reader('reader-before')
        # 提交前读取正常
        self.assertEqual(len(old_reader.read_rows()), 8)
        plan = lake.plan_withdraw('withdraw1', ['person1'])
        lake.commit(plan)
        # 旧读句柄从撤回提交起不得再返回旧行
        with self.assertRaises(BarrierStale):
            old_reader.read_rows()
        # 新句柄视图已切换，不含 person1 命中的任何 raw 行
        new_reader = lake.open_reader('reader-after')
        rows = by_id(new_reader.read_rows())
        self.assertNotIn('a', rows)
        self.assertNotIn('c', rows)
        self.assertEqual(rows['gn']['values']['amount'], 20)
        for r in rows.values():
            if r['kind'] == 'raw':
                self.assertNotIn('person1', r['subjects'])

    def test_inflight_read_revalidates_before_return(self):
        lake = lake_of('normal.json')
        reader = lake.open_reader('reader-inflight')
        plan_holder = {}

        def concurrent_withdraw():
            # 模拟读取途中另一事务完成撤回提交
            p = lake.plan_withdraw('withdraw1', ['person1'])
            plan_holder['p'] = p
            lake.commit(p)

        with self.assertRaises(BarrierStale):
            reader.read_rows(concurrent_commit=concurrent_withdraw)
        self.assertEqual(lake.epoch, 1)

    def test_retry_after_stale_succeeds(self):
        lake = lake_of('normal.json')
        reader = lake.open_reader('r1')
        lake.commit(lake.plan_withdraw('withdraw1', ['person1']))
        with self.assertRaises(BarrierStale):
            reader.read_rows()
        retried = lake.open_reader('r1-retry')
        self.assertEqual(len(retried.read_rows()), 3)


class FilterUnionGroupSemanticsTests(unittest.TestCase):
    """四类转换的重建语义与全 null/空组契约。"""

    def _base(self):
        return {
            'release': 'er0',
            'rows': [
                {'row_id': 'a', 'kind': 'raw', 'subjects': ['p1'],
                 'values': {'team': 'north', 'amount': 10}, 'parents': []},
                {'row_id': 'b', 'kind': 'raw', 'subjects': ['p2'],
                 'values': {'team': 'south', 'amount': 7}, 'parents': []},
                {'row_id': 'c', 'kind': 'raw', 'subjects': ['p3'],
                 'values': {'team': 'south', 'amount': None}, 'parents': []},
            ],
            'transforms': [
                {'id': 'f', 'kind': 'filter',
                 'where': {'column': 'team', 'value': 'north'}, 'version': 'f1'},
                {'id': 'pr', 'kind': 'project',
                 'columns': ['team', 'amount'], 'version': 'pr1'},
                {'id': 'u', 'kind': 'union', 'version': 'u1'},
                {'id': 'g', 'kind': 'group-sum',
                 'group': ['team'], 'value': 'amount', 'version': 'g1'},
            ],
            'copies': [], 'readers': [],
            'request': {'request_id': 'w', 'subjects': ['p1']},
        }

    def test_filter_dropped_when_parent_removed(self):
        v = self._base()
        v['rows'].append({'row_id': 'fa', 'kind': 'derived', 'transform': 'f',
                          'parents': ['a'], 'values': dict(v['rows'][0]['values'])})
        lake = Lakehouse(v)
        plan = lake.plan_withdraw('w', ['p1'])
        actions = {a['row_id']: a for a in plan['affected_rows']}
        self.assertEqual(actions['fa']['action'], 'removed')

    def test_filter_rebuild_predicate_branches(self):
        # 直接驱动重建函数，精确覆盖谓词通过/失败两个分支
        v = self._base()
        lake = Lakehouse(v)
        rows = {r['row_id']: r for r in v['rows']}
        t = {x['id']: x for x in v['transforms']}
        old = {'row_id': 'fx', 'kind': 'derived', 'transform': 'f',
               'parents': ['a'], 'values': {'team': 'north', 'amount': 10}}
        # 父行存活且满足常量 -> 保留，来源边指向有效父行
        rebuilt = lake._rebuild_row('fx', old, t, rows)
        self.assertEqual(rebuilt['parents'], ['a'])
        self.assertEqual(rebuilt['values'], {'team': 'north', 'amount': 10})
        # 父行存活但不再等于过滤常量 -> 过滤不通过，衍生行删除
        rows['a']['values']['team'] = 'south'
        self.assertIsNone(lake._rebuild_row('fx', old, t, rows))
        # 父行全部撤除 -> None（空输入）
        self.assertIsNone(lake._rebuild_row('fx', old, t, {}))

    def test_project_rebuilds_columns_only(self):
        v = self._base()
        lake = Lakehouse(v)
        t = {x['id']: x for x in v['transforms']}
        old = {'row_id': 'pa', 'kind': 'derived', 'transform': 'pr',
               'parents': ['a'], 'values': {'team': 'north', 'amount': 10}}
        rebuilt = lake._rebuild_row('pa', old, t,
                                    {r['row_id']: r for r in v['rows']})
        self.assertEqual(set(rebuilt['values']), {'team', 'amount'})

    def test_union_keeps_bag_multiplicity(self):
        v = self._base()
        # 两个 union 槽行同值不同 row_id，袋重数=2；同值不去重
        v['rows'].extend([
            {'row_id': 'ub1', 'kind': 'derived', 'transform': 'u',
             'parents': ['b'], 'values': {'team': 'south', 'amount': 7}},
            {'row_id': 'ub2', 'kind': 'derived', 'transform': 'u',
             'parents': ['b'], 'values': {'team': 'south', 'amount': 7}},
        ])
        lake = Lakehouse(v)
        plan = lake.plan_withdraw('w', ['p2'])
        ids = {r['row_id'] for r in plan['new_rows']}
        # b 撤除，两个槽行各自失去父行后一并撤除，无分区整体清空
        self.assertNotIn('b', ids)
        self.assertNotIn('ub1', ids)
        self.assertNotIn('ub2', ids)
        # 同值两行在原清单中本就并存（袋语义，不按值去重）
        self.assertEqual(len([r for r in v['rows']
                              if r.get('transform') == 'u']), 2)

    def test_group_sum_all_null_group_survives_then_empty_disappears(self):
        # 场景一：撤 b 保留 null 父行 c -> 全 null 组 sum=null，组行保留
        v = self._base()
        v['rows'].append({'row_id': 'gb', 'kind': 'derived', 'transform': 'g',
                          'parents': ['b', 'c'],
                          'values': {'team': 'south', 'amount': 7}})
        lake = Lakehouse(v)
        plan = lake.plan_withdraw('w', ['p2'])
        gb = {a['row_id']: a for a in plan['affected_rows']}['gb']
        self.assertEqual(gb['action'], 'recomputed')
        self.assertIsNone(gb['after_values']['amount'])
        self.assertEqual(gb['before_values']['amount'], 7)
        self.assertEqual(gb['parents_after'], ['c'])
        lake.commit(plan)
        self.assertIsNone(by_id(lake.manifest['rows'])['gb']
                          ['values']['amount'])

        # 场景二：b 与 c 同时撤除 -> 已无任何父贡献，空组消失
        v2 = self._base()
        v2['rows'].append({'row_id': 'gb', 'kind': 'derived', 'transform': 'g',
                           'parents': ['b', 'c'],
                           'values': {'team': 'south', 'amount': 7}})
        lake2 = Lakehouse(v2)
        plan2 = lake2.plan_withdraw('w', ['p2', 'p3'])
        actions2 = {a['row_id']: a for a in plan2['affected_rows']}
        self.assertEqual(actions2['gb']['action'], 'removed')
        lake2.commit(plan2)
        self.assertNotIn('gb', by_id(lake2.manifest['rows']))


class ClearanceLedgerTests(unittest.TestCase):
    """清除账：pending 阻塞、回执核验、人工例外、未知副本不承诺。"""

    def _settled_lake(self, status_overrides):
        v = copy.deepcopy(load(DATA / 'normal.json')[0])
        digest_before = 'a' * 64
        copies = []
        for c in v['copies']:
            c = copy.deepcopy(c)
            override = status_overrides[c['copy_id']]
            c['status'] = override['status']
            if override['status'] == 'deleted':
                c['receipt'] = {'copy_id': c['copy_id'],
                                'release_digest': digest_before,
                                'confirmed_at': '2026-11-12T10:00:00Z'}
            if override['status'] == 'exception':
                c['exception'] = {'reason': '监管留存要求',
                                  'scope': '2026Q4 审计冻结区'}
            copies.append(c)
        v['copies'] = copies
        return Lakehouse(v)

    def test_pending_copies_block_scope_complete(self):
        lake = lake_of('normal.json')
        plan = lake.plan_withdraw('withdraw1', ['person1'])
        ledger = plan['clearance']
        self.assertFalse(ledger['scope_complete'])
        self.assertEqual(ledger['historical_copies']['status'], 'blocked')
        for rep in ledger['historical_copies']['copies']:
            self.assertTrue(rep['in_scope'])
            if rep['status'] == 'pending':
                self.assertEqual(rep['settlement'], 'blocking')
        # 三段分别报告
        self.assertEqual(ledger['logical_rebuild']['status'], 'rebuilt')
        self.assertEqual(ledger['read_barrier']['status'], 'armed-on-commit')
        self.assertIn('未知副本不承诺清除',
                      ledger['historical_copies']['unknown_copies'])

    def test_verified_receipts_and_exceptions_complete(self):
        lake = self._settled_lake({
            'old-file': {'status': 'deleted'},
            'export-1': {'status': 'exception'},
        })
        plan = lake.plan_withdraw('withdraw1', ['person1'])
        ledger = plan['clearance']
        self.assertTrue(ledger['scope_complete'])
        reports = {c['copy_id']: c for c in ledger['historical_copies']['copies']}
        self.assertEqual(reports['old-file']['settlement'], 'verified')
        self.assertEqual(reports['old-file']['receipt']['copy_id'], 'old-file')
        self.assertEqual(reports['export-1']['settlement'],
                         'exception-registered')
        self.assertEqual(reports['export-1']['exception']['reason'],
                         '监管留存要求')

    def test_bad_receipt_blocks(self):
        lake = self._settled_lake({
            'old-file': {'status': 'deleted'},
            'export-1': {'status': 'pending'},
        })
        # 破坏回执结构
        lake.manifest['copies'][0]['receipt']['confirmed_at'] = ''
        with self.assertRaises(WithdrawBlocked) as ctx:
            lake.plan_withdraw('withdraw1', ['person1'])
        self.assertIn('receipt-invalid',
                      {b['code'] for b in ctx.exception.blockers})

    def test_unaffected_copy_marked(self):
        v = copy.deepcopy(load(DATA / 'normal.json')[0])
        v['copies'].append({'copy_id': 'clean-export', 'rows': ['b'],
                            'status': 'pending'})
        lake = Lakehouse(v)
        plan = lake.plan_withdraw('withdraw1', ['person1'])
        reports = {c['copy_id']: c for c
                   in plan['clearance']['historical_copies']['copies']}
        self.assertFalse(reports['clean-export']['in_scope'])
        self.assertEqual(reports['clean-export']['settlement'], 'unaffected')


class IdempotencyAndBindingTests(unittest.TestCase):
    """重复请求复用、新 release 输入变化后拒绝过期计划。"""

    def test_duplicate_request_reuses_completed_result(self):
        lake = lake_of('normal.json')
        first = lake.commit(lake.plan_withdraw('withdraw1', ['person1']))
        again = lake.plan_withdraw('withdraw1', ['person1'])
        self.assertEqual(again['binding']['input_digest'],
                         first['binding']['input_digest'])
        self.assertEqual(again['new_release'], first['new_release'])
        self.assertEqual(again['affected_row_ids'], first['affected_row_ids'])
        # 复用不再次推进 epoch
        self.assertEqual(lake.epoch, 1)

    def test_changed_input_rejects_stale_plan(self):
        lake = lake_of('normal.json')
        plan = lake.plan_withdraw('withdraw1', ['person1'])
        lake.commit(plan)
        # 新 release 改变输入（新增一行），旧 request_id 必须重新规划
        lake.manifest['rows'].append({
            'row_id': 'd', 'kind': 'raw', 'subjects': ['person4'],
            'values': {'team': 'east', 'amount': 1}, 'parents': []})
        with self.assertRaises(WithdrawBlocked) as ctx:
            lake.plan_withdraw('withdraw1', ['person1'])
        self.assertIn('stale-plan',
                      {b['code'] for b in ctx.exception.blockers})

    def test_commit_rejects_plan_bound_to_other_input(self):
        lake = lake_of('normal.json')
        plan = lake.plan_withdraw('withdraw1', ['person1'])
        lake.manifest['release'] = 'er9'  # 提交前 release 已被别的事务改变
        with self.assertRaises(WithdrawBlocked) as ctx:
            lake.commit(plan)
        self.assertIn('stale-plan',
                      {b['code'] for b in ctx.exception.blockers})
        self.assertEqual(lake.epoch, 0)

    def test_binding_contains_full_lineage_and_versions(self):
        lake = lake_of('normal.json')
        plan = lake.plan_withdraw('withdraw1', ['person1'])
        binding = plan['binding']
        self.assertEqual(binding['release'], 'er0')
        self.assertEqual(binding['request_id'], 'withdraw1')
        self.assertEqual(binding['subjects'], ['person1'])
        self.assertEqual(len(binding['row_lineage']), 8)
        self.assertEqual(binding['row_lineage']['gn']['parents'],
                         ['pa', 'pb'])
        self.assertEqual(binding['transform_versions']['g']['version'], 'g1')
        self.assertEqual(len(binding['input_digest']), 64)

    def test_no_overwrite_claim_and_no_anonymization(self):
        # 模块只报告逻辑清除，不声称物理字节消失
        lake = lake_of('normal.json')
        plan = lake.plan_withdraw('withdraw1', ['person1'])
        self.assertIn('不代表历史物理字节消失',
                      plan['clearance']['logical_rebuild']['note'])


class CliTests(unittest.TestCase):
    """CLI 冒烟：plan 阻塞退出码与成功输出。"""

    def test_cli_plan_blocked(self):
        import subprocess, sys
        r = subprocess.run(
            [sys.executable, '-m', 'lake_data.cli', 'plan',
             str(DATA / 'lineage-gap.json'),
             '--request-id', 'w', '--subject', 'person1'],
            cwd=DATA.parent, capture_output=True, text=True)
        self.assertEqual(r.returncode, 2)
        payload = json.loads(r.stdout)
        self.assertEqual(payload['status'], 'blocked')
        self.assertIn('missing-parent',
                      {b['code'] for b in payload['blockers']})

    def test_settled_fixture_completes_scope(self):
        lake = lake_of('clearance-settled.json')
        plan = lake.plan_withdraw('withdraw1', ['person1'])
        self.assertTrue(plan['clearance']['scope_complete'])

    def test_cli_plan_commit_read_golden_path(self):
        import subprocess, sys, tempfile
        with tempfile.TemporaryDirectory() as d:
            plan_path = Path(d) / 'plan.json'
            release_path = Path(d) / 'release1.json'
            r1 = subprocess.run(
                [sys.executable, '-m', 'lake_data.cli', 'plan',
                 str(DATA / 'normal.json'), '--request-id', 'withdraw1',
                 '--subject', 'person1', '--out', str(plan_path)],
                cwd=DATA.parent, capture_output=True, text=True)
            self.assertEqual(r1.returncode, 0, r1.stderr)
            r2 = subprocess.run(
                [sys.executable, '-m', 'lake_data.cli', 'commit',
                 str(plan_path), '--manifest', str(DATA / 'normal.json'),
                 '--out', str(release_path)],
                cwd=DATA.parent, capture_output=True, text=True)
            self.assertEqual(r2.returncode, 0, r2.stderr)
            committed = json.loads(r2.stdout)
            self.assertTrue(committed['committed']['atomic_switch'])
            r3 = subprocess.run(
                [sys.executable, '-m', 'lake_data.cli', 'read',
                 str(release_path), '--reader-id', 'after'],
                cwd=DATA.parent, capture_output=True, text=True)
            self.assertEqual(r3.returncode, 0, r3.stderr)
            view = json.loads(r3.stdout)
            self.assertEqual(view['withdraw_epoch'], 1)
            self.assertEqual([r['row_id'] for r in view['rows']],
                             ['b', 'pb', 'gn'])


if __name__ == '__main__':
    unittest.main()
