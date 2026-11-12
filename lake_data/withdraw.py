"""主体撤回的行级影响规划、衍生物重建与读屏障。

设计口径见 docs/data-contract.md：
- 主体命中任一 raw 行即撤除整行，不拆出其余主体部分；
- 影响沿父边向下游传播，但衍生行按 filter/project/union/group-sum
  各自语义重算，只有失去全部有效父行（或不再满足过滤/分组消失）才删除；
- 计划绑定 release、转换版本、全部输入摘要与完整行血缘；
- 提交产生新 release，行视图与来源边在一次赋值内切换，并递增
  withdraw_epoch 作为读屏障；
- 旧文件不覆写；清除账区分逻辑重建、读屏障与历史副本清理。
本模块只做本地逻辑可验证的判断，不声称物理字节已全部消失。
"""
from copy import deepcopy

from .io import canonical, digest
from .lineage import topological_order, validate_manifest


class BarrierStale(RuntimeError):
    """读句柄的 withdraw_epoch 已落后，缓冲结果不得返回，需按新句柄重试。"""


class WithdrawBlocked(RuntimeError):
    """规划或提交被阻塞清单拦截；调用方应处理 blockers 而不是部分生效。"""

    def __init__(self, blockers):
        super().__init__(f'撤回被 {len(blockers)} 项阻塞')
        self.blockers = blockers


def _sum_integers(parts):
    """定标整数求和；全 null 组为 null（契约）。parts 已校验为 int 或 None。"""
    seen_value = False
    total = 0
    for x in parts:
        if x is None:
            continue
        seen_value = True
        total += x
    return total if seen_value else None


class Lakehouse:
    """持有当前清单的湖仓状态机，管理撤回 epoch 与请求幂等缓存。"""

    def __init__(self, manifest, clock=None):
        self.manifest = deepcopy(manifest)
        self.epoch = int(manifest.get('withdraw_epoch', 0))
        # 每个 request_id 完成后的结果缓存，用于重复请求复用
        self._completed = {}
        self._clock = clock
        self._tick = 0

    def now(self):
        if self._clock is not None:
            return self._clock()
        self._tick += 1
        return f'epoch-{self.epoch}-tick-{self._tick}'

    # ---- 规划 ----------------------------------------------------------

    def plan_withdraw(self, request_id, subjects):
        subjects = tuple(sorted(set(subjects)))
        if not subjects:
            raise WithdrawBlocked([{'code': 'empty-subjects',
                                    'detail': '撤回主体集合为空'}])

        rows_by_id, transforms_by_id, blockers = validate_manifest(self.manifest)
        if blockers:
            raise WithdrawBlocked([b.to_dict() for b in blockers])

        # 已完成的同一请求：
        # - 当前状态正是上次提交产生的 release（重复请求重放）-> 复用已完成结果；
        # - 新 release 又被其他输入改动 -> 状态摘要不再匹配，拒绝过期行号并要求重新规划。
        prior = self._completed.get(request_id)
        if prior is not None and prior.get('post_state_digest') == self._state_digest():
            return deepcopy(prior)

        binding = self._binding(rows_by_id, transforms_by_id, request_id, subjects)
        if prior is not None:
            raise WithdrawBlocked([{
                'code': 'stale-plan',
                'detail': f'请求 {request_id} 曾在 release {prior["binding"]["release"]} '
                          f'完成，当前 release {binding["release"]} 输入已变化，必须重新规划'}])

        rows = self.manifest['rows']
        subject_set = set(subjects)

        # 1) 命中的 raw 行：整行撤除，不拆分其余主体
        hit_raw = {r['row_id'] for r in rows
                   if r['kind'] == 'raw' and subject_set.intersection(r['subjects'])}

        # 2) 影响沿来源边向下游传播的完整闭包（实际递归用 row_id）
        children = {rid: [] for rid in rows_by_id}
        for rid, r in rows_by_id.items():
            for p in r['parents']:
                children[p].append(rid)
        affected = set(hit_raw)
        stack = list(hit_raw)
        while stack:
            cur = stack.pop()
            for child in children[cur]:
                if child not in affected:
                    affected.add(child)
                    stack.append(child)

        # 3) 按拓扑序重算受影响子图，未受影响行保持原样
        order = topological_order(rows_by_id)
        new_rows_by_id = {rid: deepcopy(r) for rid, r in rows_by_id.items()}
        actions = []
        for rid in order:
            old = rows_by_id[rid]
            if rid not in affected:
                continue
            if old['kind'] == 'raw':
                del new_rows_by_id[rid]
                actions.append(self._action(rows_by_id, rid, 'removed',
                                            reason='raw 行命中撤回主体，整行撤除'))
                continue
            new = self._rebuild_row(rid, old, transforms_by_id, new_rows_by_id)
            if new is None:
                if rid in new_rows_by_id:
                    del new_rows_by_id[rid]
                actions.append(self._action(
                    rows_by_id, rid, 'removed',
                    reason='重算后无有效父行（或过滤不通过/分组消失）'))
            else:
                new_rows_by_id[rid] = new
                before = old['values']
                after = new['values']
                t = transforms_by_id[old['transform']]
                action = 'recomputed' if canonical(before) != canonical(after) \
                    or old['parents'] != new['parents'] else 'rebuilt-unchanged'
                entry = self._action(rows_by_id, rid, action)
                entry['before_values'] = deepcopy(before)
                entry['after_values'] = deepcopy(after)
                entry['parents_before'] = list(old['parents'])
                entry['parents_after'] = list(new['parents'])
                if t['kind'] == 'group-sum':
                    removed = [p for p in old['parents'] if p not in set(new['parents'])]
                    entry['group_key'] = {c: before.get(c) for c in t['group']}
                    entry['removed_contributions'] = [
                        {'parent': p,
                         'value': rows_by_id[p]['values'].get(t['value'])}
                        for p in removed]
                    entry['contributions_after'] = [
                        {'parent': p,
                         'value': new_rows_by_id[p]['values'].get(t['value'])}
                        for p in new['parents']]
                actions.append(entry)

        # 重建后来源边只指向仍有效父行（全图校验一次）
        for rid, r in new_rows_by_id.items():
            for p in r['parents']:
                if p not in new_rows_by_id:
                    raise WithdrawBlocked([{
                        'code': 'dangling-edge',
                        'detail': f'重建后行 {rid} 仍指向已撤除父行 {p}'}])

        new_rows = [new_rows_by_id[rid] for rid in order if rid in new_rows_by_id]
        unaffected = [rid for rid in order if rid not in affected]

        # 4) 清除账：逻辑重建 / 读屏障 / 历史副本三段分别报告
        ledger = self._clearance_ledger(rows_by_id, affected, hit_raw, binding)

        plan = {
            'status': 'planned',
            'binding': binding,
            'hit_raw_rows': sorted(hit_raw),
            'affected_row_ids': sorted(affected),
            'affected_rows': actions,
            'unaffected_row_ids': unaffected,
            'unaffected_unchanged': all(
                canonical(rows_by_id[rid]) == canonical(new_rows_by_id[rid])
                for rid in unaffected),
            'new_release': self._next_release(),
            'new_rows': new_rows,
            'clearance': ledger,
            'blockers': [],
        }
        return plan

    def _rebuild_row(self, rid, old, transforms_by_id, live):
        t = transforms_by_id[old['transform']]
        live_parents = [p for p in old['parents'] if p in live]
        if not live_parents:
            return None
        if t['kind'] == 'project':
            parent = live[live_parents[0]]
            values = {c: parent['values'].get(c) for c in t['columns']}
            return {'row_id': rid, 'kind': 'derived', 'transform': t['id'],
                    'parents': list(live_parents), 'values': values}
        if t['kind'] == 'filter':
            parent = live[live_parents[0]]
            where = t['where']
            if parent['values'].get(where['column']) != where['value']:
                return None
            return {'row_id': rid, 'kind': 'derived', 'transform': t['id'],
                    'parents': list(live_parents), 'values': deepcopy(parent['values'])}
        if t['kind'] == 'union':
            # union 按父行保留袋重数；每个输入槽（父行）独立决定存活
            parent = live[live_parents[0]]
            return {'row_id': rid, 'kind': 'derived', 'transform': t['id'],
                    'parents': list(live_parents), 'values': deepcopy(parent['values'])}
        # group-sum：排除被撤除贡献后按仍有效父行重算
        values = deepcopy(old['values'])
        parts = [live[p]['values'].get(t['value']) for p in live_parents]
        if any(x is not None and type(x) is not int for x in parts):
            raise WithdrawBlocked([{
                'code': 'value-not-integer',
                'detail': f"group-sum 行 {rid} 的父贡献存在非定标整数"}])
        values[t['value']] = _sum_integers(parts)
        return {'row_id': rid, 'kind': 'derived', 'transform': t['id'],
                'parents': list(live_parents), 'values': values}

    # ---- 提交 ----------------------------------------------------------

    def commit(self, plan):
        """原子切换当前行视图与来源边，递增撤回屏障；旧文件不覆写。"""
        if plan.get('blockers'):
            raise WithdrawBlocked(plan['blockers'])
        binding = plan['binding']
        current, _, blockers = validate_manifest(self.manifest)
        if blockers:
            raise WithdrawBlocked([b.to_dict() for b in blockers])
        current_binding = self._binding(current,
                                        {t['id']: t for t in self.manifest['transforms']},
                                        binding['request_id'], tuple(binding['subjects']))
        if current_binding['input_digest'] != binding['input_digest']:
            raise WithdrawBlocked([{
                'code': 'stale-plan',
                'detail': '计划绑定的输入摘要与当前 release 不一致，不能应用过期行号'}])

        old_release = self.manifest['release']
        new_manifest = deepcopy(self.manifest)
        new_manifest['release'] = plan['new_release']
        surviving_ids = {r['row_id'] for r in plan['new_rows']}
        new_manifest['rows'] = deepcopy(plan['new_rows'])
        # 墓碑：记录本 release 已从当前视图撤除的历史行 ID。
        # 历史副本仍可登记这些行以便清除账追踪；从未登记的行引用仍阻塞。
        prior_tombstones = set(self.manifest.get('tombstone_row_ids', []))
        new_manifest['tombstone_row_ids'] = sorted(
            prior_tombstones
            | {r['row_id'] for r in self.manifest['rows']
               if r['row_id'] not in surviving_ids})
        new_manifest['withdraw_epoch'] = self.epoch + 1
        new_manifest['prior_release'] = old_release
        new_manifest['last_withdraw'] = {
            'request_id': binding['request_id'],
            'subjects': list(binding['subjects']),
            'committed_at': self.now(),
        }
        # 一次赋值完成视图+来源边切换
        self.manifest = new_manifest
        self.epoch += 1

        result = deepcopy(plan)
        result['status'] = 'committed'
        result['committed'] = {
            'old_release': old_release,
            'new_release': new_manifest['release'],
            'withdraw_epoch_before': self.epoch - 1,
            'withdraw_epoch_after': self.epoch,
            'atomic_switch': True,
        }
        result['new_release_digest'] = digest(self._release_payload())
        result['post_state_digest'] = self._state_digest()
        self._completed[binding['request_id']] = deepcopy(result)
        return result

    # ---- 读屏障 --------------------------------------------------------

    def open_reader(self, reader_id):
        return Reader(self, reader_id, self.epoch, deepcopy(self.manifest),
                      set(self._forbidden_subjects()))

    def _forbidden_subjects(self):
        done = []
        for res in self._completed.values():
            done.extend(res['binding']['subjects'])
        return done

    # ---- 计划绑定与清除账 ----------------------------------------------

    def _binding(self, rows_by_id, transforms_by_id, request_id, subjects):
        row_lineage = {
            rid: {'row_id': rid, 'kind': r['kind'],
                  'parents': list(r['parents']),
                  'transform': r.get('transform'),
                  'row_digest': digest(r)}
            for rid, r in sorted(rows_by_id.items())
        }
        transform_versions = {t['id']: {'kind': t['kind'], 'version': t['version']}
                              for t in sorted(transforms_by_id.values(), key=lambda x: x['id'])}
        payload = {
            'release': self.manifest['release'],
            'transform_versions': transform_versions,
            'rows': row_lineage,
            'request_id': request_id,
            'subjects': list(subjects),
        }
        return {
            'release': self.manifest['release'],
            'request_id': request_id,
            'subjects': list(subjects),
            'transform_versions': transform_versions,
            'input_digest': digest(payload),
            'row_lineage': row_lineage,
        }

    def _next_release(self):
        return f"{self.manifest['release']}-wd"

    def _release_payload(self):
        m = self.manifest
        return {'release': m['release'], 'rows': m['rows'],
                'transforms': m['transforms']}

    def _state_digest(self):
        """当前状态摘要：行视图、来源边、转换版本、墓碑与 epoch 的绑定指纹。"""
        m = self.manifest
        return digest({
            'release': m['release'],
            'rows': m['rows'],
            'transforms': m['transforms'],
            'tombstone_row_ids': m.get('tombstone_row_ids', []),
            'withdraw_epoch': m.get('withdraw_epoch', 0),
        })

    def _action(self, rows_by_id, rid, action, reason=None):
        r = rows_by_id[rid]
        entry = {
            'row_id': rid,
            'kind': r['kind'],
            'transform': r.get('transform'),
            'action': action,
            'lineage_paths': self._lineage_paths(rows_by_id, rid),
        }
        if reason:
            entry['reason'] = reason
        return entry

    def _lineage_paths(self, rows_by_id, rid, seen=None):
        """返回该行到 raw 祖先的全部完整血缘路径（row_id 序列）。"""
        if seen is None:
            seen = frozenset()
        r = rows_by_id[rid]
        if r['kind'] == 'raw' or not r['parents']:
            return [[rid]]
        if rid in seen:
            return [[rid, '<cycle>']]
        paths = []
        for p in r['parents']:
            if p not in rows_by_id:
                paths.append([rid, f'<missing:{p}>'])
                continue
            for tail in self._lineage_paths(rows_by_id, p, seen | {rid}):
                paths.append([rid] + tail)
        return paths

    def _clearance_ledger(self, rows_by_id, affected, hit_raw, binding):
        tainted = set(affected)
        release_digest_before = digest(self._release_payload())
        copy_reports = []
        all_settled = True
        for c in self.manifest.get('copies', []):
            in_scope = bool(tainted.intersection(c['rows']))
            report = {
                'copy_id': c['copy_id'],
                'in_scope': in_scope,
                'registered': True,
                'rows': list(c['rows']),
                'tainted_rows': sorted(tainted.intersection(c['rows'])),
                'status': c['status'],
            }
            if not in_scope:
                report['settlement'] = 'unaffected'
            elif c['status'] == 'pending':
                report['settlement'] = 'blocking'
                report['detail'] = '历史文件/导出副本仍登记为 pending，阻塞本次范围清除完成'
                all_settled = False
            elif c['status'] == 'deleted':
                receipt = c['receipt']
                ok = (receipt['copy_id'] == c['copy_id']
                      and len(receipt['release_digest']) == 64
                      and bool(receipt['confirmed_at']))
                report['settlement'] = 'verified' if ok else 'blocking'
                report['receipt'] = deepcopy(receipt)
                if not ok:
                    all_settled = False
            else:  # exception：保留例外已人工登记理由与范围
                report['settlement'] = 'exception-registered'
                report['exception'] = deepcopy(c['exception'])
            copy_reports.append(report)

        logic_section = {
            'status': 'rebuilt',
            'removed_raw_rows': sorted(hit_raw),
            'note': '逻辑视图内撤除与重算已规划；不代表历史物理字节消失',
        }
        barrier_section = {
            'status': 'armed-on-commit',
            'withdraw_epoch_before': self.epoch,
            'withdraw_epoch_after': self.epoch + 1,
            'rule': '读操作开始与返回前各校验一次屏障，变化即失败重试，不返回旧缓冲',
        }
        copies_section = {
            'status': 'complete' if all_settled else 'blocked',
            'release_digest_before': release_digest_before,
            'copies': copy_reports,
            'unknown_copies': '登记范围之外的未知副本不承诺清除',
        }
        return {
            'scope_complete': all_settled,
            'logical_rebuild': logic_section,
            'read_barrier': barrier_section,
            'historical_copies': copies_section,
        }


class Reader:
    """绑定打开时刻 epoch 与 release 的读句柄。"""

    def __init__(self, lake, reader_id, epoch, manifest_snapshot, forbidden):
        self._lake = lake
        self.reader_id = reader_id
        self.epoch = epoch
        self.release = manifest_snapshot['release']
        self._snapshot = manifest_snapshot
        self._forbidden = forbidden

    def _check_barrier(self):
        # 先校验撤回屏障再读取；返回前还要再验一次
        if self._lake.epoch != self.epoch:
            raise BarrierStale(
                f'读句柄 {self.reader_id} 的 epoch {self.epoch} 已过期，'
                f'当前 epoch {self._lake.epoch}，请按新 release 重新打开并重试')

    def read_rows(self, row_ids=None, concurrent_commit=None):
        """读取当前句柄视图；concurrent_commit 模拟读取途中发生的撤回提交。"""
        self._check_barrier()
        rows = self._snapshot['rows']
        if row_ids is not None:
            wanted = set(row_ids)
            rows = [r for r in rows if r['row_id'] in wanted]
        buffered = deepcopy(rows)
        if concurrent_commit is not None:
            concurrent_commit()
        # 正在运行的读取在返回前重验屏障，否则只能失败重试
        self._check_barrier()
        # 双保险：缓冲中不得包含任一已撤回主体的 raw 行
        for r in buffered:
            if r['kind'] == 'raw' and self._forbidden.intersection(r.get('subjects', [])):
                raise BarrierStale(
                    f'缓冲行 {r["row_id"]} 包含已撤回主体，拒绝返回旧行')
        return buffered
