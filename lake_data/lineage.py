"""血缘图装载与撤回前结构校验。

校验只产出结构化阻塞清单（blocker），不抛出后静默截断；任何一项阻塞
都意味着撤回规划不能继续，避免只删看得见的几层就报完成。
"""
from dataclasses import dataclass

TRANSFORM_KINDS = ('filter', 'project', 'union', 'group-sum')
SINGLE_PARENT_KINDS = ('filter', 'project', 'union')


@dataclass(frozen=True)
class Blocker:
    code: str
    detail: str

    def to_dict(self):
        return {'code': self.code, 'detail': self.detail}


def _is_int(x):
    # 契约：定标整数；bool 不是整数
    return type(x) is int


def validate_manifest(v):
    """校验清单结构，返回 (rows_by_id, transforms_by_id, blockers)。"""
    blockers = []
    if not isinstance(v, dict):
        return {}, {}, [Blocker('manifest-shape', '顶层资料应为对象')]

    rows = v.get('rows')
    transforms = v.get('transforms')
    copies = v.get('copies', [])
    request = v.get('request')
    if not isinstance(rows, list):
        blockers.append(Blocker('manifest-shape', 'rows 必须为数组'))
        rows = []
    if not isinstance(transforms, list):
        blockers.append(Blocker('manifest-shape', 'transforms 必须为数组'))
        transforms = []
    if not isinstance(copies, list):
        blockers.append(Blocker('manifest-shape', 'copies 必须为数组'))
        copies = []
    if not isinstance(request, dict) or not isinstance(request.get('request_id'), str) \
            or not request['request_id'] or not isinstance(request.get('subjects'), list):
        blockers.append(Blocker('request-shape', 'request 需含 request_id 与 subjects 数组'))

    # 行身份唯一
    rows_by_id = {}
    for r in rows:
        if not isinstance(r, dict) or not isinstance(r.get('row_id'), str) or not r['row_id']:
            blockers.append(Blocker('row-shape', f'存在缺少 row_id 的行: {r!r}'))
            continue
        if r['row_id'] in rows_by_id:
            blockers.append(Blocker('row-id-duplicate', f"row_id 重复: {r['row_id']}"))
            continue
        rows_by_id[r['row_id']] = r

    # 转换身份唯一且类别受支持
    transforms_by_id = {}
    for t in transforms:
        if not isinstance(t, dict) or not isinstance(t.get('id'), str) or not t['id']:
            blockers.append(Blocker('transform-shape', f'转换缺少 id: {t!r}'))
            continue
        if t['id'] in transforms_by_id:
            blockers.append(Blocker('transform-id-duplicate', f"转换 id 重复: {t['id']}"))
            continue
        kind = t.get('kind')
        if kind not in TRANSFORM_KINDS:
            blockers.append(Blocker('unsupported-transform',
                                    f"转换 {t['id']} 的类别 {kind!r} 不受支持"))
        else:
            if not isinstance(t.get('version'), str) or not t['version']:
                blockers.append(Blocker('transform-shape', f"转换 {t['id']} 缺少 version"))
            if kind == 'project':
                if not isinstance(t.get('columns'), list) or not t['columns'] \
                        or not all(isinstance(c, str) and c for c in t['columns']):
                    blockers.append(Blocker('transform-shape',
                                            f"project 转换 {t['id']} 的 columns 非法"))
            elif kind == 'filter':
                where = t.get('where')
                if not isinstance(where, dict) or not isinstance(where.get('column'), str) \
                        or not where['column'] or 'value' not in where:
                    blockers.append(Blocker('transform-shape',
                                            f"filter 转换 {t['id']} 的 where 必须为 {{column,value}}"))
            elif kind == 'group-sum':
                if not isinstance(t.get('group'), list) or not t['group'] \
                        or not all(isinstance(c, str) and c for c in t['group']):
                    blockers.append(Blocker('transform-shape',
                                            f"group-sum 转换 {t['id']} 的 group 非法"))
                if not isinstance(t.get('value'), str) or not t['value']:
                    blockers.append(Blocker('transform-shape',
                                            f"group-sum 转换 {t['id']} 的 value 非法"))
        transforms_by_id[t['id']] = t

    # 行结构、父边与父行存在性
    for rid, r in rows_by_id.items():
        kind = r.get('kind')
        parents = r.get('parents')
        if not isinstance(parents, list):
            blockers.append(Blocker('row-shape', f'行 {rid} 的 parents 必须为数组'))
            continue
        if kind == 'raw':
            if parents:
                blockers.append(Blocker('raw-parent', f'raw 行 {rid} 不应有父行'))
            if not isinstance(r.get('subjects'), list) \
                    or not all(isinstance(s, str) and s for s in r['subjects']):
                blockers.append(Blocker('row-shape', f'raw 行 {rid} 的 subjects 非法'))
            if not isinstance(r.get('values'), dict):
                blockers.append(Blocker('row-shape', f'raw 行 {rid} 的 values 必须为对象'))
        elif kind == 'derived':
            if not isinstance(r.get('transform'), str) or r['transform'] not in transforms_by_id:
                blockers.append(Blocker('unknown-transform',
                                        f"derived 行 {rid} 引用未知转换 {r.get('transform')!r}"))
            for p in parents:
                if not isinstance(p, str) or not p:
                    blockers.append(Blocker('row-shape', f'行 {rid} 存在非法父边 {p!r}'))
                elif p not in rows_by_id:
                    # 契约：资料中缺父不当空贡献，必须阻塞
                    blockers.append(Blocker('missing-parent',
                                            f'行 {rid} 的父行 {p} 不存在（血缘缺口）'))
        else:
            blockers.append(Blocker('row-shape', f'行 {rid} 的 kind {kind!r} 非法'))

    # 父边基数与 group-sum 组键唯一性
    group_keys = {}
    for rid, r in rows_by_id.items():
        if r.get('kind') != 'derived':
            continue
        t = transforms_by_id.get(r.get('transform'))
        if t is None:
            continue
        parents = r.get('parents', [])
        if t['kind'] in SINGLE_PARENT_KINDS and len(parents) != 1:
            blockers.append(Blocker('parent-cardinality',
                                    f"{t['kind']} 行 {rid} 必须恰好有一个父行，实际 {len(parents)}"))
        if t['kind'] == 'group-sum':
            if not parents:
                blockers.append(Blocker('parent-cardinality',
                                        f'group-sum 行 {rid} 至少需要一个父行'))
            key = tuple((c, r.get('values', {}).get(c)) for c in t['group'])
            sk = (t['id'], key)
            if sk in group_keys:
                blockers.append(Blocker('duplicate-group-key',
                                        f"转换 {t['id']} 的分组键 {key} 同时落在行 "
                                        f"{group_keys[sk]} 与 {rid}"))
            else:
                group_keys[sk] = rid
            val = r.get('values', {}).get(t['value'])
            if val is not None and not _is_int(val):
                blockers.append(Blocker('value-not-integer',
                                        f'group-sum 行 {rid} 的聚合值必须为整数或 null'))

    # 环检测（含父边缺失时跳过，缺边已单独阻塞）
    blockers.extend(_detect_cycles(rows_by_id))

    # 副本登记：身份唯一，引用行必须存在（含历史墓碑行），状态与回执/例外结构合法
    known_row_ids = set(rows_by_id) | set(v.get('tombstone_row_ids', []))
    seen_copies = set()
    for c in copies:
        if not isinstance(c, dict) or not isinstance(c.get('copy_id'), str) or not c['copy_id']:
            blockers.append(Blocker('copy-shape', f'副本缺少 copy_id: {c!r}'))
            continue
        if c['copy_id'] in seen_copies:
            blockers.append(Blocker('copy-id-duplicate', f"copy_id 重复: {c['copy_id']}"))
            continue
        seen_copies.add(c['copy_id'])
        if not isinstance(c.get('rows'), list) or not all(isinstance(x, str) and x for x in c['rows']):
            blockers.append(Blocker('copy-shape', f"副本 {c['copy_id']} 的 rows 非法"))
        else:
            for rid in c['rows']:
                # 历史副本保留对已撤除（墓碑）行的登记，清除账据此追踪；
                # 只有引用清单中从未登记过的行才阻塞
                if rid not in known_row_ids:
                    blockers.append(Blocker('copy-unknown-row',
                                            f"副本 {c['copy_id']} 引用从未登记的未知行 {rid}"))
        status = c.get('status')
        if status not in ('pending', 'deleted', 'exception'):
            blockers.append(Blocker('copy-status',
                                    f"副本 {c['copy_id']} 的状态 {status!r} 非法"))
        elif status == 'deleted':
            receipt = c.get('receipt')
            if not isinstance(receipt, dict) or not isinstance(receipt.get('copy_id'), str) \
                    or receipt.get('copy_id') != c['copy_id'] \
                    or not isinstance(receipt.get('release_digest'), str) \
                    or len(receipt.get('release_digest', '')) != 64 \
                    or not isinstance(receipt.get('confirmed_at'), str) \
                    or not receipt['confirmed_at']:
                blockers.append(Blocker('receipt-invalid',
                                        f"副本 {c['copy_id']} 的删除回执缺少 copy_id/版本摘要/确认时刻"))
        elif status == 'exception':
            note = c.get('exception')
            if not isinstance(note, dict) or not isinstance(note.get('reason'), str) \
                    or not note['reason'] or not isinstance(note.get('scope'), str) \
                    or not note['scope']:
                blockers.append(Blocker('exception-invalid',
                                        f"副本 {c['copy_id']} 的保留例外需人工登记理由与范围"))

    return rows_by_id, transforms_by_id, blockers


def _detect_cycles(rows_by_id):
    color = {}  # 0=白 1=灰 2=黑
    blockers = []

    def dfs(rid, stack):
        color[rid] = 1
        for p in rows_by_id[rid].get('parents', []):
            if p not in rows_by_id:
                continue
            if color.get(p, 0) == 1:
                cycle = ' -> '.join(stack[stack.index(p):] + [p])
                blockers.append(Blocker('lineage-cycle', f'血缘存在环: {cycle}'))
                return True
            if color.get(p, 0) == 0 and dfs(p, stack + [p]):
                return True
        color[rid] = 2
        return False

    for rid in rows_by_id:
        if color.get(rid, 0) == 0:
            dfs(rid, [rid])
    return blockers


def topological_order(rows_by_id):
    """父行在前的拓扑序；调用前应已确认无环、无缺父。"""
    ordered = []
    done = set()

    def visit(rid):
        if rid in done:
            return
        for p in rows_by_id[rid].get('parents', []):
            visit(p)
        if rid not in done:
            done.add(rid)
            ordered.append(rid)

    for rid in rows_by_id:
        visit(rid)
    return ordered
