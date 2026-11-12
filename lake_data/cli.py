"""湖仓资料命令：原字段 inspect，以及主体撤回的 plan/commit/read。"""
import argparse
import json
from pathlib import Path

from .io import load
from .domain import summarize
from .withdraw import BarrierStale, Lakehouse, WithdrawBlocked


def _emit(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2, allow_nan=False))


def _load_lake(file):
    v, digest = load(file)
    return Lakehouse(v), digest


def main():
    p = argparse.ArgumentParser(description='湖仓主体撤回的行级影响规划与重建')
    sub = p.add_subparsers(dest='command', required=True)

    p_inspect = sub.add_parser('inspect', help='显示原字段概况，不修改源资料')
    p_inspect.add_argument('file')

    p_plan = sub.add_parser('plan', help='规划主体撤回的行级影响与衍生物重建')
    p_plan.add_argument('file')
    p_plan.add_argument('--request-id', required=True)
    p_plan.add_argument('--subject', action='append', required=True,
                        help='撤回主体，可重复传入')
    p_plan.add_argument('--out', help='把计划写入该文件（不覆写已存在文件）')

    p_commit = sub.add_parser('commit', help='执行已规划的撤回，产生新 release')
    p_commit.add_argument('plan_file')
    p_commit.add_argument('--manifest', required=True,
                          help='计划所绑定的当前清单文件')
    p_commit.add_argument('--out', required=True,
                          help='新 release 清单输出路径（不覆写旧文件）')

    p_read = sub.add_parser('read', help='以读屏障打开清单并读取行视图')
    p_read.add_argument('file')
    p_read.add_argument('--reader-id', default='cli-reader')
    p_read.add_argument('--row', action='append', help='限定读取 row_id，可重复')

    a = p.parse_args()

    if a.command == 'inspect':
        v, source_digest = load(a.file)
        _emit({'source_sha256': source_digest,
               'summary': summarize(v, Path(a.file).parent)})
        return

    if a.command == 'plan':
        lake, source_digest = _load_lake(a.file)
        try:
            plan = lake.plan_withdraw(a.request_id, a.subject)
        except WithdrawBlocked as e:
            _emit({'source_sha256': source_digest, 'status': 'blocked',
                   'blockers': e.blockers})
            raise SystemExit(2)
        plan['source_sha256'] = source_digest
        if a.out:
            out = Path(a.out)
            if out.exists():
                raise SystemExit(f'拒绝覆写已存在文件: {out}')
            out.write_text(json.dumps(plan, ensure_ascii=False, indent=2,
                                      allow_nan=False), encoding='utf8')
        _emit(plan)
        return

    if a.command == 'commit':
        plan = json.loads(Path(a.plan_file).read_text(encoding='utf8'))
        lake, _ = _load_lake(a.manifest)
        try:
            result = lake.commit(plan)
        except WithdrawBlocked as e:
            _emit({'status': 'blocked', 'blockers': e.blockers})
            raise SystemExit(2)
        out = Path(a.out)
        if out.exists():
            raise SystemExit(f'拒绝覆写已存在文件: {out}')
        out.write_text(json.dumps(lake.manifest, ensure_ascii=False, indent=2,
                                  allow_nan=False), encoding='utf8')
        _emit({'status': result['status'], 'committed': result['committed'],
               'new_release_digest': result['new_release_digest'],
               'clearance': result['clearance'], 'written': str(out)})
        return

    if a.command == 'read':
        lake, _ = _load_lake(a.file)
        reader = lake.open_reader(a.reader_id)
        try:
            rows = reader.read_rows(row_ids=a.row)
        except BarrierStale as e:
            _emit({'status': 'barrier-stale', 'error': str(e),
                   'action': '按新 release 重新打开读句柄后重试'})
            raise SystemExit(3)
        _emit({'status': 'ok', 'reader_id': a.reader_id,
               'release': reader.release, 'withdraw_epoch': reader.epoch,
               'rows': rows})


if __name__ == '__main__':
    main()
