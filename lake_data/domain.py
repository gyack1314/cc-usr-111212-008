"""清点血缘行、转换及副本登记，不传播撤回。"""
from .io import unique

def summarize(v,root):
    unique(v['rows'],'row_id');unique(v['copies'],'copy_id')
    for t in v['transforms']:
        if t['kind'] not in ('filter','project','union','group-sum'):raise ValueError('转换类别未知')
    return {'rows':len(v['rows']),'transforms':len(v['transforms']),'copies':len(v['copies']),'subjects_requested':len(v['request']['subjects'])}
