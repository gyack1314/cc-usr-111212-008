# 湖仓主体撤回的行级影响闭包与重建

本项目保存湖仓治理的合成资料与原始配置。`lake_data.io`读取JSON清单和资产身份，`lake_data.domain`检查基本字段并给数量概况，`lake_data.cli`输出原资料摘要。字段、数据类型、时间和版本口径见[资料约定](docs/data-contract.md)。数据中的设备、班组、主体和电量均为虚构，不连接电网、生产数仓或人员系统。

Python 3.11及以上，无第三方Python依赖。在项目根目录运行：

```sh
python -m unittest discover -s tests -v
python -m compileall -q lake_data tests
python -m lake_data.cli inspect data/normal.json
```

`inspect`只显示原字段概况，不修改源资料。`invalid-config.json`是基本配置错误，其余JSON为有效清单但可含需要领域操作判断的边界资料。null与缺字段应按各字段约定区分，原文件摘要绑定实际字节。

## 主体撤回的行级影响规划与重建

`lake_data.withdraw`实现以主体撤回为入口的行级影响闭包和衍生物重建，完整口径见[资料约定](docs/data-contract.md)。`lake_data.lineage`在规划前做结构性校验，任何血缘缺口、环、未知/不支持转换、坏回执等都进入阻塞清单，不允许部分生效。

```sh
# 规划：输出每条受影响行的路径、重算前后组值、未受影响行与清除账；有阻塞时退出码2
python -m lake_data.cli plan data/normal.json --request-id withdraw1 --subject person1 --out out/plan.json
# 提交：原子切换行视图与来源边、递增 withdraw_epoch，产生新 release；拒绝覆写已存在文件
python -m lake_data.cli commit out/plan.json --manifest data/normal.json --out out/release1.json
# 读取：读句柄在开始与返回前各校验一次撤回屏障，失效时退出码3并要求重试
python -m lake_data.cli read out/release1.json --reader-id after
```

规划结果中`affected_rows`逐行给出action（removed/recomputed/rebuilt-unchanged）、到raw祖先的完整`lineage_paths`、重算前后值与来源边；group-sum另给分组键、被撤除贡献与重算后完整输入贡献。`unaffected_unchanged`保证未受影响行逐字节不变。`clearance`把逻辑重建、读屏障、历史副本清理三段分别报告：pending或坏回执阻塞`scope_complete`，删除回执（copy_id、版本摘要、确认时刻）核验通过或人工保留例外登记齐全才可标完成；登记范围外的未知副本不承诺清除，逻辑清除不等于物理字节消失。`data/clearance-settled.json`是副本已结清的样例，`data/lineage-gap.json`演示缺边阻塞，`data/shared-raw-row.json`演示双主体行整行撤除。

Dockerfile固定Linux基础镜像。`docker build -t lake-material .`构建，`docker run --rm lake-material`读取正常清单；`docker run --rm lake-material python -m unittest discover -s tests -v`执行已有检查。所有资料本地可读，无需账号和在线来源。
