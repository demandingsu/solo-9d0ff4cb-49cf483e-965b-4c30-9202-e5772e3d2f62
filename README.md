# Battery Cell Cycle Capacity API

分批接收同一电芯的电流采样（UTC 秒 + 电流 A），识别完整充放电循环并按区间
前值积分计算充/放电容量（Ah），通过 FastAPI + SQLite 提供上传与查询接口。

## 业务规则

1. 电流为 0 的采样是**段界**。
2. 只有连续序列 `零 → 正电流 → 零 → 负电流 → 零` 构成一个循环；末尾未闭合
   的序列不计入。
3. 相邻采样时间差 **> 60 秒**即断档：当前序列截断，且该长区间本身不参与积分
   （恰好 60 秒不断档）。
4. 容量按**区间前值积分**（左矩形法）：`Ah += |I_prev| * (t - t_prev) / 3600`，
   充电、放电分别累计。
5. 同电芯同秒同值重复上传**幂等**（不重复插入）；同秒异值则**整批拒收**
   （HTTP 409，库内数据不变）。
6. 迟到补点与历史采样合并后**全量重算**，循环可能被改写、编号重新排列；
   每次成功导入生成一个新的报告版本，历史版本保留。
7. 导入或计算失败时事务回滚，旧采样与旧报告保持不变，并在 `upload_log` 留痕。
8. 并发：写入用 `BEGIN IMMEDIATE` 串行化；查询在单事务快照内读取同一版本的
   版本号、循环明细与总数，保证三者属于同一次已提交状态（SQLite WAL）。

## 备份恢复（单电芯）

故障备份盘上的旧库可按电芯恢复到现库，接口 `POST /cells/{cell_id}/restore`
（multipart，字段名 `file`）。恢复流程：

1. 上传文件先写入独立临时目录（不执行、不就地打开），默认上限 512 MiB
   （`BATTERY_MAX_RESTORE_BYTES` 可调）。
2. 以只读 + `immutable` 模式打开，校验 SQLite 文件头、`PRAGMA integrity_check`
   与所需表/列（`cells`/`samples`/`report_versions`/`cycles`）。
3. 读取该电芯原始采样（整数微安）并用**当前算法全量重算**，要求与备份中该电芯
   最新报告完全一致：`total_cycles`、每循环 `cycle_no`/`started_at`/`ended_at`
   严格相等，充/放电容量误差仅允许 1 µAh 浮点舍入；`report_versions` 行数与
   明细行数也必须对齐。
4. 通过后在单个 `BEGIN IMMEDIATE` 事务内写入采样、最新报告（保留原版本号）与
   循环明细，并记 `upload_log(status='restored')`。之后上传从该版本 +1 续写。
5. 以下情况整次拒绝，事务回滚且不留半份数据：
   - 非 SQLite / 文件损坏 / 缺表缺列 / 备份中无该电芯或采样或报告（HTTP 422）；
   - 备份内记录缺失或与重算结果不一致（HTTP 422）；
   - 现库已存在同名电芯（含此前上传或恢复建立的，HTTP 409）；
   - 空文件（400）、超过大小上限（413）。

仅恢复备份中该电芯的**最新版本**，更早版本不导入（响应里
`skipped_older_versions` 说明跳过数量）；备份中的其他电芯不会被写入，现库其他
电芯照常查询和上传。恢复与首次上传、另一恢复并发时由写事务串行化，只有一方成功。

## 接口（启动后见 `/docs`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/cells/{cell_id}/samples` | multipart 上传 CSV（字段名 `file`） |
| POST | `/cells/{cell_id}/restore` | multipart 上传备份 SQLite 库，恢复单个电芯 |
| GET  | `/cells/{cell_id}/report` | 最新已提交报告：版本、总数、循环明细 |
| GET  | `/cells` | 全部电芯及最新版本概览 |
| GET  | `/health` | 存活探针 |

CSV 必须含表头，列名大小写/空格不敏感，时间列支持
`timestamp/ts/timestamp_utc/timestamp_seconds/utc_seconds/time_s`，
电流列支持 `current/current_a/i/amps` 等。示例见 `examples/`。

```bash
curl -X POST http://localhost:8000/cells/CELL-001/samples \
  -F "file=@examples/sample_one_cycle.csv"
curl http://localhost:8000/cells/CELL-001/report

# from a backup database containing CELL-001
curl -X POST http://localhost:8000/cells/CELL-001/restore \
  -F "file=@/path/to/battery-backup.db"
```

## 运行

### Docker Compose（推荐）

```bash
docker compose up --build
# API:   http://localhost:8000
# Docs:  http://localhost:8000/docs
```

SQLite 数据保存在命名卷 `battery-data`（容器内 `/data/battery.db`）。

### 本地运行

```bash
pip install -r requirements.txt
BATTERY_DB_PATH=./data/battery.db uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 测试

```bash
python -m unittest discover -s tests -v       # 算法单测 + HTTP 集成测试
python tests/live_smoke.py                     # 对已运行服务的端到端冒烟
```

覆盖场景：基本循环与前值积分、断档（61s 截断 / 60s 保留）、无零符号直翻、
未闭合不计、补点改写与重新编号、幂等重投、异值整批拒收、失败回滚、
8 写 8 读并发一致性、同秒异值并发竞争；以及备份恢复的成功路径（保留版本号、
后续续写）、损坏/截断/缺表/缺记录/总数与容量不一致的整次拒绝、同名冲突、
事务中途失败无残留、恢复与上传/恢复并发竞争。

## 目录结构

```
app/
  algorithm.py    # 分词 + 状态机循环识别、Ah 积分
  csv_import.py   # CSV 表头解析与校验
  backup_restore.py # 备份库只读校验 + 原始采样重算比对
  db.py           # SQLite 事务、幂等/冲突、版本化报告、单电芯恢复
  main.py         # FastAPI 路由
tests/            # 单元、集成、实时冒烟测试
examples/         # 示例 CSV
Dockerfile, docker-compose.yml
```

## 设计说明与限制

- 电流以**微安整数**存储（`current_ma` 列，单位 µA），使幂等比较为精确整数
  比较，避免浮点文本往返误差；低于 1 µA 的差异会被取整。
- 报告只增不删：每次导入（含纯幂等重投）都追加一个 `report_versions` 行，
  便于追溯；查询接口只返回最新版本。
- 备份恢复只接受**已 checkpoint、无 `-wal` 边车文件**的单文件备份。SQLite
  的 WAL 模式下，未合并进主文件的已提交数据只存在于 `-wal` 中；只上传主文件
  会漏掉这部分采样（缺记录时按规则整次拒绝，不会写入残缺数据）。上传前先
  `sqlite3 backup.db 'PRAGMA wal_checkpoint(TRUNCATE);'`，或使用 `.backup`/
  `VACUUM INTO` 生成自包含副本。
- 恢复只在备份与**当前算法**重算结果一致时接受；算法规则若日后变更，旧备份
  会被拒绝而不是静默改写（可先用旧版本服务导出/转换）。
- 单文件 SQLite + WAL 适合本任务量级；写入在进程级连接上串行提交。
