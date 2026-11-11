# Real-Time Bet & Settlement Pipeline

基于 Kafka、Flink SQL、Iceberg 和 Spark SQL 的投注交易处理示例。项目包含实时事件校验与分流、分钟级游戏指标汇总、交易对账以及只读诊断 SQL。

> 当前仓库提供 SQL 和运行文档，不包含 Kafka、Flink、Spark、Iceberg REST Catalog 或 MinIO 的部署配置。运行前需要准备并配置这些外部服务。

## 处理流程

```text
Kafka: game_transaction_events
                 |
                 v
Flink SQL: 校验 / event_id 去重 / watermark 迟到判断
       |                |                    |
       v                v                    v
demo.silver       demo.quarantine       rejected / duplicate / late
       |
       +--------------------------+
       |                          |
       v                          v
Spark SQL: 分钟指标            Spark SQL: 交易对账
demo.gold.game_metrics_1m     demo.ops.transaction_reconciliation
```

所有 Iceberg 表通过名为 `demo` 的 Iceberg REST Catalog 访问，并使用共享的对象存储 warehouse。

## 仓库结构

```text
.
├── README.md
├── .gitignore
├── data/
│   └── sample_game_transaction_events.jsonl
├── docs/
│   └── runbook.md
└── sql/
    ├── flink/
    │   └── 01_ingest_transactions.sql
    ├── spark/
    │   ├── 02_game_metrics_1m.sql
    │   └── 03_transaction_reconciliation.sql
    └── ops/
        └── 04_diagnostics.sql
```

## 组件与依赖

- Kafka：提供 `game_transaction_events` JSON topic。
- Flink SQL Client：需要与运行环境兼容的 Kafka SQL Connector、Iceberg Connector 和 S3 FileIO/AWS 依赖。
- Iceberg REST Catalog：当前 SQL 指向 `http://iceberg-rest:8181`。
- S3 兼容对象存储：当前 endpoint 为 `http://minio:9000`，warehouse 为 `s3://warehouse/`。
- Spark SQL：需要启用 Iceberg Spark 扩展、Iceberg REST Catalog 和对应 S3 文件 IO 配置。
- 容器网络：上述服务地址必须能从 Flink/Spark 容器解析和访问。运行脚本中的容器名沿用本地笔记，可按实际环境修改。

版本、Kafka 认证、REST Catalog 认证和对象存储凭据由部署环境管理，本仓库不预设这些值。请通过受控的容器环境变量、凭据提供程序或密钥管理系统配置访问凭据；不要将真实密钥提交到仓库。

## 输入事件

Topic 中每条 JSON 消息代表一笔游戏交易事件。示例见 [`data/sample_game_transaction_events.jsonl`](data/sample_game_transaction_events.jsonl)。

| 字段 | 类型 | 说明 |
|---|---|---|
| `event_id` | string | 事件唯一标识；Flink 按此字段做重复事件识别 |
| `transaction_id` | string | 交易标识 |
| `player_id` | string | 玩家标识 |
| `game_id` | string | 游戏标识 |
| `provider_id` | string | 游戏供应商标识 |
| `event_type` | string | `BET`、`WIN`、`LOSS` 或 `CANCEL` |
| `amount` | decimal | 非负交易金额 |
| `currency` | string | 币种 |
| `event_time` | timestamp | 事件时间；Flink SQL 使用 `SQL` 时间戳格式 |
| `country` | string | 国家/地区 |
| `platform` | string | 平台 |
| `round_id` | string | 游戏局标识 |
| `ingestion_time` | timestamp | 上游写入时间 |

`country`、`platform`、`provider_id`、`round_id` 和 `ingestion_time` 当前不作为 Flink 校验必填项；请根据业务数据契约决定是否收紧校验。

## 运行

1. 确认 Kafka topic、Flink、Iceberg REST Catalog、对象存储和 Spark 已就绪，且 Flink 与 Spark 使用同一个 Iceberg Catalog/warehouse。
2. 在仓库根目录执行 [`docs/runbook.md`](docs/runbook.md) 中的 Flink SQL 命令，启动实时消费。
3. 确认 `demo.silver.game_transaction_events` 已有数据后，运行 Spark 分钟指标脚本。
4. 按需运行交易对账脚本和诊断查询。

主要脚本：

- [`sql/flink/01_ingest_transactions.sql`](sql/flink/01_ingest_transactions.sql)：Kafka 到 Iceberg silver/quarantine 的实时处理。
- [`sql/spark/02_game_metrics_1m.sql`](sql/spark/02_game_metrics_1m.sql)：按投注局的投注时间做一分钟聚合，计算投注额、派彩金额、GGR 和玩家数。
- [`sql/spark/03_transaction_reconciliation.sql`](sql/spark/03_transaction_reconciliation.sql)：检测未结算、有结算无投注、重复结算等异常。
- [`sql/ops/04_diagnostics.sql`](sql/ops/04_diagnostics.sql)：只读表检查、局级汇总及未结算投注查询。

## 当前行为与注意事项

- Flink watermark 延迟为 5 秒；事件时间比当前 watermark 落后超过 15 秒时进入迟到事件表。重复事件状态 TTL 为 24 小时。
- `game_metrics_1m` 按局聚合：同一局的 `BET`/`WIN` 事件会合并，取消局的投注额和派彩金额归零；`bet_count` 是有效投注局数，而不是原始 `BET` 事件数。
- Spark 分钟指标脚本是批处理 MERGE，不是持续运行的 Spark Streaming 作业。需要重新执行以刷新当前表快照；它不清理由此前结果产生但本次源数据已不存在的目标行。
- 对账脚本使用 INSERT；每次执行都会追加当前异常结果，重复运行可能产生重复记录。
- Kafka JSON 解析配置启用了 `json.ignore-parse-errors = true`；无法解析的消息不会进入 SQL 定义的 quarantine 表。若需追踪此类输入，请在 Kafka/Connector 层增加错误处理和监控。
- 诊断脚本不包含删除或清空表的语句。清理数据请先确认目标表、环境和备份策略，再单独执行经过审核的维护操作。

## 许可

仓库当前未指定开源许可证。发布前请按项目权属和预期使用方式选择并添加许可证文件。
