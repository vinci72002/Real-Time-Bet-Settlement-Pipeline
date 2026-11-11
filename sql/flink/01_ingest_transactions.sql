
SET 'pipeline.name' = 'game-transaction-pipeline';

--判断重复id有效的时间
SET 'table.exec.state.ttl' = '24 h';

--忽略长时间没有进入数据的source，避免阻塞 watermark
SET 'table.exec.source.idle-timeout' = '10 s';


-- 每10秒做一次 checkpoint
SET 'execution.checkpointing.interval' = '10 s';

-- Exactly Once
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- checkpoint 超时
SET 'execution.checkpointing.timeout' = '60 s';

-- 两次 checkpoint 之间至少间隔 5 秒
SET 'execution.checkpointing.min-pause' = '5 s';

-- 同时只允许一个 checkpoint
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';

-- Job 失败后自动重启
SET 'restart-strategy.type' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '3';
SET 'restart-strategy.fixed-delay.delay' = '10 s';

--创建kafka source 表
create TEMPORARY table kafka_game_transaction (
    event_id STRING,
    transaction_id STRING,
    player_id STRING,
    game_id STRING,
    provider_id STRING,
    event_type STRING,
    amount DECIMAL(18,2),
    currency STRING,
    event_time TIMESTAMP(3),
    country STRING,
    platform STRING,
    round_id STRING,
    ingestion_time TIMESTAMP(3),
    processed_time as PROCTIME(),
    watermark for event_time as event_time - interval '5' second

)
WITH (
    'connector' = 'kafka',
    'topic' = 'game_transaction_events',
    'properties.bootstrap.servers' = 'kafka:19092',
    'properties.group.id' = 'game-transaction-group-2',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json',
    'json.timestamp-format.standard' = 'SQL',
    'json.ignore-parse-errors' = 'true'
);

--创建iceberg catalog 连接
CREATE CATALOG demo WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',

    'warehouse' = 's3://warehouse/',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',

    's3.path-style-access' = 'true',
    'client.region' = 'us-east-1'

);

CREATE DATABASE IF NOT EXISTS demo.silver;

--创建银层事件表
create table if not exists demo.silver.game_transaction_events (
    event_id STRING,
    transaction_id STRING,
    player_id STRING,
    game_id STRING,
    provider_id STRING,
    event_type STRING,
    amount DECIMAL(18,2),
    currency STRING,
    event_time TIMESTAMP(3),
    country STRING,
    platform STRING,
    round_id STRING,
    ingestion_time TIMESTAMP(3),
    processed_time TIMESTAMP(3)
    
);
--创建拒绝事件表
CREATE DATABASE IF NOT EXISTS demo.quarantine;
create table if not EXISTS demo.quarantine.game_transaction_rejected (
    event_id STRING,
    transaction_id STRING,
    player_id STRING,
    game_id STRING,
    provider_id STRING,
    event_type STRING,
    amount DECIMAL(18,2),
    currency STRING,
    event_time TIMESTAMP(3),
    country STRING,
    platform STRING,
    round_id STRING,
    ingestion_time TIMESTAMP(3),
    processed_time TIMESTAMP(3),
    rejection_reason STRING
);

--创建有效的事件视图
create temporary view valid_game_transaction_events as
select 
    event_id,
    transaction_id,
    player_id,

    game_id,
    provider_id,
    event_type,

    amount,
    currency,
    event_time,

    country,
    platform,
    round_id,

    ingestion_time,
    row_number() over (partition by event_id ORDER BY processed_time ASC) as row_num
from kafka_game_transaction
where
    event_id IS not NULL and
    transaction_id IS not NULL and
    player_id IS not NULL and

    game_id IS not NULL and
    event_type IN ('BET','WIN','LOSS','CANCEL') and
    amount >= 0 and

    currency IS not NULL and 
    event_time IS not NULL and
    (
        current_watermark(event_time) is null 
        or event_time >= current_watermark(event_time) - interval '15' second
    );

--创建拒绝的事件视图
create temporary view rejected_game_transaction_events as
select
    
    event_id,
    transaction_id,
    player_id,

    game_id,
    provider_id,
    event_type,

    amount,
    currency,
    event_time,

    country,
    platform,
    round_id,

    ingestion_time,
    current_timestamp AS processed_time,

    case 
        when event_id IS NULL then 'MISSING_EVENT_ID'
        when transaction_id IS NULL then 'MISSING_TRANSACTION_ID'
        when player_id IS NULL then 'MISSING_PLAYER_ID'
        when game_id IS NULL then 'MISSING_GAME_ID'
        WHEN event_type IS NULL THEN 'MISSING_EVENT_TYPE'
        when event_type not in ('BET','WIN','LOSS','CANCEL') then 'INVALID_EVENT_TYPE'
        when amount < 0 then 'NEGATIVE_AMOUNT'
        WHEN amount IS NULL THEN 'MISSING_AMOUNT'
        when currency IS NULL then 'MISSING_CURRENCY'
        when event_time IS NULL then 'MISSING_EVENT_TIME'
        else 'UNKNOWN_REASON'
    end as rejection_reason
from kafka_game_transaction
where 
    event_id IS NULL or
    transaction_id IS NULL or
    player_id IS NULL or

    game_id IS NULL or
    event_type not in ('BET','WIN','LOSS','CANCEL') or
    event_type IS NULL or
    amount IS NULL or
    amount < 0 or
    currency IS NULL or 
    event_time IS NULL;


--创建重复交易事件交易视图
create temporary view duplicate_game_transaction_events as
select 
    event_id,
    transaction_id,
    player_id,

    game_id,
    provider_id,
    event_type,

    amount,
    currency,
    event_time,

    country,
    platform,
    round_id,

    ingestion_time,
    current_timestamp AS processed_time,
    count(*) over (
        partition by event_id 
        ORDER BY processed_time ASC
        rows between unbounded preceding and current row
        ) as row_count
from kafka_game_transaction
where 
    event_id IS not NULL and
    transaction_id IS not NULL and
    player_id IS not NULL and

    game_id IS not NULL and
    event_type IN ('BET','WIN','LOSS','CANCEL') and
    amount >= 0 and

    currency IS not NULL and 
    event_time IS not NULL and
    (
        current_watermark(event_time) is null 
        or event_time >= current_watermark(event_time) - interval '15' second
    );

--创建重复事件表
create table if not exists demo.quarantine.game_transaction_duplicate (
    event_id STRING,
    transaction_id STRING,
    player_id STRING,
    game_id STRING,
    provider_id STRING,
    event_type STRING,
    amount DECIMAL(18,2),
    currency STRING,
    event_time TIMESTAMP(3),
    country STRING,
    platform STRING,
    round_id STRING,
    ingestion_time TIMESTAMP(3),
    processed_time TIMESTAMP(3)
    
);

--创建迟到事件表
create table if not exists demo.quarantine.game_transaction_late (
    event_id STRING,
    transaction_id STRING,
    player_id STRING,
    game_id STRING,
    provider_id STRING,
    event_type STRING,
    amount DECIMAL(18,2),
    currency STRING,
    event_time TIMESTAMP(3),
    country STRING,
    platform STRING,
    round_id STRING,
    ingestion_time TIMESTAMP(3),
    processed_time TIMESTAMP(3),
    rejection_reason STRING
);

BEGIN STATEMENT SET;

--正常事件写入表
insert into demo.silver.game_transaction_events
select 
    event_id,
    transaction_id,
    player_id,

    game_id,
    provider_id,
    event_type,

    amount,
    currency,
    event_time,

    country,
    platform,
    round_id,

    ingestion_time,
    current_timestamp AS processed_time
from valid_game_transaction_events
where row_num = 1;

--拒绝事件写入表
insert into demo.quarantine.game_transaction_rejected
select
    
    event_id,
    transaction_id,
    player_id,

    game_id,
    provider_id,
    event_type,

    amount,
    currency,
    event_time,

    country,
    platform,
    round_id,

    ingestion_time,
    current_timestamp AS processed_time,

    rejection_reason
from rejected_game_transaction_events;

--重复事件写入表
insert into demo.quarantine.game_transaction_duplicate
select 
    event_id,
    transaction_id,
    player_id,

    game_id,
    provider_id,
    event_type,

    amount,
    currency,
    event_time,

    country,
    platform,
    round_id,

    ingestion_time,
    current_timestamp AS processed_time
from duplicate_game_transaction_events
where row_count > 1;

--将迟到的事件写入迟到事件表
insert into demo.quarantine.game_transaction_late
select
    event_id,
    transaction_id,
    player_id,

    game_id,
    provider_id,
    event_type,

    amount,
    currency,
    event_time,

    country,
    platform,
    round_id,

    ingestion_time,
    current_timestamp AS processed_time,

    'LATE_EVENT' as rejection_reason
from kafka_game_transaction
where 
    event_id IS not NULL and
    transaction_id IS not NULL and
    player_id IS not NULL and

    game_id IS not NULL and
    event_type IN ('BET','WIN','LOSS','CANCEL') and
    amount >= 0 and

    currency IS not NULL and 
    event_time IS not NULL and
    current_watermark(event_time) is not null and 
    event_time < current_watermark(event_time) - interval '15' second;


END;
