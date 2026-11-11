CREATE DATABASE IF NOT EXISTS demo.ops;

CREATE TABLE IF NOT EXISTS demo.ops.transaction_reconciliation (
    player_id STRING,
    round_id STRING,
    issue_type STRING,
    bet_count BIGINT,
    settlement_count BIGINT,
    bet_amount DECIMAL(18,2),
    payout_amount DECIMAL(18,2),
    detected_time TIMESTAMP
)
USING iceberg;

WITH round_summary AS (
    SELECT
        player_id,
        round_id,

        SUM(
            CASE
                WHEN event_type = 'BET' THEN 1
                ELSE 0
            END
        ) AS bet_count,

        SUM(
            CASE
                WHEN event_type IN ('WIN', 'LOSS', 'CANCEL') THEN 1
                ELSE 0
            END
        ) AS settlement_count,

        SUM(
            CASE
                WHEN event_type = 'BET' THEN amount
                ELSE 0
            END
        ) AS bet_amount,

        SUM(
            CASE
                WHEN event_type = 'WIN' THEN amount
                ELSE 0
            END
        ) AS payout_amount

    FROM demo.silver.game_transaction_events

    GROUP BY
        player_id,
        round_id
),

reconciliation_result AS (

    -- 1. 有 BET，但是没有结算
    SELECT
        player_id,
        round_id,
        'BET_WITHOUT_SETTLEMENT' AS issue_type,
        bet_count,
        settlement_count,
        bet_amount,
        payout_amount,
        CURRENT_TIMESTAMP AS detected_time

    FROM round_summary

    WHERE bet_count > 0
      AND settlement_count = 0


    UNION ALL


    -- 2. 有结算，但是没有 BET
    SELECT
        player_id,
        round_id,
        'SETTLEMENT_WITHOUT_BET' AS issue_type,
        bet_count,
        settlement_count,
        bet_amount,
        payout_amount,
        CURRENT_TIMESTAMP AS detected_time

    FROM round_summary

    WHERE bet_count = 0
      AND settlement_count > 0


    UNION ALL


    -- 3. 同一个 round 出现多个 settlement
    SELECT
        player_id,
        round_id,
        'DUPLICATE_SETTLEMENT' AS issue_type,
        bet_count,
        settlement_count,
        bet_amount,
        payout_amount,
        CURRENT_TIMESTAMP AS detected_time

    FROM round_summary

    WHERE settlement_count > 1
)

INSERT INTO demo.ops.transaction_reconciliation
SELECT *
FROM reconciliation_result;