CREATE DATABASE IF NOT EXISTS demo.gold;

CREATE TABLE IF NOT EXISTS demo.gold.game_metrics_1m (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    game_id STRING,
    provider_id STRING,
    country STRING,
    bet_count BIGINT,
    total_bet_amount DECIMAL(18,2),
    total_payout DECIMAL(18,2),
    ggr DECIMAL(18,2),
    unique_players BIGINT
)
USING iceberg;


MERGE INTO demo.gold.game_metrics_1m AS target

USING (

    WITH round_result AS (
        SELECT
            player_id,
            round_id,
            game_id,
            provider_id,
            country,
            MIN(
                CASE
                    WHEN event_type = 'BET' THEN event_time
                END
            ) AS bet_time,
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
            ) AS payout_amount,
            MAX(
                CASE
                    WHEN event_type = 'CANCEL' THEN 1
                    ELSE 0
                END
            ) AS is_cancelled

        FROM demo.silver.game_transaction_events
        GROUP BY
            player_id,
            round_id,
            game_id,
            provider_id,
            country
    ),

    effective_round AS (
        SELECT
            player_id,
            round_id,
            game_id,
            provider_id,
            country,
            bet_time,
            CASE
                WHEN is_cancelled = 1 THEN CAST(0 AS DECIMAL(18,2))
                ELSE bet_amount
            END AS effective_bet_amount,
            CASE
                WHEN is_cancelled = 1 THEN CAST(0 AS DECIMAL(18,2))
                ELSE payout_amount
            END AS effective_payout_amount
        FROM round_result
        WHERE bet_time IS NOT NULL
    ),

    window_data AS (
        SELECT
            window(bet_time, '1 minute') AS window,
            player_id,
            round_id,
            game_id,
            provider_id,
            country,
            effective_bet_amount,
            effective_payout_amount
        FROM effective_round
    )

    SELECT
        window.start AS window_start,
        window.end AS window_end,
        game_id,
        provider_id,
        country,
        SUM(
            CASE
                WHEN effective_bet_amount > 0 THEN 1
                ELSE 0
            END
        ) AS bet_count,
        SUM(effective_bet_amount) AS total_bet_amount,
        SUM(effective_payout_amount) AS total_payout,
        SUM(effective_bet_amount) - SUM(effective_payout_amount) AS ggr,

        COUNT(
            DISTINCT CASE
                WHEN effective_bet_amount > 0 THEN player_id
            END
        ) AS unique_players

    FROM window_data

    GROUP BY
        window.start,
        window.end,
        game_id,
        provider_id,
        country

) AS source


ON target.window_start = source.window_start
AND target.game_id = source.game_id
AND target.provider_id <=> source.provider_id
AND target.country <=> source.country


WHEN MATCHED THEN UPDATE SET

    target.window_end = source.window_end,
    target.bet_count = source.bet_count,
    target.total_bet_amount = source.total_bet_amount,
    target.total_payout = source.total_payout,
    target.ggr = source.ggr,
    target.unique_players = source.unique_players


WHEN NOT MATCHED THEN INSERT (
    window_start,
    window_end,
    game_id,
    provider_id,
    country,
    bet_count,
    total_bet_amount,
    total_payout,
    ggr,
    unique_players
)

VALUES (
    source.window_start,
    source.window_end,
    source.game_id,
    source.provider_id,
    source.country,
    source.bet_count,
    source.total_bet_amount,
    source.total_payout,
    source.ggr,
    source.unique_players
);