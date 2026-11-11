SHOW DATABASES;
SHOW TABLES IN demo.silver;
SHOW TABLES IN demo.quarantine;
SHOW TABLES IN demo.gold;
SHOW TABLES IN demo.ops;

SELECT *
FROM demo.silver.game_transaction_events
ORDER BY processed_time DESC
LIMIT 100;

SELECT *
FROM demo.quarantine.game_transaction_rejected
ORDER BY processed_time DESC
LIMIT 100;

SELECT *
FROM demo.quarantine.game_transaction_duplicate
ORDER BY processed_time DESC
LIMIT 100;

SELECT *
FROM demo.quarantine.game_transaction_late
ORDER BY processed_time DESC
LIMIT 100;

SELECT *
FROM demo.gold.game_metrics_1m
ORDER BY window_start DESC
LIMIT 100;

SELECT
    player_id,
    round_id,
    MAX(game_id) AS game_id,
    MAX(provider_id) AS provider_id,
    MAX(country) AS country,
    MAX(platform) AS platform,
    SUM(CASE WHEN event_type = 'BET' THEN amount ELSE 0 END) AS bet_amount,
    SUM(CASE WHEN event_type = 'WIN' THEN amount ELSE 0 END) AS payout_amount,
    CASE
        WHEN SUM(CASE WHEN event_type = 'CANCEL' THEN 1 ELSE 0 END) > 0 THEN 'CANCEL'
        WHEN SUM(CASE WHEN event_type = 'WIN' THEN 1 ELSE 0 END) > 0 THEN 'WIN'
        WHEN SUM(CASE WHEN event_type = 'LOSS' THEN 1 ELSE 0 END) > 0 THEN 'LOSS'
        ELSE 'UNSETTLED'
    END AS result,
    MIN(CASE WHEN event_type = 'BET' THEN event_time END) AS bet_time,
    MAX(CASE WHEN event_type IN ('WIN', 'LOSS', 'CANCEL') THEN event_time END) AS settlement_time
FROM demo.silver.game_transaction_events
GROUP BY player_id, round_id
ORDER BY bet_time DESC
LIMIT 100;

WITH round_status AS (
    SELECT
        player_id,
        round_id,
        MIN(CASE WHEN event_type = 'BET' THEN event_time END) AS bet_time,
        SUM(CASE WHEN event_type = 'BET' THEN 1 ELSE 0 END) AS bet_count,
        SUM(CASE WHEN event_type IN ('WIN', 'LOSS', 'CANCEL') THEN 1 ELSE 0 END) AS settlement_count
    FROM demo.silver.game_transaction_events
    GROUP BY player_id, round_id
)
SELECT
    player_id,
    round_id,
    bet_time,
    'UNSETTLED_BET_ALERT' AS alert_type,
    CURRENT_TIMESTAMP AS detected_time
FROM round_status
WHERE bet_count > 0
  AND settlement_count = 0
  AND bet_time < CURRENT_TIMESTAMP - INTERVAL 5 MINUTES;
