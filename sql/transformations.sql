DROP TABLE IF EXISTS stg_transactions;
DROP TABLE IF EXISTS mart_portfolio_exposure;
DROP TABLE IF EXISTS mart_reconciliation_summary;
DROP TABLE IF EXISTS mart_account_cash_summary;

CREATE TABLE stg_transactions AS
SELECT
    transaction_id,
    TRIM(account_id) AS account_id,
    UPPER(TRIM(asset_symbol)) AS asset_symbol,
    UPPER(TRIM(transaction_type)) AS transaction_type,
    CAST(quantity AS NUMERIC(18, 4)) AS quantity,
    CAST(price AS NUMERIC(18, 4)) AS price,
    CAST(quantity * price AS NUMERIC(18, 4)) AS gross_amount,
    CAST(transaction_date AS TIMESTAMP) AS transaction_date,
    CAST(transaction_date AS DATE) AS transaction_day
FROM transactions
WHERE
    transaction_id IS NOT NULL
    AND account_id IS NOT NULL
    AND asset_symbol IS NOT NULL
    AND transaction_type IN ('BUY', 'SELL')
    AND quantity > 0
    AND price > 0;

CREATE TABLE mart_portfolio_exposure AS
SELECT
    c.account_id,
    c.asset_symbol,
    c.computed_quantity,
    COALESCE(
        AVG(NULLIF(t.price, 0)),
        0
    ) AS avg_traded_price,
    (c.computed_quantity * COALESCE(AVG(NULLIF(t.price, 0)), 0)) AS estimated_market_value
FROM portfolio_positions_computed c
LEFT JOIN stg_transactions t
    ON c.account_id = t.account_id
    AND c.asset_symbol = t.asset_symbol
GROUP BY
    c.account_id,
    c.asset_symbol,
    c.computed_quantity;

CREATE TABLE mart_reconciliation_summary AS
SELECT
    account_id,
    COUNT(*) AS assets_compared,
    SUM(CASE WHEN status = 'MATCH' THEN 1 ELSE 0 END) AS matched_assets,
    SUM(CASE WHEN status = 'MISMATCH' THEN 1 ELSE 0 END) AS mismatched_assets,
    ROUND(AVG(abs_difference), 4) AS avg_abs_difference,
    ROUND(MAX(abs_difference), 4) AS max_abs_difference
FROM reconciliation_results
GROUP BY account_id;

CREATE TABLE mart_account_cash_summary AS
SELECT
    cb.account_id,
    cb.cash_balance,
    cb.balance_date,
    COALESCE(SUM(mpe.estimated_market_value), 0) AS estimated_portfolio_value,
        (cb.cash_balance + COALESCE(SUM(mpe.estimated_market_value), 0)) AS estimated_total_account_value
FROM cash_balances cb
LEFT JOIN mart_portfolio_exposure mpe
    ON cb.account_id = mpe.account_id
GROUP BY
    cb.account_id,
    cb.cash_balance,
    cb.balance_date;