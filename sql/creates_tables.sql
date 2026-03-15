DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS portfolio_positions_reported;
DROP TABLE IF EXISTS cash_balances;

CREATE TABLE transactions (
    transaction_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    asset_symbol TEXT NOT NULL,
    transaction_type TEXT NOT NULL,
    quantity NUMERIC(18, 4) NOT NULL,
    price NUMERIC(18, 4) NOT NULL,
    transaction_date TIMESTAMP NOT NULL
);

CREATE TABLE portfolio_positions_reported (
    account_id TEXT NOT NULL,
    asset_symbol TEXT NOT NULL,
    reported_quantity NUMERIC(18, 4) NOT NULL,
    valuation_date DATE NOT NULL
);

CREATE TABLE cash_balances (
    account_id TEXT NOT NULL,
    cash_balance NUMERIC(18, 4) NOT NULL,
    balance_date DATE NOT NULL
);