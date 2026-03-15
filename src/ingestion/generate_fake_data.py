import os
import random
from datetime import datetime

import numpy as np
import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)
np.random.seed(42)

RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

ASSETS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "JPM", "V"]
TRANSACTION_TYPES = ["BUY", "SELL"]


def generate_transactions(num_rows: int = 10000) -> pd.DataFrame:
    rows = []

    for _ in range(num_rows):
        quantity = round(random.uniform(1, 100), 2)
        price = round(random.uniform(50, 1000), 2)

        rows.append(
            {
                "transaction_id": fake.uuid4(),
                "account_id": f"ACC_{random.randint(1, 50):03d}",
                "asset_symbol": random.choice(ASSETS),
                "transaction_type": random.choice(TRANSACTION_TYPES),
                "quantity": quantity,
                "price": price,
                "transaction_date": fake.date_time_between(
                    start_date="-1y", end_date="now"
                ),
            }
        )

    return pd.DataFrame(rows)


def generate_reported_positions(transactions_df: pd.DataFrame) -> pd.DataFrame:
    tx = transactions_df.copy()

    tx["signed_quantity"] = np.where(
        tx["transaction_type"] == "BUY",
        tx["quantity"],
        -tx["quantity"],
    )

    reported = (
        tx.groupby(["account_id", "asset_symbol"], as_index=False)["signed_quantity"]
        .sum()
        .rename(columns={"signed_quantity": "reported_quantity"})
    )

    reported["valuation_date"] = pd.Timestamp(datetime.today().date())

    if len(reported) > 10:
        sampled_idx = reported.sample(frac=0.1, random_state=42).index
        reported.loc[sampled_idx, "reported_quantity"] = (
            reported.loc[sampled_idx, "reported_quantity"] + np.random.uniform(-5, 5, len(sampled_idx))
        ).round(2)

    return reported


def generate_cash_balances(num_accounts: int = 50) -> pd.DataFrame:
    rows = []

    for i in range(1, num_accounts + 1):
        rows.append(
            {
                "account_id": f"ACC_{i:03d}",
                "cash_balance": round(random.uniform(1000, 250000), 2),
                "balance_date": datetime.today().date(),
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    transactions_df = generate_transactions(10000)
    positions_df = generate_reported_positions(transactions_df)
    cash_df = generate_cash_balances(50)

    transactions_path = os.path.join(RAW_DIR, "transactions.csv")
    positions_path = os.path.join(RAW_DIR, "portfolio_positions_reported.csv")
    cash_path = os.path.join(RAW_DIR, "cash_balances.csv")

    transactions_df.to_csv(transactions_path, index=False)
    positions_df.to_csv(positions_path, index=False)
    cash_df.to_csv(cash_path, index=False)

    print(f"Generated: {transactions_path} ({len(transactions_df)} rows)")
    print(f"Generated: {positions_path} ({len(positions_df)} rows)")
    print(f"Generated: {cash_path} ({len(cash_df)} rows)")


if __name__ == "__main__":
    main()