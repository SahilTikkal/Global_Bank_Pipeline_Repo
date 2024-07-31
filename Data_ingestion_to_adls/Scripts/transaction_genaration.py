import pandas as pd
import numpy as np
import random
import time
from sklearn.datasets import make_regression

class TransactionDataGenerator:
    def __init__(self, num_transactions_per_batch=50):
        self.num_transactions_per_batch = num_transactions_per_batch
        self.start_date = pd.Timestamp("2023-01-01")
        self.current_timestamp = self.start_date
        self.current_transaction_id = 5001
        self.batch_count = 0

    def generate_transactions(self, customer_ids, branch_ids):
        transaction_ids = [f"T{self.current_transaction_id + i:04d}" for i in range(self.num_transactions_per_batch)]
        self.current_transaction_id += self.num_transactions_per_batch

        X, y = make_regression(n_samples=self.num_transactions_per_batch, n_features=1, noise=10)
        amounts = np.clip(np.abs(y), 1, 10000)

        # Introduce outliers
        num_outliers = int(0.05 * len(amounts))
        outliers_indices = np.random.choice(len(amounts), num_outliers, replace=False)
        amounts[outliers_indices] = np.random.uniform(10000, 100000, size=num_outliers)
        amounts = np.round(amounts, 2)

        status_values = ["completed"] * (int(0.9 * self.num_transactions_per_batch)) + ["pending"] * int(0.1 * self.num_transactions_per_batch)
        random.shuffle(status_values)

        # Generate continuous timestamps
        transaction_times = [self.current_timestamp + pd.Timedelta(seconds=30 * i) for i in range(self.num_transactions_per_batch)]
        self.current_timestamp += pd.Timedelta(seconds=30 * self.num_transactions_per_batch)

        transactions_df = pd.DataFrame({
            "transaction_id": transaction_ids,
            "customer_id": random.choices(customer_ids, k=self.num_transactions_per_batch),
            "branch_id": random.choices(branch_ids, k=self.num_transactions_per_batch),
            "channel": random.choices(["ATM", "web", "mobile", "branch"], k=self.num_transactions_per_batch),
            "transaction_type": random.choices(["withdrawal", "deposit", "transfer", "payment"], k=self.num_transactions_per_batch),
            "amount": amounts,
            "currency": random.choices(["USD", "EUR", "GBP"], k=self.num_transactions_per_batch),
            "timestamp": transaction_times,
            "status": status_values
        })

        return transactions_df

# Main loop for generating and saving transaction data
transaction_data_gen = TransactionDataGenerator()

# Replace with your actual paths
customers_df = pd.read_csv(r"C:\Users\sahill\Downloads\Dataset\created Data\final\Customers.csv")
branches_df = pd.read_csv(r"C:\Users\sahill\Downloads\Dataset\created Data\final\Branches.csv")
customer_ids = customers_df['customer_id'].tolist()
branch_ids = branches_df['branch_id'].tolist()

batch_folder_path = r"C:\Users\sahill\Desktop\Big data\CapstoneProject\Data\Transactions"


while True:
    new_transactions_df = transaction_data_gen.generate_transactions(customer_ids, branch_ids)
    batch_file_path = f"{batch_folder_path}\\transactions_batch_{transaction_data_gen.batch_count}.csv"
    new_transactions_df.to_csv(batch_file_path, index=False)
    
    # Update the batch count
    transaction_data_gen.batch_count += 1

    print(f"Batch {transaction_data_gen.batch_count} saved to {batch_file_path}.")
    
    # Wait for 30 seconds before generating the next batch
    time.sleep(60)

