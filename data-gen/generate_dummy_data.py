import os
from faker import Faker
import csv
import pandas as pd

fake = Faker()
output_dir = os.path.expanduser("~/hadoop-poc/data-gen/output")
os.makedirs(output_dir, exist_ok=True)

# sales_db - orders (CSV/text format)
print("Generating orders...")
with open(f"{output_dir}/orders.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "customer_id", "amount", "status", "order_date"])
    for i in range(5000):
        writer.writerow([
            i + 1,
            fake.random_int(min=1, max=1000),
            round(fake.random_number(digits=4) / 100, 2),
            fake.random_element(["completed", "pending", "cancelled"]),
            fake.date_between(start_date="-2y", end_date="today")
        ])

# sales_db - customers (Parquet format — simulating Kudu)
print("Generating customers...")
customers = [{
    "customer_id": i + 1,
    "name": fake.name(),
    "email": fake.email(),
    "region": fake.state(),
    "signup_date": str(fake.date_between(start_date="-3y", end_date="today"))
} for i in range(1000)]
pd.DataFrame(customers).to_parquet(f"{output_dir}/customers.parquet", index=False)

# inventory_db - stock (CSV/text format)
print("Generating stock...")
with open(f"{output_dir}/stock.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["item_id", "item_name", "quantity", "warehouse", "last_updated"])
    for i in range(3000):
        writer.writerow([
            i + 1,
            fake.word(),
            fake.random_int(min=0, max=500),
            fake.random_element(["WH-North", "WH-South", "WH-East", "WH-West"]),
            fake.date_between(start_date="-1y", end_date="today")
        ])

# inventory_db - suppliers (Parquet format — simulating Kudu)
print("Generating suppliers...")
suppliers = [{
    "supplier_id": i + 1,
    "name": fake.company(),
    "contact": fake.name(),
    "email": fake.company_email(),
    "country": fake.country()
} for i in range(500)]
pd.DataFrame(suppliers).to_parquet(f"{output_dir}/suppliers.parquet", index=False)

# hr_db - employees (CSV/text format)
print("Generating employees...")
with open(f"{output_dir}/employees.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["emp_id", "name", "department", "salary", "join_date"])
    for i in range(2000):
        writer.writerow([
            i + 1,
            fake.name(),
            fake.random_element(["Engineering", "Sales", "HR", "Finance", "Marketing"]),
            fake.random_int(min=40000, max=150000),
            fake.date_between(start_date="-5y", end_date="today")
        ])

print("Done! All files generated in:", output_dir)
