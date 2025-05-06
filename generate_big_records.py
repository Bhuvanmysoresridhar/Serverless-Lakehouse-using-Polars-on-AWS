import random
import json
from faker import Faker

fake = Faker()

records = []
customer_ids = list(range(1000, 1200))  # 200 unique customers

for _ in range(5000):
    customer_id = random.choice(customer_ids)  # Customers buying multiple times
    order_id = random.randint(100000, 999999)
    amount = round(random.uniform(10.0, 1000.0), 2)  # Varying order amounts
    order_date = fake.date_between(start_date='-2y', end_date='today').isoformat()

    record = {
        "customer_id": customer_id,
        "order_id": order_id,
        "amount": amount,
        "order_date": order_date
    }
    records.append(record)

# Save to a file
with open('big_5000_records.json', 'w') as f:
    for record in records:
        f.write(json.dumps(record) + '\n')

print(" Done: 5000+ records created in big_5000_records.json")
