import os
import csv
import uuid
import random
import logging
from datetime import datetime, timedelta
from faker import Faker

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ecommerce_gen")

# Faker instance
fake = Faker()

def generate_events(output_dir="ecommerce_data", num_events=100):
    os.makedirs(output_dir, exist_ok=True)

    # Sample data pools
    user_ids = [str(uuid.uuid4()) for _ in range(30)]
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports", "Beauty"]
    products = [{
        "product_id": str(uuid.uuid4()),
        "product_name": f"{fake.word().capitalize()} {fake.word().capitalize()}",
        "category": random.choice(categories),
        "price": round(random.uniform(10, 500), 2)
    } for _ in range(20)]

    event_types = ["view", "add_to_cart", "purchase"]
    weights = [0.7, 0.2, 0.1]
    start_time = datetime.now() - timedelta(days=7)
    events = []
     
     
    # Generate events
    for _ in range(num_events):
        product = random.choice(products)
        event_type = random.choices(event_types, weights)[0]
        events.append({
            "event_id": str(uuid.uuid4()),
            "timestamp": (start_time + timedelta(seconds=random.randint(0, 7 * 86400))).strftime("%Y-%m-%d %H:%M:%S"),
            "user_id": random.choice(user_ids),
            "event_type": event_type,
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "category": product["category"],
            "price": product["price"],
            "quantity": random.randint(1, 5) if event_type in ["add_to_cart", "purchase"] else ""
        })

    # Save to uniquely-named CSV file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_path = os.path.join(output_dir, f"batch_{timestamp}.csv")
    fields = list(events[0].keys())

    with open(batch_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(events)

    logger.info(f"Saved {num_events} events to {batch_path}")


if __name__ == "__main__":
    generate_events()
