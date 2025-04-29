"""
Simple E-commerce Event Data Generator
Creates fake e-commerce events and saves them to CSV files.
Uses proper logging instead of print statements.
"""
import os
import csv
import random
import uuid
import logging
from datetime import datetime, timedelta
from faker import Faker

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('ecommerce_generator')

# Initialize Faker
fake = Faker()

def generate_data(output_dir="ecommerce_data", num_events=1000):
    """Generate e-commerce event data and save to CSV files"""
    
    logger.info(f"Starting e-commerce data generation: {num_events} events")
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.debug(f"Created output directory: {output_dir}")
    
    # Define event types and their weights for random selection
    event_types = ["view", "add_to_cart", "purchase"]
    event_weights = [0.7, 0.2, 0.1]  # 70% views, 20% add to cart, 10% purchases
    
    # Generate some user IDs and product data for consistency
    logger.debug("Generating user IDs and product data")
    user_ids = [str(uuid.uuid4()) for _ in range(50)]
    
    #Generates products
    products = []
    for _ in range(30):
        products.append({
            "product_id": str(uuid.uuid4()),
            "product_name": fake.commerce.product_name(),
            "category": fake.commerce.department(),
            "price": float(fake.commerce.price(minimum=5, maximum=1000).replace(',', '.'))
        })
    
    # Generate events
    logger.info(f"Generating {num_events} e-commerce events...")
    events = []
    
    # Start time: 7 days ago
    start_time = datetime.now() - timedelta(days=7)
    
    for i in range(num_events):
        if i % 250 == 0 and i > 0:
            logger.debug(f"Generated {i} events so far")
            
        # Random timestamp within the last week
        timestamp = start_time + timedelta(
            seconds=random.randint(0, 7 * 24 * 60 * 60)
        )
        
        # Select a random user
        user_id = random.choice(user_ids)
        
        # Select a random product
        product = random.choice(products)
        
        # Select event type based on weighted probability
        event_type = random.choices(event_types, weights=event_weights)[0]
        
        # Create the event
        event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "user_id": user_id,
            "event_type": event_type,
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "category": product["category"],
            "price": product["price"]
        }
        
        # Add quantity for cart adds and purchases
        if event_type in ["add_to_cart", "purchase"]:
            event["quantity"] = random.randint(1, 5)
        else:
            event["quantity"] = ""
            
        events.append(event)
    
    # Sort events by timestamp
    logger.debug("Sorting events by timestamp")
    events.sort(key=lambda x: x["timestamp"])
    
    # Define CSV fields
    fields = ["event_id", "timestamp", "user_id", "event_type", 
              "product_id", "product_name", "category", "price", "quantity"]
    
    # Save to all_events.csv
    all_events_path = os.path.join(output_dir, "all_events.csv")
    with open(all_events_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(events)
    
    logger.info(f"Saved {len(events)} events to {all_events_path}")
    
    # Split into 5 batches to simulate streaming data
    batch_size = len(events) // 5
    for i in range(5):
        start_idx = i * batch_size
        end_idx = start_idx + batch_size if i < 4 else len(events)
        batch_events = events[start_idx:end_idx]
        
        batch_path = os.path.join(output_dir, f"events_batch_{i+1}.csv")
        with open(batch_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            writer.writerows(batch_events)
        
        logger.info(f"Saved batch {i+1} with {len(batch_events)} events to {batch_path}")
    
    logger.info("Data generation complete!")
    logger.info(f"Files saved in '{output_dir}/' directory")
    
    return events

