import pandas as pd
import random
import uuid
import datetime
from datetime import datetime, timedelta
import os
import logging
logging.basicConfig(format='%(levelname)s-%(message)s')

def random_timestamp_generator():
    now = datetime.utcnow()
    days = random.randint(0, 29)
    random_time = now - timedelta(days=days, hours=random.randint(0,23), minutes=random.randint(0, 59))
    return random_time

def random_order_generator():
    menu_file_path = 'C:/Users/acer/vscode_projects/coffehouse_chain_project/data/menu.csv'
    branch_file_path = 'C:/Users/acer/vscode_projects/coffehouse_chain_project/data/branches.csv'

    try:
        menu_df = pd.read_csv(menu_file_path)
    except FileNotFoundError:
        raise Exception(f"{menu_file_path} not found")
    
    try:
        branch_df = pd.read_csv(branch_file_path)
    except FileNotFoundError:
        raise Exception(f"{branch_file_path} not found")

    num_of_orders = random.randint(1, 5)
    row = menu_df.sample(num_of_orders)
    order_menu_df = row[["item_id", "item_name", "price"]]
    
    order_list = []

    for idx, row in order_menu_df.iterrows():
        quantity = random.randint(1,4)
        order_list.append({"item_id":row["item_id"], 
                            "item_name":row["item_name"], 
                            "price": row["price"],
                            "quantity":quantity, 
                            "amount": row["price"]*quantity})
        
    branch_list = branch_df["branch"].to_list()

    order = {
        "order_id":str(uuid.uuid4()),
        "timestamp":random_timestamp_generator(),
        "order_details":order_list,
        "branch":random.choice(branch_list),
        "total_order_amount": sum([item["amount"] for item in order_list]),
        "mode_of_payment": random.choice(["mobile banking", "debit card", "cash"])

    }
    return order

def generate_orders_batch(count=100):
    return [random_order_generator() for _ in range(count)]

def order_to_csv(orders, output_path='C:/Users/acer/Desktop/Datasets/coffee_house'):
    os.makedirs(output_path, exist_ok=True)
    filename = datetime.utcnow().strftime("orders_%Y%m%d_%H%M%S.csv")
    full_path = os.path.join(output_path, filename)

    df = pd.json_normalize(orders, record_path='order_details', meta=["order_id", "timestamp", "branch", "total_order_amount"])
    df.to_csv(full_path, index=False)
    logging.info(f"Saved {len(orders)} orders to {full_path}")

if __name__ == "__main__":
    batch_size = 500
    orders = generate_orders_batch(batch_size)
    order_to_csv(orders)

