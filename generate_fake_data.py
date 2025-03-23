import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

def generate_sample_sales_data(output_file="data/sample_sales_data.csv", num_rows=100):
    """
    Generate sample sales data and save it to a CSV file
    
    Parameters:
    output_file (str): Output CSV file path
    num_rows (int): Number of rows to generate
    """
    # Initialize Faker for realistic data
    fake = Faker()
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Define some sample data lists
    products = [
        "Laptop", "Desktop", "Monitor", "Keyboard", "Mouse",
        "Printer", "Tablet", "Smartphone", "Headphones", "Webcam"
    ]
    
    categories = ["Electronics", "Computers", "Accessories", "Mobile"]
    
    regions = ["North", "South", "East", "West"]
    
    # Generate data
    data = {
        "Order_ID": [f"ORD-{i:06d}" for i in range(1, num_rows + 1)],
        "Customer_Name": [fake.name() for _ in range(num_rows)],
        "Customer_Email": [fake.email() for _ in range(num_rows)],
        "Product": [random.choice(products) for _ in range(num_rows)],
        "Category": [random.choice(categories) for _ in range(num_rows)],
        "Unit_Price": [round(random.uniform(10.99, 999.99), 2) for _ in range(num_rows)],
        "Quantity": [random.randint(1, 10) for _ in range(num_rows)],
        "Order_Date": [(datetime(2023, 1, 1) + timedelta(days=random.randint(0, 730))) 
                      .strftime('%Y-%m-%d') for _ in range(num_rows)],
        "Region": [random.choice(regions) for _ in range(num_rows)],
        "Shipping_Cost": [round(random.uniform(5.99, 49.99), 2) for _ in range(num_rows)],
        "Payment_Method": [random.choice(["Credit Card", "PayPal", "Bank Transfer", "Cash"]) 
                         for _ in range(num_rows)],
        "Order_Status": [random.choice(["Completed", "Pending", "Shipped", "Cancelled"]) 
                        for _ in range(num_rows)],
        "Discount_Percentage": [random.choice([0, 5, 10, 15, 20]) 
                              for _ in range(num_rows)]
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Calculate Total_Amount
    df["Total_Amount"] = (df["Unit_Price"] * df["Quantity"] * 
                         (1 - df["Discount_Percentage"]/100) + 
                         df["Shipping_Cost"]).round(2)
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f"Generated sample data with {num_rows} rows and saved to {output_file}")
    
    # Display sample
    print("\nSample of the first 5 rows:")
    print(df.head().to_string())

def main():
    # Generate the sample data
    generate_sample_sales_data()

if __name__ == "__main__":
    main()