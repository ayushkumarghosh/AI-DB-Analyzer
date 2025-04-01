CREATE TABLE sample_sales_data (
    Order_ID VARCHAR(10) PRIMARY KEY,
    Customer_Name VARCHAR(100) NOT NULL,
    Customer_Email VARCHAR(100) NOT NULL,
    Product VARCHAR(50) NOT NULL,
    Category VARCHAR(50) NOT NULL,
    Unit_Price DECIMAL(10,2) NOT NULL,
    Quantity INTEGER NOT NULL CHECK (Quantity > 0),
    Order_Date DATE NOT NULL,
    Region VARCHAR(50) NOT NULL,
    Shipping_Cost DECIMAL(10,2) NOT NULL,
    Payment_Method VARCHAR(20) NOT NULL,
    Order_Status VARCHAR(20) NOT NULL,
    Discount_Percentage INTEGER NOT NULL CHECK (Discount_Percentage >= 0 AND Discount_Percentage <= 100),
    Total_Amount DECIMAL(10,2) NOT NULL
);