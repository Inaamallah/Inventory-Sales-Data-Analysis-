import sqlite3
import logging
import time
import pandas as pd
from Ingestion_db_script import ingest_db
import os


if not os.path.exists("logs"):
    os.makedirs("logs")


vendor_logger = logging.getLogger("vendor_summary")
vendor_logger.setLevel(logging.DEBUG)  


vendor_logger.propagate = False


if not vendor_logger.handlers:
  
    fh = logging.FileHandler("logs/vendor_summary.log", mode="a")  # append mode
    fh.setLevel(logging.DEBUG)  # log all levels
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

   
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)  
    ch.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))

    
    vendor_logger.addHandler(fh)
    vendor_logger.addHandler(ch)




def get_vendor_summary_table(conn):
    '''This function will create an aggregated table with relevant columns from all the tables'''

    start = time.time()
    vendor_sales_summary = pd.read_sql_query("""
    WITH FreightSummary AS (
        SELECT VendorNumber, SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    SalesSummary AS (
        SELECT 
            VendorNo,
            Brand,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """, conn)

    end = time.time()
    final_time = (end-start) * 100

    vendor_logger.info(f'Total time taken to create table: {final_time} ms')
    return vendor_sales_summary

def clean_data(df):
    start = time.time()
    
    # Changing volume datatype to float
    df['Volume'] = df['Volume'].astype('float64')

    # Filling all null values by 0
    df.fillna(0, inplace=True)

    # Removing white spaces
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    
    # Creating new columns
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SaletoPuchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

    end = time.time()
    final_time = (end-start) * 100

    vendor_logger.info(f'Total time taken to clean table: {final_time} ms')
    return df

if __name__ == '__main__':
    conn = sqlite3.connect('inventory.db', timeout=30)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    conn.commit()
    cursor.close()


    vendor_logger.info(f"{'-'*50} Creating Vendor Summary Table {'-'*50}")
    summary_df = get_vendor_summary_table(conn)
    vendor_logger.info(summary_df.head())

    vendor_logger.info(f"{'-'*50} Cleaning Table {'-'*50}")
    clean_df = clean_data(summary_df)
    vendor_logger.info(clean_df.head())

    vendor_logger.info(f"{'-'*50} Ingesting Data {'-'*50}")
    ingest_db(clean_df, 'vendor_sales_summary', conn)
    vendor_logger.info(f"{'-'*50} Completed {'-'*50}")
