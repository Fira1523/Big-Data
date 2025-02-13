# =====================================
# Import Required Libraries
# =====================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, current_date
import duckdb
import os
import glob
# =====================================
# Extraction Section (Load Data into PySpark DataFrames)
# =====================================
# Create a Spark session
spark = SparkSession.builder.appName("Olist Data Extraction").getOrCreate()
# Define the path to your Olist_main folder
data_path = r'C:\Users\divic\Desktop\Olist_main\\'
# Load datasets into PySpark DataFrames
datasets = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv"
}
dataframes = {name: spark.read.csv(data_path + filename, header=True, inferSchema=True) for name, filename in datasets.items()}

# =====================================
# Transformation Section (Data Cleaning & Processing)
# =====================================
def explore_dataframe(df, name):
    """Prints schema, row count, and sample data for a DataFrame."""
    print(f"\nExploring {name} DataFrame:")
    df.printSchema()
    print(f"Number of rows in {name}: {df.count()}")
    df.describe().show()
    df.show(5)
    duplicate_count = df.count() - df.dropDuplicates().count()
    print(f"Number of duplicate rows in {name}: {duplicate_count}")
    for column in df.columns:
        null_count = df.filter(df[column].isNull()).count()
        print(f"Number of null entries in {column}: {null_count}")
# Explore each DataFrame
for name, df in dataframes.items():
    explore_dataframe(df, name)
def handle_duplicates(df, name):
    """Removes duplicate rows from a DataFrame."""
    duplicate_count = df.count() - df.dropDuplicates().count()
    if duplicate_count > 0:
        df = df.dropDuplicates()
        print(f"Removed {duplicate_count} duplicates from {name}. New row count: {df.count()}")
    return df
def handle_nulls(df, name):
    """Handles null values in a DataFrame."""
    for column in df.columns:
        null_count = df.filter(df[column].isNull()).count()
        if null_count > 0:
            if df.schema[column].dataType == 'string':
                df = df.fillna({column: 'Unknown'})
            elif df.schema[column].dataType in ['integer', 'double']:
                df = df.fillna({column: 0})
            print(f"Filled {null_count} null values in {column} of {name}.")
    return df
# Apply transformations to each DataFrame
for name in dataframes.keys():
    dataframes[name] = handle_duplicates(dataframes[name], name)
    dataframes[name] = handle_nulls(dataframes[name], name)
# Format data types for `customers`
dataframes["customers"] = dataframes["customers"].withColumn("customer_zip_code_prefix", col("customer_zip_code_prefix").cast("integer"))
# Format dates for `orders`
dataframes["orders"] = dataframes["orders"].withColumn("order_purchase_timestamp", to_date(col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("order_approved_at", to_date(col("order_approved_at"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("order_delivered_carrier_date", to_date(col("order_delivered_carrier_date"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date"), "yyyy-MM-dd"))
# Format data types for `order_items`
dataframes["order_items"] = dataframes["order_items"].withColumn("price", col("price").cast("double")) \
    .withColumn("freight_value", col("freight_value").cast("double"))
# Remove future dates and handle negative prices
dataframes["orders"] = dataframes["orders"].filter(col("order_purchase_timestamp") <= current_date())
dataframes["order_items"] = dataframes["order_items"].withColumn("price", when(col("price") < 0, 0).otherwise(col("price")))
# Format & clean `products`
dataframes["products"] = dataframes["products"] \
    .withColumn("product_weight_g", col("product_weight_g").cast("double")) \
    .withColumn("product_length_cm", col("product_length_cm").cast("integer")) \
    .withColumn("product_height_cm", col("product_height_cm").cast("integer")) \
    .withColumn("product_width_cm", col("product_width_cm").cast("integer"))

# =====================================
# Load Data into Permanent CSV Files
# =====================================

output_directory = data_path  # Save in the same folder as the original data

def save_data_to_csv(dataframes, output_dir):
    """Saves DataFrames as CSV files."""
    os.makedirs(output_dir, exist_ok=True)

    for name, df in dataframes.items():
        try:
            csv_path = os.path.join(output_dir, name)
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
            print(f"Saved {name} to {csv_path}")
        except Exception as e:
            print(f"Error saving {name} to CSV: {e}")

# Save cleaned DataFrames
save_data_to_csv(dataframes, output_directory)

# =====================================
# Load Data into DuckDB from CSV
# =====================================

def load_data_to_duckdb(output_dir):
    """Loads CSV data into DuckDB while handling formatting errors."""
    conn = duckdb.connect('olist_main_data.db')

    table_definitions = {
        "customers": "customer_id STRING PRIMARY KEY, customer_unique_id STRING, customer_zip_code_prefix INTEGER, customer_city STRING, customer_state STRING",
        "geolocation": "geolocation_zip_code_prefix INTEGER, geolocation_lat DOUBLE, geolocation_lng DOUBLE, geolocation_city STRING, geolocation_state STRING",
        "products": """ 
            product_id STRING PRIMARY KEY, 
            product_category_name STRING, 
            product_name_lenght INTEGER, 
            product_description_lenght INTEGER, 
            product_photos_qty INTEGER, 
            product_weight_g DOUBLE, 
            product_length_cm INTEGER, 
            product_height_cm INTEGER, 
            product_width_cm INTEGER
        """,
        "sellers": "seller_id STRING PRIMARY KEY, seller_zip_code_prefix INTEGER, seller_city STRING, seller_state STRING",
        "category_translation": "product_category_name STRING PRIMARY KEY, product_category_name_english STRING",
        "orders": "order_id STRING PRIMARY KEY, customer_id STRING, order_status STRING, order_purchase_timestamp TIMESTAMP, order_approved_at TIMESTAMP, order_delivered_carrier_date TIMESTAMP, order_delivered_customer_date TIMESTAMP, order_estimated_delivery_date TIMESTAMP",
        "order_items": "order_item_id STRING PRIMARY KEY, order_id STRING, product_id STRING, seller_id STRING, price DOUBLE, freight_value DOUBLE, quantity INTEGER"
    }
    # Create tables if not exist
    for table in table_definitions.keys():
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(f"CREATE TABLE {table} ({table_definitions[table]})")
    # Loop through each table and load corresponding data
    for name in table_definitions.keys():
        folder_path = os.path.join(output_dir, name)
        csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
        if csv_files:
            csv_path = csv_files[0]  # Getting the first file in the folder
            print(f"Loading {name} from {csv_path} into DuckDB...")

            # Use DuckDB's COPY command to load data into tables
            conn.execute(f"""
                COPY {name} FROM '{csv_path}' 
                (FORMAT CSV, HEADER TRUE, 
                IGNORE_ERRORS TRUE,  -- Skip faulty rows
                STRICT_MODE FALSE,   -- Allow slight mismatches
                NULL_PADDING TRUE)   -- Fill missing values with NULL
            """)
        else:
            print(f"⚠️ Warning: No CSV file found for {name} in {folder_path}, skipping!")
    conn.close()
    print("✅ Data successfully loaded into DuckDB.")
# Run the function to load data into DuckDB
load_data_to_duckdb("C:\\Users\\divic\\Desktop\\Olist_main")

# =====================================
# Interact with DuckDB
# =====================================

# Connect to DuckDB
conn = duckdb.connect('olist_main_data.db')

# Example Query: Check the tables in the database
tables = conn.execute("SHOW TABLES").fetchall()
print("Tables in the database:", tables)

# Example Query: Fetch some data from one of the tables
customers = conn.execute("SELECT * FROM customers LIMIT 5").fetchall()
print("Sample Data from 'customers' table:", customers)

# Example Query: Get the count of records in each table
tables_with_counts = conn.execute("""
    SELECT table_name, COUNT(*) 
    FROM information_schema.tables 
    WHERE table_schema='main'
    GROUP BY table_name
""").fetchall()

print("Record counts for each table:", tables_with_counts)

# Close the connection after you're done
conn.close()
