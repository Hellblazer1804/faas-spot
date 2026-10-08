import os
import pandas as pd
import mysql.connector

# --- Configuration ---
DB_CONFIG = {
    'user':     os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', ''),
    'host':     os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', ''),
}
# Point this to the directory containing your raw trace CSV files
TRACE_DIR = "./spot-traces-csv"
TABLE_NAME = "spot_traces"

def main():
    """Parses all single-column trace CSVs and loads their data into a MySQL table."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ Connected to MySQL database.")

        # --- KEY CHANGE: Simplified table schema to match your trace data ---
        # The 'creation_timestamp' column has been removed.
        print(f"Ensuring table '{TABLE_NAME}' exists...")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                az VARCHAR(255),
                instance_type VARCHAR(255),
                lifetime_minutes FLOAT
            )
        """)
        
        # Clear existing data to ensure a fresh import
        cursor.execute(f"TRUNCATE TABLE {TABLE_NAME}")
        conn.commit()
        print(f"🗑️  Cleared any old data from '{TABLE_NAME}'.")

        # Find and process all raw trace CSV files in the directory
        for filename in os.listdir(TRACE_DIR):
            if filename.endswith(".csv") and "_cdf" not in filename and "_cost" not in filename:
                az, instance_type = filename.replace(".csv", "").split('_', 1)
                file_path = os.path.join(TRACE_DIR, filename)
                
                print(f"Processing '{file_path}'...")

                # --- KEY CHANGE: Read the single-column CSV ---
                # This assumes the first line is the header 'Lifetime'
                df = pd.read_csv(file_path)

                if 'Lifetime' not in df.columns:
                    print(f"  ⚠️ 'Lifetime' column not in {filename}. Skipping.")
                    continue

                # Add columns for AZ and instance type, parsed from the filename
                df['az'] = az
                df['instance_type'] = instance_type

                # --- KEY CHANGE: Convert lifetime from seconds (in file) to minutes (for DB) ---
                df['lifetime_minutes'] = df['Lifetime'] / 60.0
                
                # Convert the relevant columns into a list of tuples for insertion
                records_to_insert = [
                    tuple(row) for row in df[['az', 'instance_type', 'lifetime_minutes']].to_numpy()
                ]
                
                # Use executemany for a fast bulk insert
                sql = f"INSERT INTO {TABLE_NAME} (az, instance_type, lifetime_minutes) VALUES (%s, %s, %s)"
                cursor.executemany(sql, records_to_insert)
                conn.commit()
                print(f"  -> Inserted {cursor.rowcount} records for {az}/{instance_type}.")

        cursor.close()
        conn.close()
        print("\n🎉 Successfully loaded all traces into the database!")

    except mysql.connector.Error as err:
        print(f"❌ A database error occurred: {err}")
    except FileNotFoundError:
        print(f"❌ Error: The trace directory was not found at '{TRACE_DIR}'")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()