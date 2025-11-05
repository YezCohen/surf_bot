import psycopg2
import sys
import data_fetcher
from dotenv import load_dotenv
import os

# טען משתני סביבה מקובץ .env
# נצטרך את סיסמת ה-DB וגם את ה-IP החיצוני
load_dotenv() 

DB_PASS = os.environ.get('DB_PASS')
DB_HOST = os.environ.get('VM_EXTERNAL_IP') # הוסף את זה ל-.env שלך
DB_USER = "surf_bot_user"
DB_NAME = "surf_bot_db"

def seed_beaches():
    print("Connecting to database...")
    conn = None
    try:
        # התחבר ל-DB (דרך האינטרנט, בזכות ה-Firewall שפתחנו)
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST 
        )
        print("Connection successful.")
        
        # 1. קבל את רשימת החופים
        print("Fetching beach list from data_fetcher...")
        beaches = data_fetcher.get_all_beaches()
        if not beaches:
            print("Failed to fetch beaches. Aborting.")
            return

        print(f"Fetched {len(beaches)} beaches. Seeding to database...")
        
        # 2. הכנס אותם ל-DB
        with conn.cursor() as cursor:
            # נשתמש ב-ON CONFLICT כדי שנוכל להריץ את זה שוב ושוב
            # וזה פשוט יעדכן את השם אם הוא השתנה
            insert_query = """
            INSERT INTO beaches (slug, name, last_updated)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (slug) DO UPDATE
            SET name = EXCLUDED.name, last_updated = CURRENT_TIMESTAMP;
            """
            
            # הפוך את רשימת ה-dicts לרשימה של tuples
            data_to_insert = [(b['slug'], b['name']) for b in beaches]
            
            # הרץ את הפקודה על כל הרשימה בבת אחת
            cursor.executemany(insert_query, data_to_insert)
            
            # בצע Commit
            conn.commit()
            print(f"Successfully seeded/updated {len(data_to_insert)} beaches.")

    except psycopg2.OperationalError as e:
        print(f"DB Connection Error: {e}", file=sys.stderr)
        print("Error: Could not connect. Did you add the VM_EXTERNAL_IP to your .env file?")
        print("And did you create the firewall rule for your local IP?")
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        if conn:
            conn.rollback() # בטל שינויים אם משהו נכשל
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    seed_beaches()