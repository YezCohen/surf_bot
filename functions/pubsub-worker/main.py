import os
import json
import base64
import sys
import threading
import requests
import psycopg2
import psycopg2.pool
from flask import Flask, request, Response

# ייבוא מקומי של הסקרייפר
import data_fetcher 

app = Flask(__name__)

# --- Configuration (Loaded from Environment) ---
# (ללא שינוי)
WHATSAPP_API_TOKEN = os.environ.get('WHATSAPP_API_TOKEN')
PHONE_NUMBER_ID = os.environ.get('PHONE_NUMBER_ID')

DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', 5432)

# --- Global Clients (Lazy Loading) ---
# (ללא שינוי)
db_pool = None
db_pool_lock = threading.Lock()

# --- DB Getter (Lazy) ---
# (פונקציית get_db_pool ללא שינוי)
def get_db_pool():
    global db_pool
    if db_pool:
        return db_pool
    
    with db_pool_lock:
        if db_pool:
            return db_pool
        
        print("Database pool not initialized. Creating new pool...")
        try:
            db_pool = psycopg2.pool.SimpleConnectionPool(
                1, 2, # Min/max connections
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                host=DB_HOST,
                port=DB_PORT,
                connect_timeout=5
            )
            print("Database connection pool initialized successfully.")
            return db_pool
        except Exception as e:
            print(f"Error initializing database pool: {e}", file=sys.stderr)
            db_pool = None
            return None

# --- פונקציות עזר קיימות (ללא שינוי) ---

def get_all_beaches_from_db():
    pool = get_db_pool()
    if not pool: return None
    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute("SELECT name FROM beaches ORDER BY name ASC;")
            results = cursor.fetchall()
            beach_names = [result[0] for result in results]
            return beach_names
    except Exception as e:
        print(f"Database get_all_beaches failed: {e}", file=sys.stderr)
        return None
    finally:
        if conn:
            pool.putconn(conn)

def find_beach_slug(beach_name_query):
    pool = get_db_pool()
    if not pool: return None
    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cursor:
            sql_exact = "SELECT slug, name FROM beaches WHERE name ILIKE %s OR slug ILIKE %s;"
            cursor.execute(sql_exact, (beach_name_query, beach_name_query))
            result = cursor.fetchone()
            if result:
                return result # מחזירים (slug, name)

            sql_partial = "SELECT slug, name FROM beaches WHERE name ILIKE %s OR slug ILIKE %s;"
            query_param = f"%{beach_name_query}%" 
            cursor.execute(sql_partial, (query_param, query_param))
            result = cursor.fetchone()
            
            if result:
                return result # מחזירים (slug, name)
            else:
                return None
    except Exception as e:
        print(f"Database beach lookup failed: {e}", file=sys.stderr)
        return None
    finally:
        if conn:
            pool.putconn(conn)

# --- פונקציות חדשות לניהול מועדפים (ללא שינוי) ---

def add_favorite(phone_number, beach_slug):
    pool = get_db_pool()
    if not pool: return False
    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cursor:
            sql = """
            INSERT INTO favorites (phone_number, beach_slug)
            VALUES (%s, %s)
            ON CONFLICT (phone_number, beach_slug) DO NOTHING;
            """
            cursor.execute(sql, (phone_number, beach_slug))
            conn.commit()
            return True
    except Exception as e:
        print(f"Database add_favorite failed: {e}", file=sys.stderr)
        if conn: conn.rollback()
        return False
    finally:
        if conn:
            pool.putconn(conn)

def get_favorites_for_user(phone_number):
    pool = get_db_pool()
    if not pool: return []
    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cursor:
            sql = """
            SELECT f.beach_slug, b.name 
            FROM favorites f
            JOIN beaches b ON f.beach_slug = b.slug
            WHERE f.phone_number = %s;
            """
            cursor.execute(sql, (phone_number,))
            results = cursor.fetchall()
            return results
    except Exception as e:
        print(f"Database get_favorites failed: {e}", file=sys.stderr)
        return []
    finally:
        if conn:
            pool.putconn(conn)

# --- ✨ פונקציה חדשה: הודעת עזרה ---
def get_help_message():
    """
    מחזיר את הודעת העזרה הסטנדרטית
    """
    return """
אלו הפקודות שאני מכיר:

🌊 *שם של חוף* (למשל 'דדו')
   - יחזיר לך את התחזית לחוף המבוקש.

⭐ *מועדפים*
   - יחזיר לך תחזית מקוצרת לכל החופים ששמרת.

➕ *הוסף [שם חוף]* (למשל 'הוסף בת גלים')
   - יוסיף את החוף למועדפים שלך.

📋 *רשימת חופים*
   - יציג לך את כל החופים שאני מכיר.

❓ *עזרה*
   - יציג את ההודעה הזו שוב.
"""

# --- פונקציה ראשית מעודכנת (נתב פקודות) ---

@app.route('/', methods=['POST'])
def process_pubsub_message():
    """
    Endpoint that receives push messages from Pub/Sub.
    Acts as a router for different user commands.
    """
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        return Response("Bad Request", status=400)

    try:
        # 1. ניתוח הודעת ה-Pub/Sub
        message_data = base64.b64decode(envelope['message']['data']).decode('utf-8')
        job_data = json.loads(message_data)
        
        phone_number = job_data['phone_number']
        message_text = job_data['message_text'].strip().lower()
        
        print(f"Worker received job: Phone={phone_number}, Msg={message_text}")
        
        reply_text = ""

        # --- 2. ✨ נתב פקודות משודרג ---

        # פקודה: "מועדפים"
        if message_text in ["מועדפים", "המועדפים שלי", "favorites", "my favorites"]:
            print("Handling 'get favorites' command...")
            favorites = get_favorites_for_user(phone_number)
            if not favorites:
                reply_text = "עדיין לא הוספת חופים למועדפים. 🏖️\nכדי להוסיף, שלח הודעה כמו 'הוסף דדו'."
            else:
                reply_text = "עדכון יומי למועדפים שלך: 🌊\n"
                for beach_slug, beach_name in favorites:
                    forecast = data_fetcher.get_forecast(beach_slug)
                    if forecast:
                        today = forecast[0]
                        reply_text += f"\n--- {beach_name} ({today['day_name']}) ---\n"
                        # ✨ תיקון הבאג שלך - הוספנו :00
                        for hour_data in today['hourly_forecast']:
                            if hour_data['time'] in ["09", "12"]:
                                reply_text += f"  {hour_data['time']}: גלים {hour_data['wave_height']}, ים {hour_data['sea_description']}\n"
                    else:
                        reply_text += f"\n- לא הצלחתי להביא תחזית ל{beach_name}.\n"

        # פקודה: "הוסף X"
        elif message_text.startswith("הוסף ") or message_text.startswith("add "):
            beach_name_to_add = message_text.replace("הוסף ", "").replace("add ", "").strip()
            print(f"Handling 'add favorite' command for: {beach_name_to_add}")
            
            beach_result = find_beach_slug(beach_name_to_add)
            if beach_result:
                beach_slug, beach_name = beach_result
                if add_favorite(phone_number, beach_slug):
                    reply_text = f"הוספתי את '{beach_name}' למועדפים שלך! 👍"
                else:
                    reply_text = f"אירעה שגיאה בניסיון להוסיף את '{beach_name}'."
            else:
                reply_text = f"מצטער, לא מצאתי חוף בשם '{beach_name_to_add}'."

        # ✨ פקודה חדשה: "רשימת חופים"
        elif message_text in ["רשימת חופים", "list beaches"]:
            print("Handling 'list beaches' command...")
            beach_list = get_all_beaches_from_db()
            if beach_list:
                reply_text = "אלו החופים הזמינים שאני מכיר:\n\n"
                reply_text += "\n".join(beach_list)
            else:
                reply_text = "מצטער, אירעה שגיאה בניסיון לשלוף את רשימת החופים."

        # ✨ פקודה חדשה: "עזרה"
        elif message_text in ["עזרה", "help"]:
             print("Handling 'help' command...")
             reply_text = get_help_message()

        # פקודה: ברירת מחדל (נסיון חיפוש חוף)
        else:
            print(f"Handling 'find beach' (default) command for: {message_text}")
            beach_result = find_beach_slug(message_text)
            
            if not beach_result:
                # ✨ שינוי: אם לא מצאנו, שלח הודעת עזרה
                print(f"Could not find beach. Sending help message.")
                reply_text = f"מצטער, לא זיהיתי את הפקודה '{message_text}'.\n"
                reply_text += get_help_message()
            
            else:
                # --- ✨ שינוי מרכזי: תחזית ל-3 ימים ---
                beach_slug, beach_name = beach_result
                print(f"Found beach: '{beach_slug}'. Fetching forecast...")
                forecast_list = data_fetcher.get_forecast(beach_slug)
                
                if not forecast_list:
                    reply_text = "מצטער, לא הצלחתי להביא את התחזית עבור החוף הזה כרגע."
                else:
                    # בונה את התשובה עבור 3 ימים
                    reply_text = f"התחזית לחוף '{beach_name}' ל-3 הימים הקרובים:\n"
                    
                    # לולאה שעוברת על 3 הימים הראשונים שקיבלנו
                    for day_forecast in forecast_list[:3]:
                        reply_text += f"\n--- {day_forecast['day_name']} ---"
                        
                        if not day_forecast['hourly_forecast']:
                            reply_text += " (אין נתונים זמינים)\n"
                            continue # עבור ליום הבא
                        
                        # הוסף את כל השעות שגרדנו (06, 09, 12)
                        for hour_data in day_forecast['hourly_forecast']:
                            reply_text += f"\n  {hour_data['time']}: גלים {hour_data['wave_height']}, ים {hour_data['sea_description']}"
                        
                        reply_text += "\n" # רווח בין הימים

        # --- 3. שלח את התשובה הסופית ---
        print(f"Sending reply to {phone_number}...")
        send_whatsapp_message(phone_number, reply_text)

        return Response("OK", status=200)

    except Exception as e:
        print(f"Error processing Pub/Sub message: {e}", file=sys.stderr)
        return Response("Internal Server Error", status=500)


def send_whatsapp_message(to_phone, message_text):
    """
    Sends a message back to the user via the Meta Graph API.
    """
    if not WHATSAPP_API_TOKEN or not PHONE_NUMBER_ID:
        print("WhatsApp API tokens not set.", file=sys.stderr)
        return

    url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_API_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "text",
        "text": { "body": message_text }
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        print(f"Message sent to {to_phone}: {response.json()}")
    except requests.RequestException as e:
        print(f"Error sending message: {e}", file=sys.stderr)

# --- Local Testing Block ---
if __name__ == "__main__":
    print("This service is designed to be run in Cloud Run and triggered by Pub/Sub.")
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8081)))