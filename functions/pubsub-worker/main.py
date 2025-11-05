import os
import json
import base64
import sys
import threading
import requests
import psycopg2
import psycopg2.pool
from flask import Flask, request, Response

import data_fetcher 

app = Flask(__name__)

# --- Configuration (Loaded from Environment) ---
WHATSAPP_API_TOKEN = os.environ.get('WHATSAPP_API_TOKEN')
PHONE_NUMBER_ID = os.environ.get('PHONE_NUMBER_ID')

DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', 5432)

# --- Global Clients (Initialized Lazily) ---
db_pool = None
db_pool_lock = threading.Lock()

def get_db_pool():
    """
    Lazily initializes and returns a thread-safe database connection pool.
    """
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

@app.route('/', methods=['POST'])
def process_pubsub_message():
    """
    Endpoint that receives push messages from Pub/Sub.
    """
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        print("Received invalid Pub/Sub message.", file=sys.stderr)
        return Response("Bad Request", status=400)

    try:
        # Extract the Pub/Sub message
        # The message data is base64-encoded
        pubsub_message = envelope['message']
        message_data = base64.b64decode(pubsub_message['data']).decode('utf-8')
        job_data = json.loads(message_data)
        
        phone_number = job_data['phone_number']
        message_text = job_data['message_text']
        
        print(f"Worker received job: Phone={phone_number}, Msg={message_text}")

        # 2. הרצת הלוגיקה האמיתית
        # --- כאן נכניס את כל הלוגיקה העתידית ---
        # (למשל, בדיקה מה המשתמש ביקש, גישה ל-DB למועדפים...)
        
        # --- פלסטר זמני: פשוט נביא תחזית לדדו ---
        beach_slug = "sdot-yam" # פלסטר
        print(f"Fetching forecast for '{beach_slug}'...")
        forecast_list = data_fetcher.get_forecast(beach_slug)
        
        if not forecast_list:
            print("Failed to fetch forecast.", file=sys.stderr)
            send_whatsapp_message(phone_number, "מצטער, לא הצלחתי להביא את התחזית כרגע.")
            return Response("OK", status=200) # מאשרים ל-PubSub שטיפלנו

        # 3. הרכבת תשובה
        # (ניקח רק את היום הראשון לצורך הבדיקה)
        today_forecast = forecast_list[0]
        reply_text = f"היי! הנה התחזית לחוף שדות-ים ({today_forecast['day_name']}):\n"
        for hour_data in today_forecast['hourly_forecast']:
            reply_text += f"\n- {hour_data['time']}: גלים {hour_data['wave_height']}, ים {hour_data['sea_description']}"

        # 4. שליחת התשובה חזרה למשתמש
        print(f"Sending reply to {phone_number}...")
        send_whatsapp_message(phone_number, reply_text)

        # 5. אישור ל-Pub/Sub שטיפלנו בהודעה
        # אם נחזיר 200/204, Pub/Sub ימחק את ההודעה מהתור
        return Response("OK", status=200)

    except Exception as e:
        print(f"Error processing Pub/Sub message: {e}", file=sys.stderr)
        # אם נחזיר שגיאה (כמו 500), Pub/Sub ינסה לשלוח את ההודעה שוב
        return Response("Internal Server Error", status=500)


def send_whatsapp_message(to_phone, message_text):
    """
    Sends a message back to the user via the Meta Graph API.
    (העתקנו את הפונקציה הזו מה-Webhook)
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


if __name__ == "__main__":
    # אנחנו לא מריצים את זה מקומית כרגע, כי זה דורש ש-Pub/Sub
    # יוכל לשלוח בקשות למחשב המקומי (דורש חשיפה מסובכת)
    print("This service is designed to be run in Cloud Run and triggered by Pub/Sub.")
    # למרות זאת, נאפשר הרצה מקומית בסיסית אם נרצה
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8081)))