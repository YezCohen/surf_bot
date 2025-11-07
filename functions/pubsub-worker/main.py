import os
import json
import base64
import sys
import threading
import requests
import psycopg2
import psycopg2.pool
from flask import Flask, request, Response

# Import the local scraper copy
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

# --- Global Clients (Lazy Loading) ---
db_pool = None
db_pool_lock = threading.Lock()

# --- DB Getter (Lazy) ---
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

# --- NEW: Helper function to get beach list ---
def get_all_beaches_from_db():
    """
    Fetches the full list of beach names from the database.
    """
    pool = get_db_pool()
    if not pool:
        print("Failed to get DB pool, beach list failed.", file=sys.stderr)
        return None

    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cursor:
            # Get all beach names, sorted alphabetically
            cursor.execute("SELECT name FROM beaches ORDER BY name ASC;")
            results = cursor.fetchall() # Returns a list of tuples: [('חוף דדו',), ('חוף תל ברוך',)]
            # Convert list of tuples to a simple list of strings
            beach_names = [result[0] for result in results]
            return beach_names
    except Exception as e:
        print(f"Database get_all_beaches failed: {e}", file=sys.stderr)
        return None
    finally:
        if conn:
            pool.putconn(conn)

# --- UPDATED: Helper function to find a specific beach ---
def find_beach_slug(beach_name_query):
    """
    Tries to find a beach slug from the 'beaches' table
    based on the user's message.
    """
    pool = get_db_pool()
    if not pool:
        print("Failed to get DB pool, beach lookup failed.", file=sys.stderr)
        return None

    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cursor:
            # We will search for an exact match first (case-insensitive)
            sql_exact = "SELECT slug FROM beaches WHERE name ILIKE %s OR slug ILIKE %s;"
            cursor.execute(sql_exact, (beach_name_query, beach_name_query))
            result = cursor.fetchone()
            if result:
                return result[0] # Found exact match

            # If no exact match, try a partial match
            sql_partial = "SELECT slug FROM beaches WHERE name ILIKE %s OR slug ILIKE %s;"
            query_param = f"%{beach_name_query}%" 
            cursor.execute(sql_partial, (query_param, query_param))
            result = cursor.fetchone()
            
            if result:
                return result[0] # Return the first partial match
            else:
                return None # No match found
    except Exception as e:
        print(f"Database beach lookup failed: {e}", file=sys.stderr)
        return None
    finally:
        if conn:
            pool.putconn(conn)

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
        # 1. Parse the Pub/Sub message
        pubsub_message = envelope['message']
        message_data = base64.b64decode(pubsub_message['data']).decode('utf-8')
        job_data = json.loads(message_data)
        
        phone_number = job_data['phone_number']
        message_text = job_data['message_text'].strip()
        
        print(f"Worker received job: Phone={phone_number}, Msg={message_text}")

        # --- UPDATED: Main Bot Logic ---
        
        # 2. Try to find a matching beach
        beach_slug = find_beach_slug(message_text)
        
        if not beach_slug:
            # --- NEW FEATURE: Send beach list if no match ---
            print(f"Could not find beach matching: '{message_text}'. Fetching full list.")
            beach_list = get_all_beaches_from_db()
            
            if beach_list:
                # Format the list for a WhatsApp message
                reply_text = f"מצטער, לא מצאתי חוף בשם '{message_text}'.\n\nאלו החופים הזמינים (נסה לשלוח שם מדויק):\n\n"
                reply_text += "\n".join(beach_list)
            else:
                # Fallback message if the DB query also fails
                reply_text = f"מצטער, לא מצאתי חוף בשם '{message_text}'. (בנוסף, אירעה שגיאה בניסיון לשלוף את רשימת החופים)."

            send_whatsapp_message(phone_number, reply_text)
            return Response("OK (Beach not found, list sent)", status=200)

        # 3. If beach was found, fetch the forecast
        print(f"Found beach: '{beach_slug}'. Fetching forecast...")
        forecast_list = data_fetcher.get_forecast(beach_slug)
        
        if not forecast_list:
            print("Failed to fetch forecast.", file=sys.stderr)
            send_whatsapp_message(phone_number, "מצטער, לא הצלחתי להביא את התחזית עבור החוף הזה כרגע.")
            return Response("OK (Fetch failed)", status=200)

        # 4. Format the reply
        today_forecast = forecast_list[0]
        beach_name = message_text # We can improve this later by getting the 'name' from the DB
        
        reply_text = f"התחזית לחוף '{beach_name}' ({today_forecast['day_name']}):\n"
        for hour_data in today_forecast['hourly_forecast']:
            reply_text += f"\n- {hour_data['time']}: גלים {hour_data['wave_height']}, ים {hour_data['sea_description']}"

        # 5. Send the reply
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