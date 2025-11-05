import os
import json
import requests
import psycopg2
import psycopg2.pool # Import pool specifically
import sys
import threading # <-- ADDED: For thread-safe pool initialization
from google.cloud import pubsub_v1
from flask import Flask, request, Response

app = Flask(__name__)

# --- Configuration (Loaded from Environment) ---
WHATSAPP_VERIFY_TOKEN = os.environ.get('WHATSAPP_VERIFY_TOKEN')
WHATSAPP_API_TOKEN = os.environ.get('WHATSAPP_API_TOKEN')
PHONE_NUMBER_ID = os.environ.get('PHONE_NUMBER_ID')

DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', 5432)

PROJECT_ID = os.environ.get('PROJECT_ID')
TOPIC_ID = os.environ.get('TOPIC_ID')

# --- Global Clients (Initialized Lazily or Safely) ---

# Publisher client is safe to initialize globally
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# --- Lazy DB Pool Initialization ---
# We initialize these to None. They will be created on the first request.
db_pool = None
db_pool_lock = threading.Lock() # ADDED: Lock to make initialization thread-safe

def get_db_pool():
    """
    Lazily initializes and returns a thread-safe database connection pool.
    """
    global db_pool
    
    # Use a "double-check" lock pattern for efficiency.
    # Most requests will hit this first check and return immediately.
    if db_pool:
        return db_pool
    
    # If pool is None, acquire the lock to initialize it.
    with db_pool_lock:
        # Check again *inside* the lock in case another thread
        # initialized the pool while we were waiting for the lock.
        if db_pool:
            return db_pool
        
        print("Database pool not initialized. Creating new pool...")
        try:
            # Create the connection pool
            db_pool = psycopg2.pool.SimpleConnectionPool(
                1, 2, # Min/max connections
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                host=DB_HOST,
                port=DB_PORT,
                connect_timeout=5 # Set a reasonable timeout
            )
            print("Database connection pool initialized successfully.")
            return db_pool
        except Exception as e:
            print(f"Error initializing database pool: {e}", file=sys.stderr)
            # If init fails, set pool back to None so we can retry on next request
            db_pool = None 
            return None # Let the caller handle the failure

# --- REMOVED: Global call to init_db_pool() is gone! ---

@app.route('/webhook', methods=['GET', 'POST'])
def whatsapp_webhook():
    """
    Main webhook endpoint for Meta.
    Handles both verification (GET) and message notifications (POST).
    """
    
    # --- 1. Webhook Verification (GET Request) ---
    if request.method == 'GET':
        mode = request.args.get('hub.mode')
        token = request.args.get('hub.verify_token')
        challenge = request.args.get('hub.challenge')
        
        if mode == 'subscribe' and token == WHATSAPP_VERIFY_TOKEN:
            print(f"Webhook verified with challenge: {challenge}")
            return Response(challenge, status=200)
        else:
            print("Webhook verification failed.")
            return Response("Verification failed", status=403)

    # --- 2. Message Notification (POST Request) ---
    if request.method == 'POST':
        try:
            data = request.get_json()
            
            # TODO: Add logic to process the incoming message data
            print("Received POST data (message):")
            print(json.dumps(data, indent=2))

            # --- Authentication & Authorization (Placeholder) ---
            # 1. Get user's phone number from 'data'
            # user_phone = ... 
            
            # 2. Check if user is in our database
            # This will now trigger the lazy DB pool creation if needed
            # is_authorized = check_user_auth(user_phone)
            is_authorized = True # HACK: Assume all are authorized for now
            
            if not is_authorized:
                print(f"User {user_phone} is not authorized.")
                # TODO: Send "not authorized" message back
                return Response("Not authorized", status=403)

            # --- Send to Pub/Sub (Placeholder) ---
            # 3. Get message content
            # message_body = ...
            
            # 4. Publish to Pub/Sub
            # job_data = {"phone": user_phone, "message": message_body}
            # publisher.publish(topic_path, data=json.dumps(job_data).encode('utf-8'))
            print("Job published to Pub/Sub (placeholder).")

            # --- Send "Acknowledged" Reply (Placeholder) ---
            # 5. Send "קיבלתי, בודק..."
            # send_whatsapp_message(user_phone, "קיבלתי, בודק...")
            print("Sent 'Acknowledged' message (placeholder).")

            # Return 200 OK to Meta immediately
            return Response("OK", status=200)

        except Exception as e:
            print(f"Error processing POST request: {e}", file=sys.stderr)
            return Response("Internal Server Error", status=500)

# --- Helper Functions (We will build these out) ---

def check_user_auth(phone_number):
    """
    Checks if a phone number is in the authorized users list in Postgres.
    """
    # CHANGED: Call the lazy getter function
    pool = get_db_pool() 
    if not pool:
        print("Failed to get DB pool, auth check failed.", file=sys.stderr)
        return False

    conn = None
    try:
        # Get a connection from the pool
        conn = pool.getconn()
        with conn.cursor() as cursor:
            # TODO: Create this 'authorized_users' table
            cursor.execute("SELECT 1 FROM authorized_users WHERE phone = %s", (phone_number,))
            return cursor.fetchone() is not None
    except Exception as e:
        print(f"Database auth check failed: {e}", file=sys.stderr)
        return False
    finally:
        if conn:
            # Return the connection to the pool
            pool.putconn(conn) # Use the 'pool' variable

def send_whatsapp_message(to_phone, message_text):
    """
    Sends a message back to the user via the Meta Graph API.
    """
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
    # This block is only for local testing
    from dotenv import load_dotenv
    import os
    
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
    load_dotenv(dotenv_path)

    print("Starting local Flask server for testing...")
    print(f"Loaded DB_HOST: {os.environ.get('DB_HOST')}") 
    
    # This will now also use the lazy pool when a request comes in
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))