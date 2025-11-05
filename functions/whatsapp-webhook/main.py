import os
import json
import requests
import psycopg2
import psycopg2.pool
import sys
import threading
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

# --- Global Clients (Initialized Lazily) ---
db_pool = None
db_pool_lock = threading.Lock()

publisher = None
publisher_lock = threading.Lock()

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

def get_publisher():
    """
    Lazily initializes and returns a thread-safe Pub/Sub publisher client.
    """
    global publisher
    if publisher:
        return publisher
        
    with publisher_lock:
        if publisher:
            return publisher
            
        print("Pub/Sub publisher not initialized. Creating new client...")
        try:
            publisher = pubsub_v1.PublisherClient()
            print("Pub/Sub publisher initialized successfully.")
            return publisher
        except Exception as e:
            print(f"Error initializing Pub/Sub publisher: {e}", file=sys.stderr)
            publisher = None
            return None

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
            
            # --- START OF NEW/CHANGED LOGIC ---
            
            # 1. Parse the incoming message
            # We use .get() nested to avoid errors if the structure is wrong
            try:
                # Extract the first message's details
                message = data['entry'][0]['changes'][0]['value']['messages'][0]
                user_phone = message['from']
                message_type = message.get('type')

                # 2. We only care about text messages for now
                if message_type != 'text':
                    print(f"Ignoring non-text message from {user_phone}")
                    return Response("OK (Not Text)", status=200)

                message_text = message['text']['body']
                print(f"Received text message from {user_phone}: {message_text}")
                
            except (KeyError, TypeError, IndexError):
                print("Could not parse incoming message structure.", file=sys.stderr)
                print(json.dumps(data, indent=2))
                return Response("OK (Parsing Error)", status=200) # Still return 200

            # 3. Authentication (Placeholder - checking our DB)
            # is_authorized = check_user_auth(user_phone)
            is_authorized = True # HACK: Assume all are authorized for now
            
            if not is_authorized:
                print(f"User {user_phone} is not authorized.")
                # TODO: Send "not authorized" message back
                return Response("OK (Not Authorized)", status=200) # Return 200 so Meta doesn't retry

            # 4. Publish the job to Pub/Sub
            job_data = {
                "phone_number": user_phone, 
                "message_text": message_text
            }
            job_payload = json.dumps(job_data).encode('utf-8')

            pub_client = get_publisher()
            if pub_client:
                topic_path = pub_client.topic_path(PROJECT_ID, TOPIC_ID)
                pub_client.publish(topic_path, data=job_payload)
                print(f"Published job to Pub/Sub: {job_data}")
            else:
                print("Failed to get Pub/Sub publisher, job not published.", file=sys.stderr)
                # In a real app, we'd maybe try again or log this as a critical error
                return Response("Internal Error", status=500)

            # 5. Send "Acknowledged" Reply (Placeholder)
            # send_whatsapp_message(user_phone, "קיבלתי, בודק...")
            print("Sent 'Acknowledged' message (placeholder).")
            
            # --- END OF NEW/CHANGED LOGIC ---

            # Return 200 OK to Meta immediately
            return Response("OK", status=200)

        except Exception as e:
            print(f"Error processing POST request: {e}", file=sys.stderr)
            return Response("Internal Server Error", status=500)

# --- Helper Functions (unchanged) ---

def check_user_auth(phone_number):
    pool = get_db_pool()
    if not pool:
        print("Failed to get DB pool, auth check failed.", file=sys.stderr)
        return False
    conn = None
    try:
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
            pool.putconn(conn)

def send_whatsapp_message(to_phone, message_text):
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

# --- Local Testing Block (unchanged) ---
if __name__ == "__main__":
    from dotenv import load_dotenv
    import os
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
    load_dotenv(dotenv_path)
    print("Starting local Flask server for testing...")
    print(f"Loaded DB_HOST: {os.environ.get('DB_HOST')}") 
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))