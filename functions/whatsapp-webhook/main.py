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

# --- Global Clients (Lazy Loading) ---
db_pool = None
db_pool_lock = threading.Lock()

publisher = None
publisher_lock = threading.Lock()

# --- DB & PubSub Getters (Lazy) ---
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

# --- NEW: User registration function ---
def register_user_if_not_exists(phone_number):
    """
    Ensures a user exists in the 'users' table.
    Uses 'INSERT ... ON CONFLICT' for efficiency (upsert).
    Returns True on success, False on failure.
    """
    pool = get_db_pool()
    if not pool:
        print("Failed to get DB pool, user registration failed.", file=sys.stderr)
        return False # Return failure

    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cursor:
            # SQL command to insert if not exists
            sql = """
            INSERT INTO users (phone_number) 
            VALUES (%s) 
            ON CONFLICT (phone_number) DO NOTHING;
            """
            cursor.execute(sql, (phone_number,))
            conn.commit() # Must commit after a write operation
            return True # Return success
    except Exception as e:
        print(f"Database user registration failed: {e}", file=sys.stderr)
        if conn:
            conn.rollback() # Rollback changes on error
        return False
    finally:
        if conn:
            pool.putconn(conn)

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
            
            # 1. Parse the incoming message
            try:
                message = data['entry'][0]['changes'][0]['value']['messages'][0]
                user_phone = message['from']
                message_type = message.get('type')

                if message_type != 'text':
                    print(f"Ignoring non-text message from {user_phone}")
                    return Response("OK (Not Text)", status=200)

                message_text = message['text']['body']
                print(f"Received text message from {user_phone}: {message_text}")
                
            except (KeyError, TypeError, IndexError):
                print("Could not parse incoming message structure.", file=sys.stderr)
                return Response("OK (Parsing Error)", status=200)

            # 2. UPDATED: Register the user
            if not register_user_if_not_exists(user_phone):
                # If DB registration fails, stop processing
                print(f"Failed to register user {user_phone}. Aborting.", file=sys.stderr)
                return Response("Internal Error", status=500)
            
            # 3. Publish the job to Pub/Sub
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
                return Response("Internal Error", status=500)

            # 4. Send "Acknowledged" Reply (Placeholder)
            print("Sent 'Acknowledged' message (placeholder).")
            
            return Response("OK", status=200)

        except Exception as e:
            print(f"Error processing POST request: {e}", file=sys.stderr)
            return Response("Internal Server Error", status=500)

# --- Helper Functions ---
def send_whatsapp_message(to_phone, message_text):
    """
    Sends a message back to the user via the Meta Graph API.
    (This function is a placeholder and won't be used by the webhook)
    """
    pass # The webhook's job is to publish, not to reply.

# --- Local Testing Block ---
if __name__ == "__main__":
    from dotenv import load_dotenv
    import os
    
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
    load_dotenv(dotenv_path)

    print("Starting local Flask server for testing...")
    print(f"Loaded DB_HOST: {os.environ.get('DB_HOST')}") 
    
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))