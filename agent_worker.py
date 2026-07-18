import datetime
import json

import numpy as np
import psycopg2
import requests
from kafka import KafkaConsumer
import autogen
from sklearn.ensemble import IsolationForest
from urllib3.util import url
import redis

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

#definining db connection
db_conn = psycopg2.connect("host=localhost dbname=security_metrics user=postgres password=password")
db_cursor = db_conn.cursor()


#Initializing the tables in the DB
db_cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS transaction_logs(
        time TIMESTAMPTZ NOT NULL,
        transaction_id TEXT NOT NULL,
        account_id TEXT NOT NULL,
        amount DOUBLE PRECISION,
        location_score REAL,
        anomaly_score REAL,
        is_anomaly BOOLEAN,
        processing_path TEXT
    );
    """
)
try:
    db_cursor.execute("SELECT create_hypertable('transaction_logs', 'time', if_not_exists => TRUE);")
    db_conn.commit()
except Exception:
    db_conn.rollback()


#Now we r training Ml based Isolation Forest Engine
X_train = np.random.rand(200, 2)
X_train[:, 0] = X_train[:, 0] * 5000
X_train[:, 1] = X_train[:, 1] * 0.2
ml_model = IsolationForest(contamination=0.05, random_state=42).fit(X_train)

# Define configuration using local Ollama instance framework guidelines
llm_config = {
    "config_list": [
        {
            "model": "llama3.2:3b",
            "base_url": "http://localhost:11434/v1",
            "api_key": "ollama",
        }
    ],
    "cache_seed": None, # Prevents caching responses for changing log profiles
}


#We have added below function as we are using ollama model so as it is a lightweight model, it may hallucinate sometimes
#Few times without below function , it was detecting wrong account

def freeze_user_account(account_id: str) -> str:
    """
        Immediately freezes a compromised user account in the primary Spring Boot backend microservice.
        Args:
            account_id (str): The unique ID of the account to freeze (e.g., 'ACC-4412').
        """
    url = "http://localhost:8080/api/v1/transactions/mitigate"
    payload = f"SYSTEM_EXECUTION: FROZE ACCOUNT {account_id}"
    try:
        response = requests.post(url, data=payload, headers={"Content-Type": "text/plain"})
        return f"Successfully executed callback. Spring Boot status: {response.status_code}"
    except Exception as e:
        return f"Failed to reach Spring Boot: {str(e)}"


def run_agent_swarm(incident_details,anomaly_score, cache_key):
    print(" Cache Miss: Booting fresh AutoGen Swarm...")

    log_analyst = autogen.AssistantAgent(
        name="LogAnalyst",
        system_message="You are an expert Security Log Analyst. State your findings and severity level clearly. Hand over to SRE_Mitigator.",
        llm_config=llm_config,
    )

    sre_mitigator = autogen.AssistantAgent(
        name="SRE_Mitigator",
        system_message=(
            "You are an SRE. If an account freeze is warranted, call 'freeze_user_account'. "
            "Once confirmed, summarize the action and write 'TERMINATE'."
        ),
        llm_config=llm_config,
    )

    user_proxy = autogen.UserProxyAgent(
        name="UserProxy",
        human_input_mode="NEVER",
        max_consecutive_auto_reply=4,
        is_termination_msg=lambda x: "TERMINATE" in (x.get("content") or ""),
        code_execution_config=False
    )

    autogen.agentchat.register_function(
        freeze_user_account,
        caller=sre_mitigator,
        executor=user_proxy,
        name="freeze_user_account",
        description="Freezes a compromised account in the core backend service",
    )

    group_chat = autogen.GroupChat(
        agents=[user_proxy, log_analyst, sre_mitigator],
        messages=[],
        max_round=6,
        speaker_selection_method="round_robin"
    )
    manager = autogen.GroupChatManager(groupchat=group_chat, llm_config=llm_config)

    task_prompt = f"Analyze and propose action items for this incident alert metrics: {json.dumps(incident_details)}"
    user_proxy.initiate_chat(recipient=manager, message=task_prompt)

    # Cache the final resolution step to mitigate subsequent identical attacks
    # We assign a 10-minute Time-To-Live (600 seconds) expiration safety buffer
    resolved_action = f"AUTOMATED_MITIGATION: Account isolated via tool rule execution sequence."
    redis_client.setex(cache_key, 600, resolved_action)
    print(f"💾 Step complete. Resolution blueprint cached to Redis.")

def send_mitigation_callback(final_decision):

    url="http://localhost:8080/api/v1/transactions/mitigate"
    payload = {"action": final_decision}

    try:
        response = requests.post(url, json=payload)
        print(f"Callback response code received from Spring App: {response.status_code}")
    except Exception as e:
        print("Connection to application gateway failed")


if __name__ == "__main__":
    consumer = KafkaConsumer(
        'security-alerts',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print("⚡ Real-Time Fraud Agent Platform Listening to Kafka Stream...")
    for message in consumer:
        alert_data = message.value
        account_id = alert_data.get('accountId', 'UNKNOWN')
        transaction_id = alert_data.get('transactionId', 'UNKNOWN')
        amount = float(alert_data.get('amount', '0.0'))

        location_raw = alert_data.get('location') or ''
        location_score = 0.95 if "Suspicious" in location_raw else 0.05


        # Construct a unique lookup signature key combination
        cache_key = f"threat_cache:{account_id}:high_amount_anomaly"

        print(f"\n📩 Detected Incoming Kafka Alert for {account_id}")

        # Check Redis Cache
        cached_mitigation = redis_client.get(cache_key)
        current_time = datetime.datetime.now(datetime.timezone.utc)

        if cached_mitigation:
            print(f"🚀 [REDIS CACHE HIT]: Duplicate threat pattern detected for {account_id}!")
            print(f"⚡ Bypassing Agent Swarm. Instantly triggering direct backend mitigation callback execution...")
            # Fire instant callback logic without calling LLM infrastructure
            freeze_user_account(account_id)

            db_cursor.execute(
                "INSERT INTO transaction_logs VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (current_time, transaction_id, account_id, amount, location_score, 0.0, True, "REDIS_CACHE_HIT")
            )
            db_conn.commit()
            continue

        #Cache miss
        input_vector =np.array([[amount, location_score]])
        prediction = ml_model.predict(input_vector)[0]

        anomaly_score = float(ml_model.decision_function(input_vector)[0])
        is_anomaly = True if prediction == -1 else False

        path_taken = "ML_ANOMALY_TRIGGERED" if is_anomaly else "CLEAN_TRANSACTION"

        # Step C: Document Telemetry Data Point inside TimescaleDB
        db_cursor.execute(
            "INSERT INTO transaction_logs VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (current_time, transaction_id, account_id, amount, location_score, anomaly_score, is_anomaly, path_taken)
        )
        db_conn.commit()
        print(f"💾 Log Appended to TimescaleDB. Processing Path: {path_taken}")

        # Step D: Route to AutoGen Cluster conditionally based on ML inference flags
        if is_anomaly:
            run_agent_swarm(alert_data, anomaly_score, cache_key)