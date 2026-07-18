# Real-Time Event-Driven Fraud Detection & Autonomous Incident Response Platform

An intelligent, event-driven security platform that ingests live transaction streams, evaluates threat telemetry via Machine Learning anomalies, and utilizes a local multi-agent LLM swarm to autonomously isolate compromised accounts. It features a deduplication caching layer to mitigate distributed brute-force attacks at zero LLM cost.

## 🛠️ Tech Stack
* **Backend Gateway:** Spring Boot (Java), Spring Kafka, Jackson ObjectMapper
* **AI & Machine Learning Engine:** Python 3.12+, Microsoft AutoGen, Scikit-Learn (Isolation Forest)
* **Local LLM Infrastructure:** Ollama (Llama 3.2:3b)
* **Message Broker:** Apache Kafka / Zookeeper
* **Distributed Caching:** Redis (Mitigation signature caching)
* **Telemetry Data Store:** TimescaleDB (Time-series hypertable on PostgreSQL)


## ⚙️ System Flow

<img width="979" height="606" alt="image" src="https://github.com/user-attachments/assets/3e21c20f-f787-480b-9ae8-766e0ad1c8e8" />




## 🚀 Quick Start Execution Sequence

### Prerequisites
Ensure you have Docker, Python 3.12+, Maven, and Ollama installed.

### 1. Boot Infrastructure
Navigate to the incident_reponse_agent and execute below
```bash
docker-compose up -d
```

This will spin up the Docker containers for TimescaleDB,Redis,Zookeeper and Kafka.


### 2. Run  IncidentReponseAgentApplication.java

This starts the Spring Boot application


### 3. Start Ollama

Pull any Ollama model and run it through the command
```bash
ollama run llama3.2:3b
```

You can check available models in your system through

```bash
ollama list 
```

### 4. Start the Agent Worker

Execute the agent_worker.py file through terminal or preferably through any IDE.


## Testing the application

Through curl or Postman first we send a clean transaction payload 

```bash
curl -X POST http://localhost:8080/api/v1/transactions/process \
     -H "Content-Type: application/json" \
     -d '{"transactionId":"TXN-9901", "accountId":"ACC-USER-01", "amount":120.50, "location":"HomeCity"}'
```

The Main java application will show kafka logs and the agent_worker.py terminal will show below message 
<img width="728" height="129" alt="image" src="https://github.com/user-attachments/assets/b3430800-ac1d-4eba-8cd5-c2508b3e5e6e" />



Now in case of malicious transaction we send below

```bash
curl -X POST http://localhost:8080/api/v1/transactions/process \
     -H "Content-Type: application/json" \
     -d '{"transactionId":"TXN-ATTACK-09", "accountId":"ACC-TARGET-99", "amount":987000.00, "location":"SuspiciousLocation"}'
```

The terminal reflects as below

<img width="1786" height="287" alt="image" src="https://github.com/user-attachments/assets/20bff5f1-e67a-4212-a29c-bb38743f6991" />


Full log file in --> [View Fraud Detection Logs (PDF)](https://github.com/Bhuvan588/ml-agentic-fraud-mitigation/blob/master/fraud_detection_log.pdf)



In order to check working of Redis we send above request again and it will show below output

<img width="1015" height="127" alt="image" src="https://github.com/user-attachments/assets/1e9eb7e9-d1e3-4171-b522-537501678e0c" />


## 🤔 Q&A

### Which ML model are we using to classify anomalies ?

 We are using Isolation Forest. An Isolation Forest is an unsupervised anomaly detection algorithm that scales to multi-dimensional feature footprints. It isolates outliers dynamically by analyzing how variables interact globally (e.g., mapping transaction sizes relative to location anomalies simultaneously) without requiring rigid hardcoded cutoffs.

### Why TimescaleDB instead of traditional PostgreSQL?

TimescaleDB uses automated time-based partitioning ("hypertables"). This allows the platform to maintain blazing-fast, predictable write and query speeds even under heavy, continuous telemetry streaming, keeping our audit paths production-ready.


### How are we ensuring that the local model does't hallucinate?

If we look into the ollama config, then we see that the local model is just a 3B model . This was a constraint due to my system as I wanted to use a complete local model. To protect the core application, the AutoGen agents are never trusted to execute text commands or raw code directly. Instead, we use autogen.agentchat.register_function. This forces the agent to interact using a strictly typed, schema-validated Python tool function, ensuring the downstream backend webhook receives pure, deterministic variables.
