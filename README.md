# Smart City IoT Air Quality Data Pipeline
![Google Cloud Pub/Sub](https://img.shields.io/badge/Google_Cloud_Pub/Sub-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)
![Google Cloud Dataflow](https://img.shields.io/badge/Google_Cloud_Dataflow-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)
![Apache Beam](https://img.shields.io/badge/Apache_Beam-0288D1?style=for-the-badge&logo=apachebeam&logoColor=white)
![Google BigQuery](https://img.shields.io/badge/Google_BigQuery-669DF6?style=for-the-badge&logo=googlebigquery&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

This project is a high-performance, end-to-end data engineering pipeline designed to process and analyze Air Quality Index (AQI) data in real-time from various smart city IoT sensors. 

Data is ingested and processed via streaming using **Google Cloud Dataflow (Apache Beam)**, structured within **Google BigQuery** using a Medallion architecture, and translated into actionable business insights through a **Microsoft Power BI Dashboard**.

---

## 🏗️ System Architecture
```
[ IoT Sensor Simulator ]
          │
          ▼
[ Google Cloud Pub/Sub ]  --> (Streaming Data Ingestion)
          │
          ▼
[ Google Cloud Dataflow ] --> (Data Transformation & Aggregation via Apache Beam)
          │
          ▼
[ Google BigQuery ]       --> (Gold Table Storage & DirectQuery)
        │
        ▼
[ Power BI Dashboard ]    --> (Real-Time Monitoring & Analytics)
```

---

## ✨ Key Features
* **Real-time Streaming**: Processes sensor telemetry data (PM2.5, CO2, humidity, temperature) with extremely low latency and zero data loss.
* **Scalable Pipeline**: Leverages Google Cloud Dataflow, a fully managed service that automatically scales compute resources (auto-scaling) to handle throughput spikes seamlessly.
* **Medallion Data Architecture**: Structures data sequentially from a raw state (*Bronze*) to an analytics-ready state (Gold Layer) within BigQuery for production-grade analytics.
* **Interactive Dashboard**: Delivers visualizations of regional air quality statistics and pollution trend lines using Microsoft Power BI via *DirectQuery* connection (eliminating the need for static data extracts).

---

## 📂 Repository Structure
```text
smart-city-aqi/
├── dashboard/
│   └── dashboard.png             # Screenshot of the Power BI Dashboard
├── dataflow/
│   └── aqi_dataflow_pipeline.py  # Apache Beam/Dataflow code for data transformation
├── simulator/
│   └── air_quality_simulator.py  # Python script to generate and publish mock IoT data to Pub/Sub
├── .env.example
├── README.md                     # Main project documentation 
└── requirements.txt              # Required Python dependencies
```
## 🚀 Getting Started
### 1. Prerequisites
* A **Google Cloud Platform (GCP)** account with access to Pub/Sub, Dataflow, Cloud Storage, and BigQuery.

* **Python** 3.11+ installed locally.

* **Microsoft Power BI** Desktop application.

### 2. Installation
Clone this repository and install the required dependencies:

```bash
git clone https://github.com/your-username/smart-city-aqi.git
cd smart-city-aqi
pip install -r requirements.txt

```
### 3. Executing the Pipeline
* (Add brief instructions here on how to configure your .env files, run the air_quality_simulator.py, and submit the Dataflow job to GCP)*
"""

with open("README-SmartCity-AQI.md", "w") as f:
f.write(markdown_content)

print("File generated successfully.")

```text?code_stdout&code_event_index=1
File generated successfully.
```
