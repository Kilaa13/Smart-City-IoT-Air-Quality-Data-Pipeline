# Smart City IoT Air Quality Data Pipeline
![Google Cloud Pub/Sub](https://img.shields.io/badge/Google_Cloud_Pub/Sub-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)
![Google Cloud Dataflow](https://img.shields.io/badge/Google_Cloud_Dataflow-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)
![Apache Beam](https://img.shields.io/badge/Apache_Beam-0288D1?style=for-the-badge&logo=apachebeam&logoColor=white)
![Google BigQuery](https://img.shields.io/badge/Google_BigQuery-669DF6?style=for-the-badge&logo=googlebigquery&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
Proyek ini adalah *end-to-end data engineering pipeline* berkinerja tinggi untuk memproses dan menganalisis data kualitas udara (*Air Quality Index*) secara *real-time* dari berbagai sensor IoT kota pintar. 

Data diproses secara *streaming* menggunakan **Google Cloud Dataflow (Apache Beam)**, disimpan di **Google BigQuery**, dan divisualisasikan melalui **Power BI Dashboard**.

---

## Arsitektur Sistem
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

## Fitur Utama

* **Real-time Streaming:** Memproses data telemetry sensor (PM2.5, CO2, kelembapan, suhu) tanpa *latency* tinggi.
* **Scalable Pipeline:** Menggunakan Google Cloud Dataflow yang dapat menyesuaikan kapasitas komputasi (*auto-scaling*) secara otomatis.
* **Medallion Data Architecture:** Menstrukturkan data dari mentah hingga siap guna (*Gold Layer*) di BigQuery.
* **Interactive Dashboard:** Visualisasi statistik kualitas udara per wilayah dan garis tren polusi menggunakan Power BI via koneksi *DirectQuery*.

---

## Struktur Repositori

```text
smart-city-aqi/
├── simulator/
│   └── air_quality_simulator.py    # Skrip simulator untuk mengirim data IoT buatan ke Pub/Sub
│   └── .env
├── dataflow/
│   └── aqi_dataflow_pipeline.py     # Kode Apache Beam/Dataflow untuk transformasi data
│   └── .env
├── dashboard/
│   └── dashboard.png          # Tangkapan layar Power BI Dashboard
├── requirements.txt           # Dependensi Python yang dibutuhkan
└── README.md                  # Dokumentasi proyek
```
