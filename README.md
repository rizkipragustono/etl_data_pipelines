# ETL and Data Pipelines with Shell, Airflow and Kafka
You are a Data Engineer at a data analytics consulting company, assigned to a project that aims to de-congest national highways by analyzing road traffic data from different toll plazas. Each highway is operated by a different toll operator with varying IT setups and file formats. Your job is to create three data pipelines to collect, process, and store this data.

Pipeline 1: Batch Processing with Apache Airflow and BashOperator
The first pipeline uses Apache Airflow with BashOperator to automate batch data collection from different toll operators. It fetches data files in various formats (e.g., CSV, JSON, XML), processes them using bash scripts, and consolidates them into a single, unified file.

Pipeline 2: Data Processing with Apache Airflow and PythonOperator
The second pipeline uses Apache Airflow with PythonOperator to process the consolidated data. Python scripts handle data transformations, such as aggregating traffic data by toll plaza, and load it into a database for further analysis.

Pipeline 3: Real-Time Streaming with Kafka
The third pipeline collects real-time data as vehicles pass through toll plazas. Vehicle data, including vehicle_id, vehicle_type, toll_plaza_id, and timestamp, is streamed to Kafka. The data is then processed in real-time and loaded into a database for live traffic analysis.

These three pipelines together provide a solution for handling both historical and real-time traffic data to optimize highway traffic flow.
