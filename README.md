# Insurance-Data-Pipeline

End-to-End Python ETL Pipeline using Pandas


Project Overview

This project implements an End-to-End Insurance Data Pipeline using Python and Pandas.  
It automates data ingestion, validation, preprocessing, transformation, aggregation, logging, and archival.

The main goal of this project is to simulate how a real-world data pipeline works in an industry environment.

The pipeline processes multiple insurance datasets such as:

Claims  
Customers  
Policies  
Agents  
Payments  


Pipeline Flow

The pipeline follows a layered ETL architecture:

Ingestion → Preprocessed → Curated → Semantic → Retention


Ingestion Layer

Reads CSV files from the source folder.  
Validates file structure using JSON control files.  
Only valid files are allowed for further processing.


Preprocessing Layer

Removes duplicate records.  
Adds audit columns like ingestion_date and file_date.  
Converts cleaned data into parquet format.


Curated Layer

Combines data from multiple sources.  
Applies business logic using joins and transformations.  
Creates an enriched claims dataset.


Semantic Layer

Generates aggregated business reports such as:

Policy Type vs Total Claim Amount  
City vs Average Claim Amount  


Retention Layer

Archives processed source files.  
Maintains logs for ingestion, preprocessing, and retention steps.  
Ensures traceability of pipeline execution.


Technologies Used

Python  
Pandas  
JSON Processing  
File Handling (os, shutil, pathlib)  
CSV and Parquet File Formats  


Project Structure

Insurance-Data-Pipeline/

config/control_files → JSON validation schemas  
data → Data layer folders  
logs → Pipeline logs  
src → Python source code modules  
requirements.txt → Project dependencies  


How to Run the Project

1. Place CSV files inside the data/ingestion folder.

2. Run the pipeline using:

python src/main.py

3. The pipeline will automatically process the data and generate outputs in different layers.


Key Features

Control-file driven validation  
Automated ETL workflow  
Modular Python design  
Business rule based transformations  
Audit logging system  
Automatic file archival  


Learning Outcomes

This project helped in understanding:

How ETL pipelines work in real systems  
Data cleaning and transformation using Pandas  
Designing modular Python applications  
Handling large datasets efficiently  
Implementing logging and audit mechanisms  


Author

Sakshi Kulkarni
