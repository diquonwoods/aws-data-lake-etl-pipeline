# AWS Data Lake ETL Pipeline

An end-to-end cloud data engineering project that processes customer and IoT sensor data through landing, trusted, and curated data layers using AWS Glue, Amazon S3, PySpark, Spark SQL, and AWS Glue Data Quality.

## Project Overview

This project demonstrates the design and implementation of a layered data lake pipeline on AWS.

Raw customer and sensor data is ingested into Amazon S3 and transformed through AWS Glue ETL jobs. The pipeline filters data based on research consent, validates datasets, joins related data sources, removes unnecessary customer attributes, handles duplicate records, and produces curated datasets for downstream analytics and machine learning.

The project was originally developed as part of graduate coursework in data engineering and has been reorganized and documented as a portfolio project.

## Architecture

The pipeline follows a three-layer data lake design:

### Landing Layer

Raw JSON data is stored in Amazon S3 and exposed through external table definitions.

Data sources include:

- Customer data
- Accelerometer sensor data
- Step Trainer sensor data

### Trusted Layer

AWS Glue and PySpark jobs clean, filter, join, and validate the raw datasets.

Trusted datasets include:

- `customer_trusted`
- `accelerometer_trusted`
- `step_trainer_trusted`

### Curated Layer

Trusted datasets are combined and prepared for downstream use.

Curated datasets include:

- `customer_curated`
- `machine_learning_curated`

## Pipeline Flow

```text
                        Amazon S3
                       Landing Layer
                            |
             +--------------+--------------+
             |              |              |
             v              v              v
         Customer      Accelerometer   Step Trainer
             |              |              |
             v              |              |
        AWS Glue ETL        |              |
             |              |              |
             v              |              |
     Customer Trusted ------+              |
             |              |              |
             |              v              |
             |      Accelerometer Trusted  |
             |              |              |
             +-------+------+              |
                     |                     |
                     v                     |
              Customer Curated ------------+
                     |
                     v
             Step Trainer Trusted
                     |
                +----+----+
                |         |
                v         v
          Step Trainer  Accelerometer
             Trusted       Trusted
                |             |
                +------+------+
                       |
                       v
             Machine Learning
                   Curated
```

## ETL Workflows

### Customer Trusted

The customer trusted job:

1. Reads raw customer JSON data from the Amazon S3 landing layer.
2. Filters customer records based on research-sharing consent.
3. Performs a basic AWS Glue data-quality validation.
4. Writes validated records into the trusted layer.
5. Updates the AWS Glue Data Catalog.

### Accelerometer Trusted

The accelerometer trusted job:

1. Reads raw accelerometer measurements from the landing layer.
2. Loads trusted customer records from the AWS Glue Data Catalog.
3. Joins accelerometer records to customers using the user/email relationship.
4. Retains sensor records associated with customers who consented to research.
5. Removes unnecessary customer attributes from the sensor dataset.
6. Performs data-quality validation.
7. Writes the resulting dataset to the trusted layer.

### Customer Curated

The customer curated job:

1. Loads trusted customer and accelerometer datasets.
2. Joins customer records with corresponding accelerometer activity.
3. Removes accelerometer measurement fields.
4. Deduplicates resulting customer records.
5. Validates the dataset.
6. Writes the result to the curated customer layer.

### Step Trainer Trusted

The Step Trainer trusted job:

1. Loads raw Step Trainer sensor data.
2. Loads the curated customer dataset.
3. Deduplicates customer records.
4. Associates Step Trainer devices with matching customer records using device serial numbers.
5. Removes customer attributes that are not required downstream.
6. Performs data-quality validation.
7. Writes the resulting sensor dataset into the trusted layer.

### Machine Learning Curated

The final curated job combines trusted sensor datasets to create an ML-ready dataset.

The pipeline:

1. Loads trusted accelerometer data.
2. Loads trusted Step Trainer data.
3. Joins sensor observations using corresponding reading timestamps.
4. Performs data-quality validation.
5. Writes the combined dataset to the curated layer.

## Technologies

| Technology | Purpose |
|---|---|
| Amazon S3 | Data lake storage |
| AWS Glue | ETL orchestration and processing |
| AWS Glue Data Catalog | Dataset metadata and table management |
| AWS Glue Data Quality | Dataset validation |
| PySpark | Distributed data transformations |
| Spark SQL | Filtering and dataset joins |
| SQL | External table definitions |
| Python | ETL development |
| JSON | Source and output data format |

## Repository Structure

```text
aws-data-lake-etl-pipeline/
│
├── glue_jobs/
│   ├── accelerometer_trusted.py
│   ├── customer_curated.py
│   ├── customer_trusted.py
│   ├── machine_learning_curated.py
│   └── step_trainer_trusted.py
│
├── sql/
│   ├── accelerometer_landing.sql
│   ├── customer_landing.sql
│   └── step_trainer_landing.sql
│
├── .gitignore
└── README.md
```

## Data Quality

AWS Glue Data Quality is incorporated into the ETL workflows before data is written to trusted or curated locations.

The current validation rule confirms that output datasets contain columns:

```text
Rules = [
    ColumnCount > 0
]
```

This provides a basic validation step and establishes a foundation for more comprehensive data-quality checks.

Potential production improvements could include:

- Required-column validation
- Null-value thresholds
- Data type validation
- Duplicate detection
- Valid timestamp ranges
- Referential integrity checks
- Sensor measurement boundaries

## Data Privacy

The pipeline includes privacy-aware processing logic.

Customer records are filtered using research-sharing consent before associated sensor data is promoted into trusted datasets. Customer attributes that are not required for downstream sensor analysis are removed during transformation.

This public repository does not include:

- AWS access keys
- AWS secret keys
- Passwords
- Production customer data
- Private datasets

S3 locations use the placeholder:

```text
s3://your-data-lake-bucket/
```

Users deploying the project must replace this value with their own Amazon S3 bucket.

## Skills Demonstrated

This project demonstrates practical experience with:

- Cloud data engineering
- ETL pipeline development
- Amazon S3 data lake architecture
- AWS Glue
- PySpark
- Spark SQL
- Data transformation
- Data cleansing
- Dataset joins
- Deduplication
- Data-quality validation
- AWS Glue Data Catalog
- Layered data architecture
- Privacy-aware data processing
- Machine-learning dataset preparation

## Future Improvements

The pipeline could be extended by:

- Converting JSON outputs to Apache Parquet
- Partitioning S3 datasets for more efficient querying
- Adding infrastructure as code with Terraform or AWS CloudFormation
- Parameterizing S3 bucket and Data Catalog configuration
- Adding automated unit and integration tests
- Expanding AWS Glue Data Quality rules
- Adding workflow orchestration
- Adding CI validation with GitHub Actions
- Implementing logging and monitoring
- Adding an architecture diagram

## Project Background

This project was developed as part of graduate-level data engineering coursework and later reorganized as a portfolio demonstration.

The objective was to build a cloud-based pipeline that takes multiple raw datasets through progressively refined data layers and produces trusted and curated datasets suitable for analytics and machine-learning workflows.

