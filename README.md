# Data Engineering Take Home Assignment

## Overview
This project demonstrates a complete solution for the Data Engineering Take Home Assignment. The assignment required us to:
- Create a data warehouse schema (a star schema) from the provided sample data.
- Summarize the analysis at the UNIT_ID, APP_INSTANCE_ID, and APP_TYPE_ID levels.
- Discuss two possible solutions for adding a column to large, live production tables without downtime.

The solution is implemented in Python using pandas for data exploration and SQLAlchemy for creating a simple SQLite database schema. The code is written in a modular, human-friendly style.

## Project Structure
- **solution.py**: The main Python script that:
  - Loads sample CSV files from a folder.
  - Merges data from multiple CSVs to derive key fields.
  - Creates a star schema (data warehouse) in SQLite.
  - Summarizes data by `unit_id`, `app_instance_id`, and `app_type_id`.
  - Provides a discussion of production schema change strategies.
- **take_home_data/**: A folder that contains the unzipped CSV files. The expected files are:
  - `APP_TYPE.csv`
  - `APP_TYPE_REF.csv`
  - `LOG_INSTANCE.csv`
  - `APP_INSTANCE.csv`
  - `POINT.csv`
  - `LOG_RCD_B.csv`
  - `LOG_RCD_F.csv`
- **data_warehouse.db**: (Optional) The SQLite database created by the script.

## Data Modeling and Exploration
### What We Did
1. **Data Loading**:  
   The script loads each CSV file into a pandas DataFrame. We observed the following:
   - **APP_TYPE**: 19,040 rows, 2 columns
   - **APP_TYPE_REF**: 919 rows, 3 columns
   - **LOG_INSTANCE**: 935 rows, 4 columns
   - **APP_INSTANCE**: 1,000,000 rows, 3 columns
   - **POINT**: 1,000,000 rows, 15 columns
   - **LOG_RCD_B**: 843,971 rows, 5 columns
   - **LOG_RCD_F**: 1,000,000 rows, 5 columns

2. **Data Warehouse Schema**:  
   We created a simple star schema using SQLAlchemy:
   - **Dimension Tables**:
     - `dim_application_type`: Contains application type details.
     - `dim_application_instance`: Contains application instance details.
     - `dim_unit`: Contains unit details.
   - **Fact Table**:
     - `fact_logs`: Contains log data (with additional columns possible for timestamps, status, etc.).

3. **Data Merging and Summarization**:  
   Since `LOG_INSTANCE.csv` did not directly include `unit_id` or `app_instance_id`, we merged it with `POINT.csv`, `APP_INSTANCE.csv`, and `APP_TYPE_REF.csv` to derive:
   - `unit_id`
   - `app_instance_id`
   - `app_type_id`
   
   We then grouped the merged data to summarize log counts by each of these keys.

### Additional Data & Assumptions
- **Assumptions**:
  - Each APP_INSTANCE belongs to a single application type.
  - The relationships are one-to-many (e.g., one APP_INSTANCE can have multiple log entries).
  - The CSV data is relatively clean; however, additional data like timestamps, user metadata, or error codes could provide further insights.
- **Additional Data**:
  - Timestamps for detailed time-based analysis.
  - User or operator details for tracking who generated each log.
  - Detailed status or error codes for better troubleshooting.

## Production Schema Change Discussion
When making schema changes in production for large, live tables, downtime must be minimized. We discussed two strategies:

1. **Online Schema Change Tools**:
   - **Tools**: pt-online-schema-change (for MySQL) or gh-ost.
   - **Approach**: Create a new table with the updated schema and copy data incrementally while tracking changes.
   - **Pros**: Minimal downtime and locking; suitable for high-traffic databases.
   - **Cons**: Requires careful configuration and monitoring; may need additional temporary resources.

2. **Shadow Table + Rolling Update**:
   - **Approach**: Create a shadow table with the new schema and implement dual-write logic to write to both the old and new tables. Gradually backfill historical data, verify consistency, and then switch over.
   - **Pros**: Can potentially achieve zero downtime.
   - **Cons**: More complex to implement and maintain; risks of data inconsistency if dual-writing is not managed properly.

## How to Run the Project

1. **Activate the Virtual Environment:**
   - **On Windows:**
     ```bash
     .\venv\Scripts\activate
     ```
   - **On macOS/Linux:**
     ```bash
     source venv/bin/activate
     ```

2. **Install Dependencies:**
   ```bash
   pip install pandas sqlalchemy
   
## Prepare the Data

- Manually unzip the provided data into a folder named `take_home_data` in the project root.
- Verify that the folder contains the following files:
  - `APP_TYPE.csv`
  - `APP_TYPE_REF.csv`
  - `LOG_INSTANCE.csv`
  - `APP_INSTANCE.csv`
  - `POINT.csv`
  - `LOG_RCD_B.csv`
  - `LOG_RCD_F.csv`

## Run the Script

Open a terminal and run the following command:

```bash
python solution.py

Review the Output
When you run the script, the console will display:

The data loading process for each CSV file.
Summaries grouped by unit_id, app_instance_id, and app_type_id.
A discussion on production schema change strategies (including two methods to safely add a column to large, live tables).
