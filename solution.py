#!/usr/bin/env python3
"""
Data Engineering Take Home Assignment

This script covers:
1. Data Modeling and Exploration:
   - Loads sample data from multiple CSVs (APP_TYPE, APP_TYPE_REF, LOG_INSTANCE, APP_INSTANCE, POINT, LOG_RCD_B, LOG_RCD_F).
   - Merges them to derive UNIT_ID, APP_INSTANCE_ID, and APP_TYPE_ID for summarization.
   - Shows what other data might be helpful (timestamps, user info, error codes, etc.).
   - Assumptions:
       • One-to-many relationships (each APP_INSTANCE belongs to one APP_TYPE_REF, each REF has one UNIT_ID, etc.).
       • Data is relatively clean, though some columns are missing in certain files.

2. Summarizing at Three Levels:
   - UNIT_ID
   - APP_INSTANCE_ID
   - APP_TYPE_ID
   (All derived by chaining merges across the relevant CSVs.)

3. Data Scenario:
   - Discusses two strategies for adding a column to large, live production tables without downtime.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# SQLAlchemy Base for table definitions (if needed)
Base = declarative_base()


# -- Optional Star Schema (you can extend this if you want to store data in SQLite) --
class DimApplicationType(Base):
    __tablename__ = 'dim_application_type'
    app_type_id = Column(Integer, primary_key=True)
    app_type_description = Column(String)


class DimApplicationInstance(Base):
    __tablename__ = 'dim_application_instance'
    app_instance_id = Column(Integer, primary_key=True)
    app_type_ref_id = Column(Integer)  # Could link to APP_TYPE_REF


class DimUnit(Base):
    __tablename__ = 'dim_unit'
    unit_id = Column(Integer, primary_key=True)
    unit_name = Column(String)


class FactLogs(Base):
    __tablename__ = 'fact_logs'
    log_id = Column(Integer, primary_key=True, autoincrement=True)
    unit_id = Column(Integer, ForeignKey('dim_unit.unit_id'))
    app_instance_id = Column(Integer, ForeignKey('dim_application_instance.app_instance_id'))
    # Additional columns (timestamp, status, error codes) could be added as needed.


def load_csv_files(folder: str) -> dict:
    """
    Loads the CSV files from 'folder' into pandas DataFrames.
    """
    paths = {
        'APP_TYPE': os.path.join(folder, 'APP_TYPE.csv'),
        'APP_TYPE_REF': os.path.join(folder, 'APP_TYPE_REF.csv'),
        'LOG_INSTANCE': os.path.join(folder, 'LOG_INSTANCE.csv'),
        'APP_INSTANCE': os.path.join(folder, 'APP_INSTANCE.csv'),
        'POINT': os.path.join(folder, 'POINT.csv'),
        'LOG_RCD_B': os.path.join(folder, 'LOG_RCD_B.csv'),
        'LOG_RCD_F': os.path.join(folder, 'LOG_RCD_F.csv')
    }

    dfs = {}
    for name, path in paths.items():
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                dfs[name] = df
                print(f"Loaded {name} => {df.shape[0]} rows, {df.shape[1]} columns")
            except Exception as e:
                print(f"Error loading {name}: {e}")
        else:
            print(f"Warning: {name} file not found at {path}")
    return dfs


def create_data_warehouse_schema(db_url='sqlite:///data_warehouse.db'):
    """
    Creates a simple star schema in SQLite.
    store data or just do in-memory merges.
    """
    try:
        engine = create_engine(db_url, echo=False)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        print("Data warehouse schema created successfully.\n")
        return sessionmaker(bind=engine)
    except SQLAlchemyError as e:
        print(f"Error creating data warehouse schema: {e}")
        return None


def summarize_data(dfs: dict):
    """
    Summarize data at UNIT_ID, APP_INSTANCE_ID, and APP_TYPE_ID levels by chaining merges.

    LOG_INSTANCE doesn't have UNIT_ID or APP_INSTANCE_ID, so we do:
      LOG_INSTANCE
        -> merges on point_id with POINT (which has app_instance_id)
        -> merges on app_instance_id with APP_INSTANCE (which has app_type_ref_id)
        -> merges on app_type_ref_id with APP_TYPE_REF (which has unit_id, app_type_id)

    Once we have that merged DataFrame, we can group by:
      - unit_id
      - app_instance_id
      - app_type_id
    """
    # Check if essential data is present
    needed = ['LOG_INSTANCE', 'POINT', 'APP_INSTANCE', 'APP_TYPE_REF']
    missing = [key for key in needed if key not in dfs]
    if missing:
        print(f"Cannot summarize because these DataFrames are missing: {missing}")
        return

    # Merge step by step
    try:
        df_merged = (
            dfs['LOG_INSTANCE']
            .merge(dfs['POINT'][['point_id', 'app_instance_id']], on='point_id', how='left')
            .merge(dfs['APP_INSTANCE'][['app_instance_id', 'app_type_ref_id']], on='app_instance_id', how='left')
            .merge(dfs['APP_TYPE_REF'][['app_type_ref_id', 'unit_id', 'app_type_id']], on='app_type_ref_id', how='left')
        )
    except Exception as e:
        print(f"Error merging DataFrames: {e}")
        return

    # Summarize by unit_id
    if 'unit_id' in df_merged.columns:
        by_unit = df_merged.groupby('unit_id').size().reset_index(name='log_count')
        print("\nSummary by UNIT_ID (derived via merges):")
        print(by_unit.head())
    else:
        print("\nNo 'unit_id' column found after merging.")

    # Summarize by app_instance_id
    if 'app_instance_id' in df_merged.columns:
        by_app_inst = df_merged.groupby('app_instance_id').size().reset_index(name='log_count')
        print("\nSummary by APP_INSTANCE_ID (derived via merges):")
        print(by_app_inst.head())
    else:
        print("\nNo 'app_instance_id' column found after merging.")

    # Summarize by app_type_id
    if 'app_type_id' in df_merged.columns:
        by_app_type = df_merged.groupby('app_type_id').size().reset_index(name='log_count')
        print("\nSummary by APP_TYPE_ID (derived via merges):")
        print(by_app_type.head())
    else:
        print("\nNo 'app_type_id' column found after merging.")


def production_schema_change_discussion():
    """
    Discuss two possible solutions for adding a column to large, live tables in production.
    """
    print("\nData Scenario: Adding a Column to Large, Live Tables\n")
    print("Method 1: Online Schema Change Tools (e.g., pt-online-schema-change, gh-ost)")
    print(" - Create a new table with the updated schema, copy data incrementally, and track changes.")
    print(" - Pros: Very little downtime, minimal locking, suitable for large production databases.")
    print(" - Cons: Requires careful setup, monitoring, and potential extra disk usage.\n")

    print("Method 2: Shadow Table + Rolling Update")
    print(" - Create a shadow table with the new schema. Write to both old and new tables (dual-write).")
    print(" - Gradually backfill historical data into the shadow table. Once verified, switch over.")
    print(" - Pros: Potential for zero downtime if done carefully.")
    print(" - Cons: More complex. Risk of data inconsistency if dual-writing isn't managed properly.\n")


def main():
    data_folder = 'take_home_data'  # Adjust if needed

    # 1. Load CSVs
    dfs = load_csv_files(data_folder)

    # 2. (Optional) Create a star schema in SQLite
    session_maker = create_data_warehouse_schema()
    if session_maker is None:
        print("Could not create DB schema, but we'll continue with in-memory merges.\n")

    # 3. Summarize data by unit_id, app_instance_id, and app_type_id
    summarize_data(dfs)

    # 4. Production schema change discussion
    production_schema_change_discussion()

    print("\nData processing and analysis completed.")


if __name__ == '__main__':
    main()
