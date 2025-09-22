#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Global Verifications Pipeline
import pandas as pd
import psycopg2
import re
from io import StringIO
import json
import numpy as np
from sqlalchemy import create_engine

# ------------------ Loading Data ------------------
# ------------------ Nexus DB Connection Details ------------------
nexus_user = "xxxxxx"
nexus_pwd = "xxxxxx"
nexus_port = xxxxxx
nexus_host = "xxxxxx.rds.amazonaws.com"
nexus_db = "xxxxxx"  # Ensure correct quoting for hyphenated DB names if needed

# ------------------ Create SQLAlchemy Engine ------------------
nexus_engine = create_engine(
    f'postgresql+psycopg2://{nexus_user}:{nexus_pwd}@{nexus_host}:{nexus_port}/{nexus_db}'
)
# ------------------ Define SQL Query for Selected Columns ------------------
call_report_query = """
    SELECT 
        c.id,
        c.name,
        c.phone as main_contact,
        c.updated_at,
        p.survey_id,
        p.country,
        p.start,
        p.activator_label,
        p.activator_id,
        p.survey_region,
        p.survey_channel,
        p.sn_mod,
        p.product_name,
        p.first_name,
        p.alternative_phone_label,
        p.residence_level1,
        p.residence_level2,
        p.residence_level3,
        p.preferred_language_to_be_called,
        p.submission_time,
        p.territory_sales_manager,
        p.team_leader,
        p.processing_date,
        p.created_at,
        p.call_attempts,
        p.notes,
        p.is_verified,
        p.last_name,
        p.cx_agent,
        p.follow_up_agent,
        p.preferred_time_to_be_called,
        p.preferred_time_to_be_called_cx,
        p.status,
        p.last_call_time,
        p.recent_disposition,
        p.priority,
        p.verification_type,
        p.biomass_prospect_delivery_date,
        p.metadata
    FROM public.calls_report_accumulated c
    LEFT JOIN public.sales_verification p
        ON c.phone = p.main_contact
    WHERE c.created_at > '2025-01-01'
      AND p.verification_type = 'SALES_VERIFICATION';
"""

# ------------------ Load Data into DataFrame ------------------
call_report = pd.read_sql_query(call_report_query, con=nexus_engine)


# Split 'name' into 'first_name' and 'last_name'
call_report[['first_name', 'last_name']] = call_report['name'].str.split(' ', 1, expand=True)

# Drop the original 'name' column
call_report.drop(columns=['name'], inplace=True)

# Clean 'main_contact' by removing all non-digit characters (e.g., +, spaces, dashes) and stripping spaces
call_report["main_contact"] = call_report["main_contact"].astype(str).str.replace(r"\D", "", regex=True).str.strip()


# Remove duplicate rows based on the 'phone' column, keeping the first occurrence
call_report = call_report.sort_values(by="updated_at", ascending=False).drop_duplicates(subset="main_contact", keep="first")

# For every record in call report, set source_callsheet = "Nexus Verification - Call Disposition"
call_report["source_callsheet"] = "Nexus Verification - Call Disposition"


# Database connection parameters for reading data
read_db_name = "xxxxxx"
read_db_password = "xxxxxx"
read_db_host = "xxxxxx.rds.amazonaws.com"
read_db_port = xxxxxx
read_user = "xxxxxx"

# Establish connection to Nexus (read database)
read_conn = psycopg2.connect(
    dbname=read_db_name,
    host=read_db_host,
    port=read_db_port,
    user=read_user,
    password=read_db_password,
    sslmode="require"
)

# Read tables from the database
tables = ["surveys", "survey_questions", "survey_responses", "sales_verification"]
dataframes = {}

# Function to read selected columns from the contacts table
def read_contacts_table(conn):
    query = "SELECT id, first_name, last_name, phone FROM contacts WHERE created_at > '2025-01-01'"
    return pd.read_sql_query(query, conn)

dataframes["contacts"] = read_contacts_table(read_conn)

for table in tables:
    query = f"SELECT * FROM {table}"
    dataframes[table] = pd.read_sql_query(query, read_conn)

# Close the read database connection
read_conn.close()

# Print column names for debugging
print("Columns in contacts:", dataframes["contacts"].columns)
print("Columns in sales_verification:", dataframes["sales_verification"].columns)


# dataframes["sales_verification"].to_csv("s3://xxxxxx-s3/3CX/sales_verification.csv", index=False)

# Keep only rows where verification_type is 'SALES_VERIFICATION'
dataframes["sales_verification"] = dataframes["sales_verification"][
    dataframes["sales_verification"]["verification_type"] == "SALES_VERIFICATION"
].reset_index(drop=True)

# For every record in call report, set source_callsheet = "Nexus Verification - Call Disposition"
dataframes["sales_verification"]["source_callsheet"] = "Nexus Verification - Sales Verification"

# Clean 'main_contact' by removing all non-digit characters (e.g., +, spaces, dashes) and stripping spaces
dataframes["sales_verification"]["main_contact"] = dataframes["sales_verification"]["main_contact"].astype(str).str.replace(r"\D", "", regex=True).str.strip()

#Append Call report accumulated to sales verification
dataframes["sales_verification"] = pd.concat([dataframes["sales_verification"], call_report], ignore_index=True)

# dataframes["sales_verification_APPENDED"].to_csv("s3://xxxxxx-s3/3CX/sales_verification_APPENDED.csv", index=False)

# Identify the phone column in contacts
contacts_phone_column = 'phone' if 'phone' in dataframes["contacts"].columns else 'main_contact'

# Identify the phone column in sales_verification
sales_verification_phone_column = 'phone' if 'phone' in dataframes["sales_verification"].columns else 'main_contact'

# Filter surveys
filtered_surveys = dataframes["surveys"][dataframes["surveys"]["id"].isin([8, 9])]

# # Filter questions
# filtered_questions = dataframes["survey_questions"][
#     (dataframes["survey_questions"]["id"].between(59, 77)) &
#     (dataframes["survey_questions"]["survey_id"].isin([8, 9]))
# ].drop(columns=["topic", "status"])

# Filter questions
filtered_questions = dataframes["survey_questions"][
    (
        (dataframes["survey_questions"]["id"].between(59, 77)) |
        (dataframes["survey_questions"]["id"] == 92)
    ) &
    (dataframes["survey_questions"]["survey_id"].isin([8, 9]))
].drop(columns=["topic", "status"])


# Filter responses
filtered_responses = dataframes["survey_responses"][
    dataframes["survey_responses"]["survey_question_id"].isin(filtered_questions["id"])
]

# Merge filtered responses with filtered questions
merged_df = pd.merge(filtered_questions, filtered_responses, left_on="id", right_on="survey_question_id")

# Merge contact details
nexus_response = dataframes["contacts"][dataframes["contacts"]["id"].isin(merged_df["contact_id"])]
nexus_qoai = pd.merge(nexus_response, merged_df, left_on="id", right_on="contact_id")
nexus_data = nexus_qoai[["first_name", "last_name", contacts_phone_column, "question", "answer"]]

# Convert data to wide format
nexus_data_wide = nexus_data.pivot(index=[contacts_phone_column], columns='question', values='answer').reset_index()

# Remove leading '+' from phone numbers
nexus_data_wide[contacts_phone_column] = nexus_data_wide[contacts_phone_column].str.replace(r"^\+", "", regex=True)
dataframes["sales_verification"][sales_verification_phone_column] = dataframes["sales_verification"][sales_verification_phone_column].str.replace(r"^\+", "", regex=True)

# Rename columns to ensure consistency
nexus_data_wide = nexus_data_wide.rename(columns={contacts_phone_column: 'phone'})
dataframes["sales_verification"] = dataframes["sales_verification"].rename(columns={sales_verification_phone_column: 'phone'})

# Merge with sales_verification
sales_verification_complete_data = pd.merge(dataframes["sales_verification"], nexus_data_wide, on='phone', how='left')

# Clean column names
sales_verification_complete_data.columns = [re.sub(r'[^\w\s]', '', col.lower().replace(' ', '_')) for col in sales_verification_complete_data.columns]

# Function to safely convert to string
def safe_convert_to_string(value):
    if pd.isna(value):
        return None
    return str(value)

# Apply the conversion to all columns
for col in sales_verification_complete_data.columns:
    sales_verification_complete_data[col] = sales_verification_complete_data[col].apply(safe_convert_to_string)

# Function to fix the JSON string by replacing single quotes with double quotes
def fix_json_string(json_str):
    if pd.isna(json_str) or json_str == '':
        return None
    try:
        # Replace single quotes with double quotes, but not within string values
        fixed_str = re.sub(r"(?<!\\)'(?!(?:(?<!\\)(?:\\\\)*'):)", '"', json_str)
        # Replace "True" and "False" with "true" and "false" for valid JSON
        fixed_str = fixed_str.replace(': True', ': true').replace(': False', ': false')
        return fixed_str
    except Exception as e:
        print(f"Error fixing JSON string: {str(e)}")
        return None

# Function to extract checklist columns from the 'metadata' column
def extract_checklist_columns(metadata_str):
    if pd.isna(metadata_str) or metadata_str == '':
        return pd.Series()
    try:
        metadata_fixed = fix_json_string(metadata_str)
        if metadata_fixed is None:
            return pd.Series()
        
        metadata = json.loads(metadata_fixed)
        
        if 'verification' in metadata and isinstance(metadata['verification'], list):
            checklist_data = {}
            for item in metadata['verification']:
                name = item['name']
                checklist_data[f"checklist_{name}_verified_status"] = str(item['verified'])
                checklist_data[f"checklist_{name}_updated_to"] = str(item['update_to']) if item['update_to'] else 'blank'
            return pd.Series(checklist_data)
        else:
            return pd.Series()
    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {str(e)}")
        print(f"Problematic metadata: {metadata_str[:200]}...")  # Print first 200 characters for debugging
        return pd.Series()
    except Exception as e:
        print(f"Unexpected error while processing metadata: {str(e)}")
        return pd.Series()
        
# Unnest the Metadata column and add Checklist columns
print("Attempting to extract checklist columns from metadata.")

# Apply the extraction function
checklist_columns = sales_verification_complete_data['metadata'].apply(extract_checklist_columns)

# Check if any checklist columns were extracted
if not checklist_columns.empty:
    # Merge the extracted checklist columns with the main DataFrame
    sales_verification_complete_data = pd.concat([sales_verification_complete_data, checklist_columns], axis=1)
    # Drop the original metadata column after extraction
    sales_verification_complete_data.drop(['metadata'], axis=1, inplace=True)
    print("Checklist columns successfully extracted and merged.")
else:
    print("No valid checklist data found in metadata.")
    
sales_verification_complete_data = sales_verification_complete_data.replace("blank", None)

# Apply regex replacement to all string cells [' and '] in the DataFrame
sales_verification_complete_data = sales_verification_complete_data.applymap(
    lambda x: re.sub(r"^\['(.*)'\]$", r"\1", x) if isinstance(x, str) else x
)

#Uniformize DRC records
sales_verification_complete_data['country'] = sales_verification_complete_data['country'].replace('Democratic Republic of Congo', 'DRC')
sales_verification_complete_data['country'] = sales_verification_complete_data['country'].replace('COD', 'DRC')
sales_verification_complete_data['country'] = sales_verification_complete_data['country'].replace('MDG', 'MADAGASCAR')
# Remove .0 and handle missing values
sales_verification_complete_data["survey_id"] = (
    sales_verification_complete_data["survey_id"]
    .fillna("")             # Replace NaN with empty string
    .replace([float("inf"), float("-inf")], "")  # Replace infinities
    .astype(str)            # Convert to string
    .str.replace(r"\.0$", "", regex=True)  # Remove .0 at the end
)

# Remove duplicate rows based on the 'phone' column, keeping the first occurrence
sales_verification_complete_data = sales_verification_complete_data.sort_values(by="updated_at", ascending=False).drop_duplicates(subset="phone", keep="first")

# Convert to numeric first, then back to string without decimals
sales_verification_complete_data["call_attempts"] = (
    pd.to_numeric(sales_verification_complete_data["call_attempts"], errors="coerce")
    .astype("Int64")  # keep as integer, supports nulls
    .astype(str)      # finally convert back to string
)

# Define mapping dictionary for standardization
country_map = {
    "KEN": "KENYA",
    "KENYA": "KENYA",
    "DRC": "DRC",
    "MOZ": "MOZAMBIQUE",
    "MOZAMBIQUE": "MOZAMBIQUE",
    "MWI": "MALAWI",
    "MALAWI": "MALAWI",
    "NGA": "NIGERIA",
    "NIGERIA": "NIGERIA",
    "TANZANIA": "TANZANIA",
    "ZAMBIA": "ZAMBIA",
    "MADAGASCAR": "MADAGASCAR"
}

# Apply mapping
sales_verification_complete_data["country"] = (
    sales_verification_complete_data["country"]
    .str.strip()  # remove extra spaces
    .str.upper()  # make uniform in case some are lowercase
    .map(country_map)
)

#Importing registrations data
# ------------------ DNA DB Connection Details ------------------
dna_host = "xxxxxx.rds.amazonaws.com"
dna_port = "xxxxxx"
dna_db   = "xxxxxx"
dna_user = "xxxxxx"
dna_pwd  = "xxxxxx"

# ------------------ Create SQLAlchemy Engine ------------------
from sqlalchemy import create_engine
import pandas as pd

dna_engine = create_engine(
    f'postgresql+psycopg2://{dna_user}:{dna_pwd}@{dna_host}:{dna_port}/{dna_db}'
)

# ------------------ Define SQL Query for Selected Columns ------------------
verification_surveys_query = """
    SELECT 
        main_contact AS phone,
        id as survey_id,
        sn_mod,
        team_leader
    FROM xxxxxx.registrations_surveys;
"""
# ------------------ Load Data into DataFrame ------------------
verification_surveys = pd.read_sql_query(verification_surveys_query, con=dna_engine)

# create mapping dictionary from verification_surveys
team_leader_map = verification_surveys.set_index("survey_id")["team_leader"].to_dict()

# map team_leader into master_call_sheet_combined
sales_verification_complete_data["team_leader"] = sales_verification_complete_data["team_leader"].fillna(
    sales_verification_complete_data["survey_id"].map(team_leader_map)
)

#Pushing to the warehouse
# Print debug information
print("Columns after processing:")
print(sales_verification_complete_data.columns)
print("\nSample of processed data (first 5 rows, first 10 columns):")
print(sales_verification_complete_data.iloc[:5, :10])

# Database connection parameters for writing data
write_db_name = "xxxxxx"
write_db_password = "xxxxxx"
write_db_host = "xxxxxx-1.rds.amazonaws.com"
write_db_port = xxxxxx
write_user = "xxxxxx"
db_schema = "xxxxxx"
db_table = "xxxxxx"

# Establish a connection to the target database
write_conn = psycopg2.connect(
    dbname=write_db_name,
    user=write_user,
    password=write_db_password,
    host=write_db_host,
    port=write_db_port
)

try:
    with write_conn.cursor() as write_cursor:
        # Set the search path to the dna schema
        write_cursor.execute(f"SET search_path TO {db_schema}")

        # Drop the table if it exists
        write_cursor.execute(f"DROP TABLE IF EXISTS {db_schema}.{db_table}")
        print(f"Table {db_schema}.{db_table} dropped if it existed.")

        # Create the table
        columns = ", ".join([f"{col} TEXT" for col in sales_verification_complete_data.columns])
        create_table_query = f"""
        CREATE TABLE {db_schema}.{db_table} (
            {columns}
        )
        """
        write_cursor.execute(create_table_query)
        print(f"Table {db_schema}.{db_table} created.")

        # Write the DataFrame to the target database
        buffer = StringIO()
        sales_verification_complete_data.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)

        write_cursor.copy_expert(f"COPY {db_schema}.{db_table} FROM STDIN WITH CSV DELIMITER E'\\t' NULL '\\N'", buffer)
        print(f"Data copied to {db_schema}.{db_table}.")

        # Grant permission to metabase_user
        grant_permission_query = f"GRANT SELECT ON TABLE {db_schema}.{db_table} TO metabase_user"
        write_cursor.execute(grant_permission_query)
        print("SELECT permission granted to metabase_user.")

    # Commit the changes
    write_conn.commit()
    print("Data processing and database operations completed successfully.")

except Exception as e:
    write_conn.rollback()
    print(f"An error occurred: {str(e)}")
    # Print more details about the error
    import traceback
    print(traceback.format_exc())

finally:
    write_conn.close()  # Ensure the connection is closed

import pandas as pd


# CX Verification Pipeline ---------------------------------------------------------------------------------------------------------
# S3 file path
cx_pipeline_historical_url = "s3://xxxxxx-s3/registrations supplementary datasets/historical_master_call_sheet_combined.parquet"

# Read parquet file
cx_pipeline_historical = pd.read_parquet(cx_pipeline_historical_url, engine="pyarrow")

# keep only the required columns
cx_pipeline_historical = cx_pipeline_historical[[
    # "survey_id",
    "name",
    "phone",
    "sn_mode",
    "call_status",
    "sn_cc",
    "cc_phone",
    "cc_name",
    "cc_alternative_phone",
    "call_max_date",
    "start_date",
    "call_agent",
    "call_attempts",
    "primary_fuel",
    "country",
    "source_callsheet"
]]

# create new dataframe with selected columns
nexus_verifications = sales_verification_complete_data[[
    # "survey_id",
    "first_name",
    "last_name",
    "phone",
    "sn_mod",
    "recent_disposition",
    "checklist_serial_number_updated_to",
    "checklist_phone_updated_to",
    "checklist_customer_name_updated_to",
    "checklist_alternative_phone_number_updated_to",
    "last_call_time",
    "start",
    "cx_agent",
    "call_attempts",
    "what_fuel_do_you_use_most_in_your_household_to_cook_",
    "country",
    "source_callsheet"
]].copy()

# concatenate first_name and last_name into a single column 'name'
nexus_verifications["name"] = (
    nexus_verifications["first_name"].fillna("").astype(str).str.strip() + " " +
    nexus_verifications["last_name"].fillna("").astype(str).str.strip()
).str.strip()

# drop the separate first_name and last_name columns
nexus_verifications.drop(columns=["first_name", "last_name"], inplace=True)

# reorder columns so 'name' appears first
nexus_verifications = nexus_verifications[[
    # "survey_id",
    "name",
    "phone",
    "sn_mod",
    "recent_disposition",
    "checklist_serial_number_updated_to",
    "checklist_phone_updated_to",
    "checklist_customer_name_updated_to",
    "checklist_alternative_phone_number_updated_to",
    "last_call_time",
    "start",
    "cx_agent",
    "call_attempts",
    "what_fuel_do_you_use_most_in_your_household_to_cook_",
    "country",
    "source_callsheet"
]]

# define the renaming mapping
rename_mapping = {
    # "survey_id": "survey_id",
    "name": "name",
    "phone": "phone",
    "sn_mod": "sn_mode",
    "recent_disposition": "call_status",
    "checklist_serial_number_updated_to": "sn_cc",
    "checklist_phone_updated_to": "cc_phone",
    "checklist_customer_name_updated_to": "cc_name",
    "checklist_alternative_phone_number_updated_to": "cc_alternative_phone",
    "last_call_time": "call_max_date",
    "start": "start_date",
    "cx_agent": "call_agent",
    "call_attempts": "call_attempts",
    "what_fuel_do_you_use_most_in_your_household_to_cook_": "primary_fuel",
    "country": "country",
    "source_callsheet": "source_callsheet"
}

# apply renaming
nexus_verifications.rename(columns=rename_mapping, inplace=True)

# combine both dataframes
master_call_sheet_combined = pd.concat(
    [cx_pipeline_historical, nexus_verifications],
    ignore_index=True
)

# ---- Further Transformations ----
# Convert all entries in 'call_status' column to uppercase
master_call_sheet_combined['call_status'] = master_call_sheet_combined['call_status'].str.upper()

# Trim spaces and replace NA with empty string for sn_mode, sn_cc, call_status
for col in ['sn_mode', 'sn_cc', 'call_status']:
    master_call_sheet_combined[col] = (
        master_call_sheet_combined[col]
        .fillna("")         # replace NaN with empty string
        .astype(str)        # ensure string type
        .str.strip()        # trim spaces
    )

# Replace "VERIFIED" with "FULLY VERIFIED" in call_status
master_call_sheet_combined['call_status'] = master_call_sheet_combined['call_status'].replace(
    {"VERIFIED": "FULLY VERIFIED"}
)

# Clean up 'sn_mode' column:
# - If the value contains a hyphen ("-"), split and keep only the part before the first hyphen
# - Otherwise, leave the value unchanged
master_call_sheet_combined["sn_mode"] = (
    master_call_sheet_combined["sn_mode"]
    .astype(str)                                      # ensure all values are strings
    .apply(lambda x: x.split("-", 1)[0].strip() if "-" in x else x)
)

#Capitalize sn_cc
master_call_sheet_combined["sn_cc"] = (
    master_call_sheet_combined["sn_cc"]
    .astype(str)       # ensure string type
    .str.upper()       # convert to uppercase
)

#clean sn_cc column
import re

def clean_sn(value):
    if pd.isna(value):
        return ""
    
    # Convert to string, uppercase, strip spaces
    val = str(value).upper().strip()
    
    # Remove known unwanted keywords
    if any(keyword in val for keyword in ["TEST", "FULLY VERIFIED", "DISTRITO", "NANDI", "SABON", "INCHWARA"]):
        return ""
    
    # Replace common separators with space
    val = re.sub(r"[,&/]|AND", " ", val)
    
    # Remove prefixes like 'SN ' or ':'
    val = re.sub(r"\bSN\b[: ]*", "", val)
    
    # Remove everything that is not A-Z0-9 or space
    val = re.sub(r"[^A-Z0-9 ]", " ", val)
    
    # Collapse multiple spaces
    val = re.sub(r"\s+", " ", val).strip()
    
    return val

# Apply cleaning
master_call_sheet_combined["sn_cc"] = master_call_sheet_combined["sn_cc"].apply(clean_sn)


#sn_match
master_call_sheet_combined["sn_match"] = master_call_sheet_combined["sn_mode"] == master_call_sheet_combined["sn_cc"]

#valid_sn_cc
# Get maximum length of existing sn_mode values
max_len = master_call_sheet_combined["sn_mode"].dropna().str.len().max()

# Set minimum length explicitly to 5 (not dynamic)
min_len = 5

# Build a regex pattern dynamically: min fixed at 5, max from sn_mode
pattern = rf"^[A-Z0-9]{{{min_len},{max_len}}}$"

# Apply validation: True if sn_cc follows same format as sn_mode
master_call_sheet_combined["valid_sn_cc"] = master_call_sheet_combined["sn_cc"].str.match(
    pattern,
    na=False
)

# Add today's date as process_date
master_call_sheet_combined["process_date"] = pd.Timestamp.today().normalize()

# convert start_date to datetime, set invalid ones to NaT
master_call_sheet_combined["start_date"] = pd.to_datetime(
    master_call_sheet_combined["start_date"], 
    errors="coerce"
)

# Clean 'main_contact' by removing all non-digit characters (e.g., +, spaces, dashes) and stripping spaces
master_call_sheet_combined["phone"] = master_call_sheet_combined["phone"].astype(str).str.replace(r"\D", "", regex=True).str.strip()

# Remove duplicate rows from the dataframe
master_call_sheet_combined = master_call_sheet_combined.drop_duplicates()

# create mapping dicts from verification_surveys
survey_id_map = verification_surveys.set_index("phone")["survey_id"].to_dict()
sn_mod_map = verification_surveys.set_index("phone")["sn_mod"].to_dict()

# map survey_id into master_call_sheet_combined
master_call_sheet_combined["survey_id"] = master_call_sheet_combined["phone"].map(survey_id_map)

# fill missing sn_mode values using sn_mod_map
master_call_sheet_combined["sn_mode"] = master_call_sheet_combined.apply(
    lambda row: sn_mod_map.get(row["phone"], row["sn_mode"]) if pd.isna(row["sn_mode"]) else row["sn_mode"],
    axis=1
)

# Convert specific text columns to datetime (date only)
master_call_sheet_combined[["call_max_date", "start_date", "process_date"]] = (
    master_call_sheet_combined[["call_max_date", "start_date", "process_date"]]
    .apply(pd.to_datetime, errors="coerce")  # convert to datetime
    .apply(lambda col: col.dt.date)          # keep only the date part
)

# Push to the warehouse
write_db_name = "xxxxx"
write_db_password = "xxxxx"
write_db_host = "xxxxx.rds.amazonaws.com"
write_db_port = xxxxx
write_user = "xxxxx"
db_schema = "xxxxx"
db_table = "xxxxx"

# Handle call_attempts column specifically - convert <NA> values to "0" as string
if 'call_attempts' in master_call_sheet_combined.columns:
    # Handle both NaN and <NA> values, then convert to string
    master_call_sheet_combined['call_attempts'] = master_call_sheet_combined['call_attempts'].astype(str).replace(['<NA>', 'nan', 'None'], '0')

# Establish a connection to the target database
write_conn = psycopg2.connect(
    dbname=write_db_name,
    user=write_user,
    password=write_db_password,
    host=write_db_host,
    port=write_db_port
)

try:
    with write_conn.cursor() as write_cursor:
        # Set the search path to the dna schema
        write_cursor.execute(f"SET search_path TO {db_schema}")
        
        # Drop the table if it exists
        write_cursor.execute(f"DROP TABLE IF EXISTS {db_schema}.{db_table}")
        print(f"Table {db_schema}.{db_table} dropped if it existed.")
        
        # Create the table
        columns = ", ".join([f"{col} TEXT" for col in master_call_sheet_combined.columns])
        create_table_query = f"""
        CREATE TABLE {db_schema}.{db_table} (
            {columns}
        )
        """
        write_cursor.execute(create_table_query)
        print(f"Table {db_schema}.{db_table} created.")
        
        # Write the DataFrame to the target database
        buffer = StringIO()
        master_call_sheet_combined.to_csv(
            buffer, index=False, header=False, sep='\t'
        )
        buffer.seek(0)
        
        write_cursor.copy_expert(
            f"COPY {db_schema}.{db_table} FROM STDIN WITH CSV DELIMITER E'\\t' NULL '\\N'", 
            buffer
        )
        print(f"Data copied to {db_schema}.{db_table}.")
        
        # Grant permission to metabase_user
        grant_permission_query = f"GRANT SELECT ON TABLE {db_schema}.{db_table} TO metabase_user"
        write_cursor.execute(grant_permission_query)
        print("SELECT permission granted to metabase_user.")
    
    # Commit the changes
    write_conn.commit()
    print("Data processing and database operations completed successfully.")
    
except Exception as e:
    write_conn.rollback()
    print(f"An error occurred: {str(e)}")
    # Print more details about the error
    import traceback
    print(traceback.format_exc())
    
finally:
    write_conn.close()  # Ensure the connection is closed

