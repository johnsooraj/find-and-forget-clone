I want to build a terraform modularized project to perform a batch procssing of S3 data.

This project have 2 phases:
phase 1: Find the matching records from S3
phase 2: Process the matching records

Phase 1:
- We have a data lake in s3 which is a compressed (gz) CSV file, which contains the following columns:
    - TRAN_ID (unique identifier for the transaction)
    - SEQ_NB (sequence number for the transaction)
    - PROFILE_ID (profile identifier)
    - PROD_OPT (product option)
    - TP_DB_TS (transaction processing timestamp)
    - RAW_RESPONSE (raw XML response from the transaction)
        - This raw XML response have customer phone, address, email, name, etc.

I will be providing a json data which will have a run_id, hash value, match_type(PHONE, EMAIL, NAME), and the value.
sameple:
{
    "run_id": "1234567890",
    "hash_value": "1234567890",
    "match_type": "PHONE"
}

I need to create a glue job which will read the json data and the data lake file, and then find the matching records from the data lake file.

The source of lake data is in S3 and its format is
s3://data-dake/DBD001/archive/2026-11-11/AB_CDEFG_DATA_11.tar.gz, once the file decompressed, it will be a same name and extension will be .csv

The output from the glue job will be a json file which will have the following structure:
{
    "run_id": "1234567890",
    "hash_value": "1234567890",
    "match_status": "MATCHED" | "NO_MATCH"
    "match_type": "PHONE",
    "data_identifier": {
        "<s3-arn for the archive file>": "transaction_id"
    }
}
What I'm planning is, after getting this result, I will run second glue job to process the matching records.

Second glue job will read the output from the first glue job and the data lake file, and then delete those transactions from the data lake file.

