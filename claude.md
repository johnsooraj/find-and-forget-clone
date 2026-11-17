I'm expecting 2 script one for find and prepare a list, second is the script that read the report and load the archive data and run sql query or python loop then save as new file with same name.
In first script report must group based on the file, suppose the same file needs deletion for multiple record, all must done with a single execution.

Suppose If i have 12 month archive file in s3 - the total deletion and file creation will be 12. No matter the number of record to be deleted.

# Expected Input
# The Hash value will SHA256 value of the user data, maybe phone or email or address
[
    {
        "run_id": "uuid",
        "hash_value": "string",
        "match_type": "EMAIL | PHONE | NAME | ADDRESS"
    }
]

# Expected Output
# The input data will be tracked here as well with the run id
[
    {
        "run_id": "uuid",
        "hash_value": "string",
        "match_result": "MATCH | NO_MATCH | ERROR",
        "data_identifier": {
            "<where the data was found, file location in s3>": "transaction id from the csv file"
        }
    }
]

So the matching will be, parse the xml data and hash the field value then compare it with the hash value provided in the input.
If it matches, return "MATCH" along with the location of the data and transaction id. If it does not match, return "NO_MATCH". If there is an error during processing, return "ERROR".


The second script delete will read the file location and know the transaction id (that table raw id, if we load data into spark data frame).
The drop the record for the give list.