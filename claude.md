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


-----------------------------------

I wanted to redesign this project to a batch tool to perform multiple opeations, Strategy design pattern.

Like in java we have interface and we can implementent multiple sevice in differnt form. Like wise, here we have find and forget as the  inerface layer and implementaion will be build based on the requirement.

How I see the final product - during the piepline run, I will give required paramter and there should be a drop down to choose the behaviour or the batch process.
Option 1. Pasrse the XML in given csv and perpare a results json which will have track of find summber to use at forget tool, based on the input hash.
Option 2. I only want the result the trasaction id is end with _c, so i expect the result will be the path and trasaction id with _c in it, do not read the hash input, just read the file and pick the raw.

So the overall project will be layered in 3. 

Layer 1 - parse the input arguments for the second layer
Layer 2 - actual find, based on the input option, perpare the map with transaction id _c OR read the input hash data and parse the xml and perpare the result
Layer 3 - forget the give result data. It can have options like, should the orginal s3 file need to replaced by delete older or when upload new file apply a life cycle policy to older file version to delete automaticly

I wanted a system to track the processed file records - to avoid the repeated execution.
It can either a dynamodb table or Update the actual file tag with a marker.
Befor the find process starts, make sure to check the flag.

I would be needed a common email template to send notificatoion based on the events.
Events like - after getting triggered from pipeline and at the end of finish, or during a error happened and the whole job stuck or killed.
This part i will give more details later, just give placeholder everywhere.

So the forget solution is only reading the result of the middle layer and perform the delete.
