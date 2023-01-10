Python Script to read the excel file and insert into the sql database, upload the currated resources into the blod storage in azure portal.
(Need to predownload all the currated files, not very efficient)

/Currated_res : folder containing all the downloaded curated files to insert into the blob storage

Driver.py: The driver script that calls the data validation script, insert the data into the sql database, and insert the downloaded files into the blob storage in azure portal.

InsertData.py: Script that actually inserts the resource data into the sql database. The inserte data information is stored in "Data_insert_report.txt"

insertBlob.py: script that inserts the actual files into the azure blob storage. The inserted data information is stored in "blob_upload_report.txt"

deleteData.py: delete specified resources from the database.

validateData.py: Validates the data in the excel file prior to inserting the resources into the database and the blob storage.

modifyPath.py: Change the filesharepath with the GUID of each resource. (only used to change paths initially)
