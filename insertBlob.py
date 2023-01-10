
import os
from sqlite3 import Row
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from configparser import ConfigParser

file = "config.ini"
config = ConfigParser()
config.read(file)
access = config['blob_upload']

CONNECTION_STRING = access['connection_string']
FOLDER_NAME = access['folder_name']
CONTAINER_NAME = access['container_name']
DIRECTORY_NAME = access['directory_name']
   
class insertBlob:
  def __init__(self, data, report_name, conn):
    self.o = open(report_name, "w")
    self.report_name = report_name
    self.data = data
    self.cursor = conn.cursor()

    try:
      print("Connecting to azure blob storage...")
      blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    except Exception as ex:
      print("Exception: ")
      print(ex)

    self.container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    self.property_list = []
    self.container_list = []

  def populateDict(self):
    self.search_dict = {}

    row = self.cursor.execute("select FileName, SharedFileName from core.resource")
    listRow = row.fetchall()

    for l in listRow:
      if(l[0] != None):
        extension = l[0].split('.')[-1]
        valName = '.'.join([l[1], extension])
        self.search_dict[l[0]] = valName



  def setUpContainerList(self):
    blob_list = self.container_client.list_blobs(name_starts_with = "currated/")
    for blob in blob_list:
      split_name = blob.name.split('/')
      self.container_list.append(split_name[1])

  def upload_all_files(self):

    if(self.validateFileName()):
      #populate the search dictionary
      self.populateDict()

      #list with all the guid of resources already in container
      self.setUpContainerList()

      insertedCount = 0
      
      for file in self.search_dict:
        if(self.search_dict[file] in self.container_list):
          self.o.write(f"File already in blob container: {file}\n")
        else:
  
          self.upload_file(file)
          insertedCount  += 1

      self.o.write("Total files uploaded: " + str(insertedCount))
      print("Finished uploading %d files successfully" % insertedCount)
      self.getInsertedProperties()

    else:
      print("File names do not match.")
      return
      

  def upload_file(self, file_name):

    blob_name = '/'.join([DIRECTORY_NAME, self.search_dict[file_name]])
  
   
    upload_file_path = os.path.join(FOLDER_NAME, file_name)

    print(f"Uploading file - {file_name}")
  
    with open(upload_file_path, "rb") as data:
      blob_client = self.container_client.upload_blob(name = blob_name, data=data)

    self.o.write(f"Uploaded file -  {file_name} as {blob_name} \n")
    self.property_list.append(blob_client.get_blob_properties())


  def getInsertedProperties(self):
    return self.property_list

  #validate whether the filenames in database matches with the actual filename in the folder
  def validateFileName(self):
    row = self.cursor.execute("select FileName from core.resource")
    names = row.fetchall()
    dir_list = os.listdir(FOLDER_NAME)

    for n in names:
      if(n[0] != None and n[0] not in dir_list):
        print(f"Invalid file name: {n[0]} \n")
        return False
    return True
 