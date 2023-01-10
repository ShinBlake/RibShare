
import pandas as pd
import validateData
import pyodbc
from configparser import ConfigParser
import InsertData
import insertBlob

#parse config for connectionstring data
file = "config.ini"
config = ConfigParser()
config.read(file)
access = config['Connection']

server = access['server']
database = access['database']
username = access['username']
password = access['password']
driver = access['driver']
port = access['port']

#create connection to azure database with connection string
try:
  conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT='+port+';DATABASE='+database+';UID='+username+';PWD='+password)

except pyodbc.Error as ex:
  sqlstate = ex.args[1]
  print(sqlstate)

file_name = 'Resource Catalog.xlsx'
sheet_name = 'InTASC aligned resources'

try:
  df = pd.read_excel(file_name, sheet_name=[sheet_name])
  data = pd.concat(df)

except Exception as e:
  print(e)

upload_report = "blob_upload_report"


# validate data
dataValidator = validateData.validateData(data, conn, "DataValidationReport.txt")
isVal = dataValidator.validateAll()

if(isVal):
  #insert into the sql database
  InsertData.insertData(conn, data, file_name, sheet_name)

  #insert files into the blod storage
  blob_inserter = insertBlob.insertBlob(data, upload_report, conn)
  blob_inserter.upload_all_files()

else:
  print("Invalid Data exists in sheet.")






  


