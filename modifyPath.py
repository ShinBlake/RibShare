import pyodbc
import pandas as pd
from configparser import ConfigParser

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


try:
  conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT='+port+';DATABASE='+database+';UID='+username+';PWD='+password)

except pyodbc.Error as ex:
  sqlstate = ex.args[1]
  print(sqlstate)

o = open("modified_paths.txt", "w")
cursor = conn.cursor()

row = cursor.execute("select FileName, FileSharePath,SharedFileName from core.resource")
cursor.fast_executemany = True
nameList = row.fetchall()

newNameList = []

pathURL = "https://ribbitlearningsandbox.blob.core.windows.net/resource/currated/"

#file name, shared path, guid

#new file path = pathURL + guid + extension
for n in nameList:
  if(n[0] is None):
    newNameList.append([str(n[1]), str(n[2])])
  else:
    extension = str(n[0]).split('.')[-1]
    newName = pathURL + str(n[2]) + "." + extension
    newNameList.append([newName, str(n[2])])


#list with new filesharepath, sharedfilename

updateName = "update core.resource SET FileSharePath = ? Where SharedFileName = ?"

cursor.executemany(updateName, newNameList)

cursor.commit()