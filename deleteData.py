import pandas as pd
import pyodbc
from configparser import ConfigParser
from openpyxl import load_workbook

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

file_name = 'Resource Catalog (5).xlsx'
sheet_name = 'InTASC aligned resources'

try:
  df = pd.read_excel(file_name, sheet_name=[sheet_name])
  data = pd.concat(df)

except Exception as e:
  print(e)


cursor = conn.cursor()
cursor.fast_executemany = True

colArr = data.columns
titleCol = colArr[11]
pathCol = colArr[10]

#range = 2 - data.shape[0] + 1
maxRange = data.shape[0] + 1

#take input and check for validity
inp = input("Enter rows to delete, separated by a comma: ")

inp = inp.replace(' ', '')
inp_list = inp.split(',')

num_list = []
for i in inp_list:
  if(not i.isdigit()):
    print("Must provide valid numbers within range")
    break
 
  if(int(i) > maxRange or int(i) < 2):
    print("Number must be within range.")
    break
  num_list.append(int(i) - 2)

id_list = []
getId = "select ResourceId from core.resource where Title = ? and FileSharePath = ?"

for num in num_list:
  row = cursor.execute(getId, data[titleCol][num], data[pathCol][num])
  resId = row.fetchval()
  id_list.append([resId])

print(id_list)


#sql query to delete specified resource
delPop = "delete from core.ResourcePopulationType where ResourceId = ?"
delTag = "delete from core.ResourceResourceTag where ResourceId = ?"
delContent = "delete from core.ResourceContentArea where ResourceId = ?"
delInd = "delete from core.ResourceIndicator where ResourceId = ?"
delGrade = "delete from core.ResourceGrade where ResourceId = ?"
delGradeBand = "delete from core.ResourceGradeBand where ResourceId = ?"
delRes = "delete from core.Resource where ResourceId = ?"


#commit the changes into the database
cursor.executemany(delPop, id_list)
cursor.executemany(delTag, id_list)
cursor.executemany(delContent, id_list)
cursor.executemany(delInd, id_list)
cursor.executemany(delGrade, id_list)
cursor.executemany(delGradeBand, id_list)
cursor.executemany(delRes, id_list)
cursor.commit()

wb = load_workbook(filename=file_name)
sheet = wb[sheet_name]

for i in inp_list:
  sheet.delete_rows(int(i), amount= 1)
  wb.save(filename=file_name)




