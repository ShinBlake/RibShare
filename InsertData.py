import pandas as pd
import helpers
from openpyxl import load_workbook


def insertData(connection, data, file_name, sheet_name):
  #set up cursor and excel
  cursor = connection.cursor()
  cursor.fast_executemany = True
  colArr = data.columns
  rowLen = data.shape[0]

  o = open("Data_insert_report", "w")

  #for writing to excel
  work_book = load_workbook(filename=file_name)
  work_sheet = work_book[sheet_name]

  #lists of necessary columns for resources
  titleCol = data[colArr[11]]
  descCol = data[colArr[13]]
  citeCol = data[colArr[14]]
  mimeCol = data[colArr[9]]
  licenseCol = data[colArr[15]]
  pathCol = data[colArr[10]]
  typeCol = data[colArr[7]]
  stateCol = data[colArr[18]]
  fileNameCol = data[colArr[20]]
  
  
  #------------------------------------------------------------------------------------------------
  #Insert into resource table
  #------------------------------------------------------------------------------------------------

  #list to check which data has already been inputted.
  inserted = cursor.execute("select Title, FileSharePath from core.Resource")
  insertedData = []
  allInserted = inserted.fetchall()
  for row in allInserted:
    insertedData.append(list(row))

  insertResource = "insert into core.Resource(licensetypeid, title, [description], citation, [filename], mimetypeid, filesharepath, ResourceTypeId, StateSpecific) values (?,?,?,?,?,?,?,?,?)"

  #list of indices of rows to be added
  insertIndexList = []

  #create a list of all the info for bulk insert into resource table
  resourceList = []
  for i in range(0, rowLen):
    if([titleCol[i], pathCol[i]] not in insertedData):
      citation = None if pd.isnull(citeCol[i]) else citeCol[i]
      description = None if pd.isnull(descCol[i]) else descCol[i]
      fileName = None if pd.isnull(fileNameCol[i]) else fileNameCol[i]
      resourceList.append((int(licenseCol[i]), titleCol[i], description, citation, fileName, int(mimeCol[i]), pathCol[i], int(typeCol[i]), int(stateCol[i])))
      o.write(titleCol[i] + "\n")
      insertIndexList.append(i)
  
  #check if there are any resources left to insert
  if(resourceList):
    
    cursor.executemany(insertResource, resourceList)

  else:
    print("All data have already been inserted.")
    return

  #------------------------------------------------------------------------------------------------
  # Get resourceIds from resource table
  #------------------------------------------------------------------------------------------------

  #put resource Id, title and filepath from resource table into list
  rows = cursor.execute("select ResourceId, Title, FileSharePath from core.Resource")
  resourceIdList = rows.fetchall()
  
  #use the list generated to create a dictionary to lookup ID using title and filepath
  dict_res = {}
  for r in resourceIdList:
    dict_res[(r[1],r[2])] = r[0]

  #Search through the created dictionary to match each resourceId to corresponding row
  indId = {}
  for i in insertIndexList:
    title = titleCol[i]
    path = pathCol[i]
    #build dictionary to search resource Id with row index
    indId[i] = dict_res[(title,path)]


  #------------------------------------------------------------------------------------------------
  #prepare for indicator insertion
  #------------------------------------------------------------------------------------------------

  #get IndicatorId things
  indicator = cursor.execute("select Id, StandardId, Level from assessment.Indicator")
  resIndicList = indicator.fetchall()

  #put into dictionary
  indic_dict = {}
  for i in resIndicList:
    indic_dict[(i[1], i[2])] = i[0]

  # get standardId
  standard = cursor.execute("select Id, [order] from assessment.Standard")
  resourceStandardList = standard.fetchall()

  #put into lookup dictionary
  stand_dict = {}
  for s in resourceStandardList:
    stand_dict[s[1]] = s[0] 
  

  #------------------------------------------------------------------------------------------------
  #Insert all meta data
  #------------------------------------------------------------------------------------------------

  #columns from excel
  primCol = data[colArr[0]]
  secCol = data[colArr[1]] 
  gradeCol = data[colArr[2]]
  gradeBandCol = data[colArr[3]]
  contentCol = data[colArr[4]]
  popCol = data[colArr[5]]
  tagCol = data[colArr[6]]

  #list to use to insert data
  indList = []
  gradeList = []
  gradeBandList = []
  contentList = []
  popList = []
  tagList = []

  #sql script for insertion
  insertIndicator = "insert into core.ResourceIndicator (resourceid, indicatorid, isprimary, isactive) values (?, ?, ?, 1)"
  insertGrade = "insert into core.ResourceGrade(resourceid, gradeid, isactive) select ?, cast(value as int), 1 from string_split(?, ',')"
  insertGradeBand = "insert into core.ResourceGradeBand(resourceid, gradebandid, isactive) select ?, cast(value as int), 1 from string_split(?, ',')"
  insertContent = "insert into core.ResourceContentArea(resourceid, contentareaid, isactive) select ?, cast(value as int), 1 from string_split(?, ',')"
  insertPop = "insert into core.ResourcePopulationType(resourceid, populationtypeid, isactive) select ?, cast(value as int), 1 from string_split(?, ',')"
  insertTag = "insert into core.ResourceResourceTag(resourceid, resourcetagid, isactive) select ?, cast(value as int), 1 from string_split(?, ',')"

 
  for i in insertIndexList:

    #append to corresponding list
    gradeList.append((indId[i], helpers.removeComma(gradeCol[i])))
    gradeBandList.append((indId[i],int(gradeBandCol[i])))
    contentList.append((indId[i], helpers.removeComma(contentCol[i])))
    popList.append((indId[i], helpers.removeComma(popCol[i])))
    tagList.append((indId[i], helpers.removeComma(tagCol[i])))

    #for indicators

    #search and insert resourceId, indicatorId into the list
    primaryList = str(primCol[i]).split('.')
    primResId = indic_dict[stand_dict[int(primaryList[0])], int(primaryList[1])]
    indList.append((indId[i], primResId, 1))

    #check if there is secondaryIndicator and do the same
    if(not pd.isnull(secCol[i]) and isinstance(secCol[i], float)):
      secondaryList = str(secCol[i]).split('.')
      secResId = indic_dict[stand_dict[int(secondaryList[0])], int(secondaryList[1])]
      indList.append((indId[i], secResId, 0))
    

    # write to the excel file to indicate resource has been inserted
    work_sheet["T" + str(i + 2)] = "yes"
 
  #save and print result
  work_book.save(filename=file_name)

  #execute if any to be inserted
  if(insertIndexList):
    cursor.executemany(insertIndicator, indList)
    cursor.executemany(insertGrade, gradeList)
    cursor.executemany(insertGradeBand, gradeBandList)
    cursor.executemany(insertContent, contentList)
    cursor.executemany(insertPop, popList)
    cursor.executemany(insertTag, tagList)

    cursor.commit()
    
    #for rolling back the changes instead of committing: uncomment the above line
    # cursor.rollback()

  print(str(len(insertIndexList)) + " data(s) have been inserted.")




