import pandas as pd
import helpers

class validateData:
  def __init__(self, dataSheet, connection, file_path):
    self.dataSheet = dataSheet
    self.conn = connection
    self.colArr = self.dataSheet.columns
    self.colLen = self.dataSheet.shape[1]
    self.rowLen = self.dataSheet.shape[0]
    self.cursor = self.conn.cursor()
    self.file_path = file_path
    self.col_dict = {}

  #validates all fields and returns true or false
  def validateAll(self):
    self.populateDict()

    val = True
    with open(self.file_path, "w") as o:
      val = self.validateIndicator(o)

      if(not val):
        return val

      for r in self.col_dict.keys():
        isStr = self.col_dict[r] == "Str"
        if(not self.validate(r, o, isStr)):
          val = False

    return val

  def validateIndicator(self, o):
    #get primary and secondary columns and the list of all indicators
    primIndicators = self.dataSheet[self.colArr[0]]
    secondIndicators = self.dataSheet[self.colArr[1]]
    indList = self.getIndicatorList()

    #validate all indicators
    valid = True
    for i in range(0, self.rowLen):
      if(pd.isnull(primIndicators[i]) or not isinstance(primIndicators[i], float)):
        valid = False
        o.write("Must provide valid Primary Indicator in row " + str(i + 2) + "\n")
      else:
        if(primIndicators[i] not in indList):
          valid = False
          o.write("Primary Indicator number not in key in row " + str(i + 2) + "\n")
      
      if(not pd.isnull(secondIndicators[i]) and isinstance(secondIndicators[i], float)):
        if(secondIndicators[i] not in indList):
          valid = False
          o.write("Secondary Indicator number not in key in row " + str(i + 2) + "\n")

    return valid
    

  def validate(self, colNum, o, isStr):
    
    resCol = self.dataSheet[self.colArr[colNum]]
    if(not isStr):
      resList = self.getList(self.col_dict[colNum][0], self.col_dict[colNum][1])
    valid = True
    i = 2
    for res in resCol:
      #check for empty cell
      if(pd.isnull(res)):
        o.write("Empty cell in column: " + self.colArr[colNum] + " in row: " + str(i) + "\n")
        valid = False
      elif(not isStr):
        #if its string or multiple integers
        if(isinstance(res, str)):
          newRes = helpers.removeComma(res)
          newRes = newRes.replace(" ", "")
          splitRes = newRes.split(',')
      
          for r in splitRes:
            if(not r.isdigit()):
              valid = False
              o.write("Must consist of valid numbers separated by comma in column: " + self.colArr[colNum] + " in row: " + str(i) + "\n")
            else:
              if(int(r) not in resList):
                valid = False
                o.write("Invalid number in column: " + self.colArr[colNum] + " in row: " + str(i) + "\n")
          #if there's a single integer
        elif(isinstance(res, int)):
          if(res not in resList):
            valid = False
            o.write("Invalid number in column: " + self.colArr[colNum] + " in row: " + str(i) + "\n")
        else:
          valid = False
          o.write("Invalid entry in column: " + self.colArr[colNum] + " in row: " + str(i) + "\n")
      i += 1

    return valid

  #returns a list of possible indicators
  def getIndicatorList(self):
    indicList = self.cursor.execute("select concat(s.[order], '.', i.level) [StandardIndicator] "
                                        + "from assessment.indicator i "
                                        + "join assessment.standard s "
                                        + "on i.StandardId = s.id "
                                        + "order by concat(s.[order], '.', i.level)")
    indicators = [float(a[0]) for a in indicList.fetchall()]
    return indicators

  def getList(self, colName, tableName):
    row = self.cursor.execute("select " + colName + " from " + tableName)
    rowVals = row.fetchall()
    rowValList = [a[0] for a in rowVals]
    return rowValList

  def populateDict(self):
    self.col_dict = {2:("Id", "org.Grade"),3:("GradeBandId", "org.GradeBand"),
    4:("Id", "org.ContentArea"),5:("PopulationTypeId", "core.PopulationType"),
    6:("ResourceTagId", "core.ResourceTag"), 7:("ResourceTypeId", "core.ResourceType"),
    9:("MimeTypeId", "core.MimeType"), 10: "Str", 11: "Str", 
    15:("LicenseTypeId", "core.LicenseType"), 18:("StateId", "core.StateSpecificResource")}