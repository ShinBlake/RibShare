def removeComma(s):
  if(isinstance(s, str) and s.endswith(',')):
    return s[:-1]
  else:
    return s
