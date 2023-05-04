import pandas as pd
# First DataFrame
dataframe1 = pd.DataFrame({'emp_id': ['A1', 'A2', 'A3', 'A4'],'Name': ['Name1', 'Name2', 'Name3', 'Name4']})
  
# Second DataFrame
dataframe2 = pd.DataFrame({'emp_id': ['B1', 'B2', 'B3', 'B4'],'Name': ['Name_1', 'Name_2', 'Name_3', 'Name_4']})
  
  
frames = [dataframe1, dataframe2]
  
result = pd.concat(frames)
print(result)
