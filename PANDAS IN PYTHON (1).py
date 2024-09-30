# Databricks notebook source
# DBTITLE 1,EXPLORING DBFS
dbutils.help()


# COMMAND ----------

# DBTITLE 1,to see file path exist or not
dbutils.fs.ls(".")


display(dbutils.fs.ls("dbfs:/FileStore/tables/company1-4.csv"))

# COMMAND ----------

# DBTITLE 1,CREATE DATAFRAME
import pandas as pd

data = {'Name':['vidhi','shona','piyu'], 'Age' :[12,34,26],'Salary':[12000,25000,50000]}

d1 = pd.DataFrame(data)
print(d1)



# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/tables/company1-4.csv')

# COMMAND ----------

# DBTITLE 1,USING CSV FILE TO UPLOAD
# File location and type
file_location = "/FileStore/tables/company1-4.csv"
file_type = "csv"
 
# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

d1 = df.toPandas()
print(d1)
type(d1)

# COMMAND ----------

# DBTITLE 1,EXPLORE DATA IN PANDAS
import pandas as pd


# File location and type
file_location = "/FileStore/tables/company1-4.csv"
file_type = "csv"
 

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/company1-4.csv"
file_type = "csv"
 
# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

d1 = df.toPandas()
print(d1)
type(d1)

# COMMAND ----------

# DBTITLE 1,check first 10 columns
print(d1.head(10))

# COMMAND ----------

# DBTITLE 1,check last 5 columns
print(d1.tail(5))

# COMMAND ----------

# DBTITLE 1,check type of d1
print(type(d1))

# COMMAND ----------

# DBTITLE 1,check information of d1
print(d1.info)

# COMMAND ----------

# DBTITLE 1,check null values
print(d1.isnull().sum())

# COMMAND ----------

# DBTITLE 1,handle duplicates values


# COMMAND ----------


# File location and type
file_location = "/FileStore/tables/company1-4.csv"
file_type = "csv"
 
# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

d1 = df.toPandas()
print(d1)
type(d1)

# COMMAND ----------

print(d1.duplicated())

# COMMAND ----------

# Check for duplicate EEID values
print(d1["_c0"].duplicated())


# COMMAND ----------

print(d1['_c0'].duplicated())

# COMMAND ----------

# DBTITLE 1,REMOVE DUPLICATES
print(d1.drop_duplicates('_c0'))

# COMMAND ----------

# DBTITLE 1,ways to HANDLINF MISSING VALUES
print(d1.isnull().sum())

# COMMAND ----------

# DBTITLE 1,drop null values
print(d1.dropna())

# COMMAND ----------

# DBTITLE 1,use of replace values -- in nulls
print(d1)

# COMMAND ----------

import numpy as np
print(d1.replace(np.nan,'30000'))

# COMMAND ----------

# DBTITLE 1,now use specific category
import numpy as np 
d1['_c3'] =d1['_c3'].replace(np.nan,'24400')
print(d1)

# COMMAND ----------

print(d1)

# COMMAND ----------

# DBTITLE 1,now to fill values in names or gender
print(d1.fillna(method = 'ffill'))

# COMMAND ----------

print(d1.fillna(method= 'bfill'))

# COMMAND ----------

print(d1.isnull())

# COMMAND ----------

# DBTITLE 1,COLUMN TRANSFORMATION
print(d1)

# COMMAND ----------

# Convert the _c3 (salary) column to numeric in Pandas
d1['_c3'] = pd.to_numeric(d1['_c3'], errors='coerce')  # Convert to numeric (int/float)

# Now perform the operations using Pandas
d1.loc[(d1['_c3'] == 24400), "Getbonus"] = 'no bonus'
d1.loc[(d1['_c3'] > 25000), "Getbonus"] = 'bonus'

# Print the modified Pandas DataFrame
print(d1)


# COMMAND ----------

print(d1)

# COMMAND ----------

data = {"Months":["january","February","March","april"]}

d1 = pd.DataFrame(data)
print(d1)

# COMMAND ----------

# DBTITLE 1,if i want to add new column using old column --- use function
def myfun(a):
    return a[0:3]


d1['Short Months'] = d1['Months'].map(myfun)
print(d1)

# COMMAND ----------

# DBTITLE 1,GROUP BY
# Convert the first row of the DataFrame into column headers
# First, remove the first row and assign it as column names
new_header = d1.iloc[0]  # First row for the header
d1 = d1[1:]  # Take the data after the header row
d1.columns = new_header  # Assign new column names

# Now the columns are renamed based on the first row
print(d1)


# COMMAND ----------

print(d1)

# COMMAND ----------

# DBTITLE 1,group by based on salary using count on gender
gp = d1.groupby("salary").agg({"gender":'count'})
print(gp)

# COMMAND ----------

gp = d1.groupby(["salary","gender"]).agg({"EEID":'count'})
print(gp)

# COMMAND ----------



# Now group by 'Name' and find the max salary
gp = d1.groupby("Name").agg({'salary': 'max'})

# Print the result
print(gp)


# COMMAND ----------

# DBTITLE 1,MERGE ,  CONCATE and join
import pandas as pd

data1 = {"Emp id": ["E01","E02","E03","E04","E05","E06"],
        "Name":["vidhi","sagar","nhay","shyam","shona",'pihu'],
        "Age":[22,35,56,33,23,40]}

data2 = {"Emp id": ["E01","E07","E03","E04","E08","E06"],
         "Salary":[40000,45000,30000,50000,36000,25000]}

df1 = pd.DataFrame(data1)
print(df1)

df2 = pd.DataFrame(data2)
print(df2)
        




# COMMAND ----------

# DBTITLE 1,MERGE
print(pd.merge(df1,df2, on = "Emp id"))

# COMMAND ----------

print(pd.merge(df1,df2, on = "Emp id", how = 'left'))

# COMMAND ----------

print(pd.merge(df1,df2, on = 'Emp id', how = 'right'))

# COMMAND ----------

# DBTITLE 1,join
import pandas as pd 
print(pd.merge(df1,df2))

# COMMAND ----------

# DBTITLE 1,CONCAT
import pandas as pd

data1 = {"Emp id": ["E01","E02","E03","E04","E05","E06"],
        "Name":["vidhi","sagar","nhay","shyam","shona",'pihu'],
        "Age":[22,35,56,33,23,40]}

data2 =  {"Emp id": ["E07","E08","E09","E010","E011","E012"],
        "Name":["vihan","somya","ajay","seeta","pinky",'maya'],
        "Age":[22,35,56,33,23,40]}

df1 = pd.DataFrame(data1)
print(df1)

df2 = pd.DataFrame(data2)
print(df2)

# COMMAND ----------

# DBTITLE 1,CONCAT[DF1,DF2]


print(pd.concat([df1,df2]))

# COMMAND ----------

# DBTITLE 1,COMPARE PANDAS DATAFRAME
import pandas as pd

df1 = {"Fruits":["Mango","Apple","Orange","Bannana"],
       "Price": [100,150,50,35],
       "Quantity": [15,10,10,3]}

df1 = pd.DataFrame(df1)
print(df1)       

# COMMAND ----------

# DBTITLE 1,copy data
df2 = df1.copy()
print(df2)

df2.loc[0,"Price"]=120
df2.loc[1,"Price"]=175
df2.loc[3,"Price"] = 30


df2.loc[0,"Quantity"]=12
df2.loc[1,"Quantity"]=15
df2.loc[3,"Quantity"] =5

print(df2)



# COMMAND ----------

# DBTITLE 1,COMPARE DATAFRAMES
print(df1.compare(df2))


# COMMAND ----------

print(df1.compare(df2,align_axis=0))

# COMMAND ----------

# DBTITLE 1,USE KEEP SHAPE WHEN U WANT TO SEE CHANGES WHERE UMADE
print(df1.compare(df2,keep_shape=True))

# COMMAND ----------

# DBTITLE 1,PIVOTING
data = {"keys":["K1","K2","K1","K2"],
        "Names":["Vidhi","Shona","yash","Maya"],
        "Houses":["Red","Blue","Green","Red"]}

d1 = pd.DataFrame(data)    
print(d1)    

# COMMAND ----------

print(d1.pivot("keys","Names","Houses"))

# COMMAND ----------

data = {"keys":["K1","K2","K1","K2"],
        "Names":["Vidhi","Shona","yash","Maya"],
        "Houses":["Red","Blue","Green","Red"],
        "Grades":["3rd","8th","9th","8th"]}

d1 = pd.DataFrame(data)    
print(d1)    

print(d1.pivot(index = "keys", columns = "Names", values = ["Houses","Grades"]))

# COMMAND ----------

# DBTITLE 1,MELTING
data = {"Names":["Vidhi","Shona","yash","Maya"],
        "Houses":["Red","Blue","Green","Red"],
        "Grades":["3rd","8th","9th","8th"]}

d2 = pd.DataFrame(data)
print(d2)

# COMMAND ----------

print(pd.melt(d2,id_vars=["Names"],value_vars=['Houses']))

# COMMAND ----------

print(pd.melt(d2,id_vars=["Names"],value_vars=['Houses','Grades']))

# COMMAND ----------

# DBTITLE 1,if i want to change variable and values name
print(pd.melt(d2,id_vars=["Names"],value_vars=['Houses','Grades'],var_name="Houses&grades",value_name="Valuesss"))
