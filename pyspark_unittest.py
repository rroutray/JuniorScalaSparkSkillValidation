import pytest
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark_test import assert_pyspark_df_equal
from distutils import dir_util

sc = SparkContext.getOrCreate(conf=conf)
spark_session = SparkSession(sc)

# Created employee data frame
employee = [(1,"Sachin",-1,"2000","1","M",3000),(2,"Virat",1,"2011","2","M",4000),(3,"Ravinder",1,"2018","1","M",1000),(4,"Kapil",2,"1980","1","M",2000),(5,"Ravi",2,"1985","4","M",5000),(6,"Sunil",2,"1975","5","M",1000)]
employeeColumns = ["emp_id","emp_name","superior_emp_id","year","emp_dept_id","gender","salary"]
employeeDF = spark.createDataFrame(data=employee, schema = employeeColumns)

# Added 3 columns to employee data frame
employeeDF.withColumn("bonus_per", lit(2)).show()
employeeDF.withColumn("bonus", employeeDF.salary*2).show()
employeeDF.withColumn("emp_grade",when((employeeDF.salary < 4000), lit("A")).otherwise(lit("B"))).show()

# Created department data frame
department = [("Finance Dept",1),("Marketing Dept",2),("Sales Dept",3),("IT Dept",4),("HR Dept",5)]
departmentColumns = ["department_name","dept_id"]
departmentDF = spark.createDataFrame(data=department, schema = departmentColumns)

# Joined employee and department data frame
dfJoin = employeeDF.join(departmentDF,employeeDF.emp_dept_id ==  departmentDF.dept_id,"inner")

# Load the expected result from CSV file to compare with 
expected_df = spark.read.option("header",True).csv("employee_dept.csv")

# Comparing the 2 data frames for equality
assert_pyspark_df_equal(dfJoin, expected_df)