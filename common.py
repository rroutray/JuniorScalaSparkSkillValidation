import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import when
import pytest
from pyspark_test import assert_pyspark_df_equal

# 1: define spark context object
sc = SparkContext.getOrCreate(conf=conf)
spark_session = SparkSession(sc)

def create_emp_df():
   """define employee data frame with column names and values"""
   employee = [(1,"Sachin",-1,"2000","1","M",3000),(2,"Virat",1,"2011","2","M",4000),(3,"Ravinder",1,"2018","1","M",1000),(4,"Kapil",2,"1980","1","M",2000),(5,"Ravi",2,"1985","4","M",5000),(6,"Sunil",2,"1975","5","M",1000)]
   employee_columns = ["emp_id","emp_name","superior_emp_id","year","emp_dept_id","gender","salary"]
   employee_df = spark_session.createDataFrame(data=employee, schema = employee_columns)

   # added new static column of bonus percent to employee data frame
   employee_df.withColumn("bonus_per", lit(2))

   # added new bonus amount for each employee in employee data frame
   employee_df.withColumn("bonus", employee_df.salary*2)

   # added new employee grade column based upon salary of employee
   employee_df.withColumn("emp_grade",when((employee_df.salary < 4000), lit("A")).otherwise(lit("B")))
   return employee_df

def create_dept_df():
   """define department data frame with column names and values"""
   department = [("Finance Dept",1),("Marketing Dept",2),("Sales Dept",3),("IT Dept",4),("HR Dept",5)]
   department_columns = ["department_name","dept_id"]
   department_df = spark_session.createDataFrame(data=department, schema = department_columns)
   return department_df

def create_expected_df():
   """define expected data frame"""
   expected_join_data = [(1,"Sachin",-1,"2000","1","M",3000,3,9000,"A",1,"Finance Dept"),(2,"Virat",1,"2011","2","M",4000,3,12000,"A",2,"Marketing Dept"),(3,"Ravinder",1,"2018","1","M",1000,3,3000,"A",1,"Finance Dept"),(4,"Kapil",2,"1980","1","M",2000,3,6000,"A",1,"Finance Dept"),(5,"Ravi",2,"1985","4","M",5000,3,15000,"B",4,"IT Dept"),(6,"Sunil",2,"1975","5","M",1000.3,3000,"A",5,"HR Dept")]
   expected_columns = ["emp_id","emp_name","superior_emp_id","year","emp_dept_id","gender","salary","bonus_per","bonus","grade","dept_id","department_name"]
   expected_df = spark_session.createDataFrame(data=expected_join_data, schema = expected_columns)

employee_df = create_emp_df()
department_df = create_dept_df()
expected_df = create_expected_df()

# 2: Created temporary table/views (employee & department) for SQL query
employee_df.createOrReplaceTempView("employee")
employee_df.createOrReplaceTempView("department")