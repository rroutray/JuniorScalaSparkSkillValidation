from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import when
from pyspark_test import assert_pyspark_df_equal

# define spark context object
sc = SparkContext.getOrCreate(conf=conf)
spark_session = SparkSession(sc)

# define employee data frame with column names and values
employee = [(1,"Sachin",-1,"2000","1","M",3000),(2,"Virat",1,"2011","2","M",4000),(3,"Ravinder",1,"2018","1","M",1000),(4,"Kapil",2,"1980","1","M",2000),(5,"Ravi",2,"1985","4","M",5000),(6,"Sunil",2,"1975","5","M",1000)]
employeeColumns = ["emp_id","emp_name","superior_emp_id","year","emp_dept_id","gender","salary"]
employeeDF = spark_session.createDataFrame(data=employee, schema = employeeColumns)
# display employee data frame
employeeDF.show()

# added new static column of bonus percent to employee data frame
employeeDF.withColumn("bonus_per", lit(2)).show()

# added new bonus amount for each employee in employee data frame
employeeDF.withColumn("bonus", employeeDF.salary*2).show()

# added new employee grade column based upon salary of employee
employeeDF.withColumn("emp_grade",when((employeeDF.salary < 4000), lit("A")).otherwise(lit("B"))).show()

# define department data frame with column names and values
department = [("Finance Dept",1),("Marketing Dept",2),("Sales Dept",3),("IT Dept",4),("HR Dept",5)]
departmentColumns = ["department_name","dept_id"]
departmentDF = spark_session.createDataFrame(data=department, schema = departmentColumns)
# display department data frame
departmentDF.show()

# Joined (inner) and display result of employee and department data frames
dfJoin = employeeDF.join(departmentDF,employeeDF.emp_dept_id ==  departmentDF.dept_id,"inner")
dfJoin.show()

# Joined (left) and display result of employee and department data frames
dfLeftJoin = employeeDF.join(departmentDF,employeeDF.emp_dept_id ==  departmentDF.dept_id,"left")
dfLeftJoin.show()

# Joined the output of employee and department data frame with employee data frame
dfFinal = employeeDF.join(departmentDF,employeeDF.emp_dept_id == departmentDF.dept_id,"inner").join(dfJoin,employeeDF.emp_dept_id == dfJoin.emp_dept_id,"inner")
dfFinal.show()

# Created temporary table/views for SQL query
employeeDF.createOrReplaceTempView("EMPLOYEE")
departmentDF.createOrReplaceTempView("DEPARTMENT")
dfFinal.createOrReplaceTempView("FINALDF")

# Created a data frame by joining employee and department views in SQL 
joinDF = spark_session.sql("select * from EMPLOYEE e INNER JOIN DEPARTMENT d ON e.emp_dept_id == d.dept_id").show(truncate=False)

# Created a data frame multiple join of employee, department and joined result in SQL 
FinalDF = spark_session.sql("select * from FINALDF").show(truncate=False)

# comparing the final data frames from different joins of employee, department and its joined output
assert_pyspark_df_equal(FinalDF, dfFinal)
assert_pyspark_df_equal(joinDF, dfJoin)
assert_pyspark_df_equal(joinDF, dfLeftJoin)
