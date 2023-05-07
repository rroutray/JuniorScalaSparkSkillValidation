from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pyspark_test import assert_pyspark_df_equal

sc = SparkContext.getOrCreate(conf=conf)
spark_session = SparkSession(sc)

employee = [(1,"Sachin",-1,"2000","1","M",3000),(2,"Virat",1,"2011","2","M",4000),(3,"Ravinder",1,"2018","1","M",1000),(4,"Kapil",2,"1980","1","M",2000),(5,"Ravi",2,"1985","4","M",5000),(6,"Sunil",2,"1975","5","M",1000)]
employeeColumns = ["emp_id","emp_name","superior_emp_id","year","emp_dept_id","gender","salary"]
employeeDF = spark_session.createDataFrame(data=employee, schema = employeeColumns)
employeeDF.show()

department = [("Finance Dept",1),("Marketing Dept",2),("Sales Dept",3),("IT Dept",4),("HR Dept",5)]
departmentColumns = ["department_name","dept_id"]
departmentDF = spark_session.createDataFrame(data=department, schema = departmentColumns)
departmentDF.show()

dfJoin = employeeDF.join(departmentDF,employeeDF.emp_dept_id ==  departmentDF.dept_id,"inner")
dfJoin.show()

dfLeftJoin = employeeDF.join(departmentDF,employeeDF.emp_dept_id ==  departmentDF.dept_id,"left")
dfLeftJoin.show()

dfFinal = employeeDF.join(departmentDF,employeeDF.emp_dept_id == departmentDF.dept_id,"inner").join(dfJoin,employeeDF.emp_dept_id == dfJoin.emp_dept_id,"inner")
dfFinal.show()

employeeDF.createOrReplaceTempView("EMPLOYEE")
departmentDF.createOrReplaceTempView("DEPARTMENT")
dfFinal.createOrReplaceTempView("FINALDF")

joinDF = spark_session.sql("select * from EMPLOYEE e INNER JOIN DEPARTMENT d ON e.emp_dept_id == d.dept_id").show(truncate=False)
FinalDF = spark_session.sql("select * from FINALDF").show(truncate=False)

assert_pyspark_df_equal(FinalDF, dfFinal)
assert_pyspark_df_equal(joinDF, dfJoin)
assert_pyspark_df_equal(joinDF, dfLeftJoin)
