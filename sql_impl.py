import common

# 1: Created a data frame by joining (inner) employee and department views in SQL 
emp_dept_inner_join_sql = spark_session.sql("SELECT * from employee e inner join department d ON e.emp_dept_id == d.dept_id").show(truncate=False)

# 1: Created a data frame by joining (left) employee and department views in SQL 
emp_dept_left_join_sql = spark_session.sql("SELECT * from employee e left join department d ON e.emp_dept_id == d.dept_id").show(truncate=False)
