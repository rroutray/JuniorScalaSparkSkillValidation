import common

employee_df = create_emp_df()
department_df = create_dept_df()

# 1: Joined (inner) and display result of employee and department data frames
emp_dept_inner_join = employee_df.join(department_df,employee_df.emp_dept_id ==  department_df.dept_id,"inner")
emp_dept_inner_join.show()

# 2: Joined (left) and display result of employee and department data frames
emp_dept_left_join = employee_df.join(department_df,employee_df.emp_dept_id ==  department_df.dept_id,"left")
emp_dept_left_join.show()

# 3: Joined the output of employee and department data frame with employee data frame
multiple_join = employee_df.join(department_df,employee_df.emp_dept_id == department_df.dept_id,"inner").join(emp_dept_inner_join,employee_df.emp_dept_id == emp_dept_inner_join.emp_dept_id,"inner")
multiple_join.show()

# 4: Created temporary table/views (employee & department) for SQL query
employee_df.createOrReplaceTempView("employee")
employee_df.createOrReplaceTempView("department")

# 5: Created a data frame by joining (inner) employee and department views in SQL 
emp_dept_inner_join_sql = spark_session.sql("SELECT * from employee e inner join department d ON e.emp_dept_id == d.dept_id").show(truncate=False)

# 6: Created a data frame by joining (left) employee and department views in SQL 
emp_dept_left_join_sql = spark_session.sql("SELECT * from employee e left join department d ON e.emp_dept_id == d.dept_id").show(truncate=False)

# 7: comparing the final data frames from different joins of employee, department and its joined output
assert_pyspark_df_equal(emp_dept_left_join_sql, emp_dept_left_join)
assert_pyspark_df_equal(emp_dept_inner_join_sql, emp_dept_inner_join)