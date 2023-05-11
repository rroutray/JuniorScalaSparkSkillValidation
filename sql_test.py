import common

# 1: Created a data frame by joining (inner) employee and department views in SQL 
emp_dept_inner_join_sql = spark_session.sql("SELECT * from employee e inner join department d ON e.emp_dept_id == d.dept_id")

# 2: Created a data frame by joining (left) employee and department views in SQL 
emp_dept_left_join_sql = spark_session.sql("SELECT * from employee e left join department d ON e.emp_dept_id == d.dept_id")

# 3: comparing the output of inner and left join of data frames
assert_pyspark_df_equal(emp_dept_inner_join_sql, expected_df)
assert_pyspark_df_equal(emp_dept_left_join_sql, expected_df)