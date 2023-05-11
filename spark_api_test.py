import common

# Joined employee and department data frame
emp_dept_inner_df = employee_df.join(department_df,employee_df.emp_dept_id ==  department_df.dept_id,"inner")

# Joined employee and department data frame
emp_dept_left_df = employee_df.join(department_df,employee_df.emp_dept_id ==  department_df.dept_id,"left")

# Comparing the 2 data frames for equality
assert_pyspark_df_equal(emp_dept_inner_df, expected_df)
assert_pyspark_df_equal(emp_dept_left_df, expected_df)