import common

employee_df = create_emp_df()
department_df = create_dept_df()
expected_df = create_expected_df()

# Joined employee and department data frame
emp_dept_inner_df = employee_df.join(department_df,employee_df.emp_dept_id ==  department_df.dept_id,"inner")

# Comparing the 2 data frames for equality
assert_pyspark_df_equal(emp_dept_inner_df, expected_df)