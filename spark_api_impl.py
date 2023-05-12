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