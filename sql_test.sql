select count(*) from emp_dept_join_inner
go
select count(*) from emp_dept_join_left
go
select top 2 * from emp_dept_join_inner order by emp_id asc
go
select top 2 * from emp_dept_join_left order by emp_id asc
go
select * from emp_dept_join_inner where emp_id = 2
go
select * from emp_dept_join_left where emp_id = 2