create table employee (emp_id int,emp_name string,superior_emp_id int,year string,emp_dept_id int,gender string,salary decimal(38,2));
insert into employee (emp_id,emp_name,superior_emp_id,year,emp_dept_id,gender,salary) values (1,'Sachin',-1,'2000',1,'M',3000),
(2,'Virat',1,'2011',2,'M',4000),(3,'Ravinder',1,'2018',1,'M',1000),(4,'Kapil',2,'1980',1,'M',2000),(5,'Ravi',2,'1985',4,'M',5000),values (6,'Sunil',2,'1975',5,'M',1000);

create table department(dept_id int, department_name string);
insert into department (dept_id, department_name) values (1, 'Finance Dept'),(2, 'Marketing Dept'),(3, 'Sales Dept'),(4, 'IT Dept'),(5, 'HR Dept');

select * into emp_dept_join_inner from (select emp_id, emp_name, superior_emp_id, year, emp_dept_id, gender, salary, 3 as bonus_per, 3*salary as bonus, case when salary <= 4000 then 'A' else 'B' end as grade, dept_id, department_name from employee_testing e inner join department d on e.emp_dept_id = d.dept_id);

select * into emp_dept_join_left from (select emp_id, emp_name, superior_emp_id, year, emp_dept_id, gender, salary, 3 as bonus_per, 3*salary as bonus, case when salary <= 4000 then 'A' else 'B' end as grade, dept_id, department_name from employee_testing e left join department d on e.emp_dept_id = d.dept_id);

select * from employee_testing e inner join department d on e.emp_dept_id = d.dept_id inner join emp_dept_join ed on e.emp_dept_id = ed.emp_dept_id;
