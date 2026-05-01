set SQL_SAFE_UPDATES=0;
show variables like 'secure_file_priv';
drop table customer_info;
create database final_project5;
select*from customer_info;
update customer_info set Gender= null where Gender='';
update customer_info set Age= null where Age='';
alter table customer_info modify age int null;

create table transactions
(date_new DATE,
Id_check INT,
ID_client INT,
Count_products DECIMAL(10,3),
Sum_payment DECIMAL (10,2));

Load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/transactions_info.csv"
into table transactions
fields terminated by ','
lines terminated by '\r\n'
ignore 1 rows
(date_new, Id_check, ID_client, Count_products, Sum_payment);

select*from transactions;
#(информация о транзакциях за период с 01.06.2015 по 01.06.2016), нужно вывести:
#список клиентов с непрерывной историей за год, то есть каждый месяц на регулярной основе 
#без пропусков за указанный годовой период, средний чек за период с 01.06.2015 по 01.06.2016, 
#средняя сумма покупок за месяц, количество всех операций по клиенту за период;
#информацию в разрезе месяцев:

with monthly as (select ID_client, 
date_format(date_new,'%Y-%m') as months,
sum(Sum_payment) as month_sum,
count(*) as operations,
avg(Sum_payment) as avg_payment
from transactions
where date_new between '2015-06-01'and '2016-06-01'
group by ID_client, months),

full_clients as (
select ID_client
from transactions
where date_new between '2015-06-01'and '2016-06-01'
group by ID_client
having count(distinct date_format(date_new,'%Y-%m'))=12)

select m.ID_client, m.months, m.month_sum, m.operations, m.avg_payment from monthly m
join full_clients f on m.ID_client=f.ID_client
order by m.ID_client, m.months;

#средняя сумма чека в месяц;
select ID_client, 
date_format(date_new,'%Y-%m') as months,
sum(Sum_payment)/count(*) as avg_check
from transactions
where date_new between '2015-06-01'and '2016-06-01'
group by ID_client, months;

#среднее количество операций в месяц;
select ID_client, 
date_format(date_new,'%Y-%m') as months,
count(*) as avg_operation
from transactions
where date_new between '2015-06-01'and '2016-06-01'
group by ID_client, months;

#среднее количество клиентов, которые совершали операции;
select avg(clients_per_months) as avg_clients
from (select date_format(date_new,'%Y-%m') as months,
count(distinct ID_client) as clients_per_months
from transactions
where date_new between '2015-06-01'and '2016-06-01'
group by months) t;

#долю от общего количества операций за год и долю в месяц от общей суммы операций;
#за год
select ID_client, count(*) as client_operation,
count(*)/(select count(*)
from transactions
where date_new between '2015-06-01'and '2016-06-01') as share_operation
from transactions where date_new between '2015-06-01'and '2016-06-01'
group by ID_client;

#за месяц
select ID_client, 
date_format(date_new,'%Y-%m') as months,
sum(Sum_payment)/sum(sum(Sum_payment)) over (partition by date_format(date_new,'%Y-%m')) as share_month_sum
from transactions
where date_new between '2015-06-01'and '2016-06-01'
group by ID_client, months;

#вывести % соотношение M/F/NA в каждом месяце с их долей затрат;
select date_format(date_new,'%Y-%m') as months,
c.Gender,
sum(t.Sum_payment) as gender_sum,
count(distinct t.ID_client) as gender_clients,
sum(t.Sum_payment)/sum(sum(t.Sum_payment)) over (partition by date_format(t.date_new,'%Y-%m')) as share_spend,
count(distinct t.ID_client)/sum(count(distinct t.ID_client)) over (partition by date_format(t.date_new,'%Y-%m')) as share_clients
from transactions t
join customer_info c on t.Id_client=c.Id_client
where t.date_new between '2015-06-01'and '2016-06-01'
group by months, Gender;

#возрастные группы клиентов с шагом 10 лет и отдельно клиентов, у которых нет данной информации, с параметрами сумма и количество операций за весь период, и поквартально - средние показатели и %.
select age_group, quart,
sum(Sum_payment) as total_sum,
count(*) as total_operations,
avg(sum(Sum_payment)) over (partition by age_group) as avg_sum_group,
sum(Sum_payment) /sum(sum(Sum_payment)) over (partition by quart) as share_in_quarter
from(select t.Id_client, t.Sum_payment,
quarter(t.Date_new) as quart,
case 
when c.Age is null then 'Unknown'
else concat(floor(c.Age / 10) * 10, '-', floor(c.Age / 10) * 10 + 9) end as age_group
from transactions t
join customer_info c on t.Id_client = c.Id_client
where t.Date_new between '2015-06-01' and '2016-06-01') base
group by age_group, quart;