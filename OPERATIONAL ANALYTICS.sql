create database job_analytics;

use job_analytics;

create table job_data(
ds date,
 job_id  int primary key,
 actor_id int ,
 event varchar (20),
 language varchar (20),
 time_spent int,
 org varchar(20)
);

select * from job_data;

describe job_data;

# Tasks:

/*Jobs Reviewed Over Time:
Objective: Calculate the number of jobs reviewed per hour for each day in November 2020.
Your Task: Write an SQL query to calculate the number of jobs reviewed per hour for each day in November 2020.
*/

SELECT 
DS AS ACTIVITY_DATE,
COUNT(JOB_ID) AS JOBS_REVIEWED,
SUM(TIME_SPENT)/3600 AS HOURS 
 FROM JOB_DATA  GROUP BY DS ORDER BY ACTIVITY_DATE;


/*Throughput Analysis:
Objective: Calculate the 7-day rolling average of throughput (number of events per second).
Your Task: Write an SQL query to calculate the 7-day rolling average of throughput. Additionally, explain whether you prefer using the daily metric or the 7-day rolling average for throughput, and why.
*/

select ds, j_per_s as jobs_reviewed_per_s, 
round(avg(j_per_s)over(order by ds rows between 6 preceding and current row),5) as rolling_avg
from
(select ds, round(cast(count(event) as float)/cast(sum(time_spent)as float),5) as j_per_s from job_data
group by ds) d;

/*Language Share Analysis:
Objective: Calculate the percentage share of each language in the last 30 days.
Your Task: Write an SQL query to calculate the percentage share of each language over the last 30 days.
*/

select language, round(cnt*100/sum(cnt)over(),2) as percentage_share
from 
(select language, count(language) as cnt
from job_data
group by language)d
group by language;

SELECT * from job_data;

/*Duplicate Rows Detection:
Objective: Identify duplicate rows in the data.
Your Task: Write an SQL query to display duplicate rows from the job_data table.
*/
select actor_id, count(actor_id) as actor_count
from job_data
group by actor_id
having actor_count>1;



# Case Study 2: Investigating Metric Spike
# Tasks:

USE JOB_ANALYTICS;

 
CREATE TABLE USERS(
user_id	INT,
created_at	VARCHAR(500),
company_id	INT,
language	VARCHAR(100),
activated_at VARCHAR(500),
state VARCHAR(100)
);

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
INTO TABLE users
FIELDS TERMINATED BY ','
enclosed by  '"'
LINES TERMINATED BY '\n'
ignore 1 rows;
 
 select * FROM USERS;
 
ALTER TABLE USERS ADD COLUMN TEMP_CREATED_AT DATETIME;
UPDATE USERS SET TEMP_CREATED_AT = STR_TO_DATE(CREATED_AT,'%d-%m-%Y %H:%i');
ALTER TABLE USERS DROP COLUMN CREATED_AT;
ALTER TABLE USERS CHANGE COLUMN TEMP_CREATED_AT CREATED_AT  DATETIME;
 
CREATE TABLE EVENTS(
user_id	 INT,
occurred_at	 VARCHAR(500),
event_type VARCHAR(500),
event_name VARCHAR(500),
location VARCHAR(100),
device VARCHAR(100),
user_type INT
);

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/EVENTS.csv"
INTO TABLE EVENTS
FIELDS TERMINATED BY ','
enclosed by  '"'
LINES TERMINATED BY '\n'
ignore 1 rows;

SELECT * FROM EVENTS;

ALTER TABLE EVENTS ADD COLUMN TEMP_OCCURED_AT DATETIME;
UPDATE EVENTS SET TEMP_OCCURED_AT = STR_TO_DATE(OCCURRED_AT,'%d-%m-%Y %H:%i');
ALTER TABLE EVENTS DROP COLUMN OCCURRED_AT;
ALTER TABLE EVENTS CHANGE COLUMN TEMP_OCCURED_AT OCCURRED_AT  DATETIME;
 
CREATE TABLE EMAIL_EVENTS(
user_id	 INT,
occurred_at VARCHAR(100),
action	 VARCHAR(100),
user_type INT
);

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
INTO TABLE EMAIL_EVENTS
FIELDS TERMINATED BY ','
enclosed by  '"'
LINES TERMINATED BY '\n'
ignore 1 rows;

SELECT * FROM EMAIL_EVENTS;

ALTER TABLE EMAIL_EVENTS ADD COLUMN TEMP_OCCURRED_AT DATETIME;
UPDATE EMAIL_EVENTS SET TEMP_OCCURRED_AT = STR_TO_DATE(OCCURRED_AT,'%d-%m-%Y %H:%i');
ALTER TABLE EMAIL_EVENTS DROP COLUMN OCCURRED_AT;
ALTER TABLE EMAIL_EVENTS CHANGE COLUMN TEMP_OCCURRED_AT OCCURRED_AT  DATETIME;
 
 SELECT * FROM USERS;
 SELECT * FROM EVENTS;
 SELECT * FROM EMAIL_EVENTS;
 
/*Weekly User Engagement:
Objective: Measure the activeness of users on a weekly basis.
Your Task: Write an SQL query to calculate the weekly user engagement.
*/
 
select extract(week from occurred_at) as weeks, 
count(distinct user_id) as no_of_users from events 
where event_type="engagement"
group by weeks order by weeks;

/* User Growth Analysis:
Objective: Analyze the growth of users over time for a product.
Your Task: Write an SQL query to calculate the user growth for the product.
*/

SELECT
    DATE_FORMAT(created_at, '%Y-%m-01') AS month_start_date,
    COUNT(DISTINCT user_id) AS total_users
FROM users
GROUP BY month_start_date
ORDER BY month_start_date;

/* Weekly Retention Analysis:
Objective: Analyze the retention of users on a weekly basis after signing up for a product.
Your Task: Write an SQL query to calculate the weekly retention of users based on their sign-up cohort.
*/

select * from events;
select extract(week from occurred_at) as weeks, 
count(distinct user_id) as no_of_users from events
where event_type="signup_flow" and event_name="complete_signup" 
group by weeks order by weeks;

/* Weekly Engagement Per Device:
Objective: Measure the activeness of users on a weekly basis per device.
Your Task: Write an SQL query to calculate the weekly engagement per device.
*/

select * from events;
select device, extract(week from occurred_at) as weeks, 
count(distinct user_id) as no_of_users from events 
where event_type="engagement"
group by device, weeks order by weeks; 

/*Email Engagement Analysis:
Objective: Analyze how users are engaging with the email service.
Your Task: Write an SQL query to calculate the email engagement metrics.
*/

select * from email_events;
SELECT
    action,
    COUNT(DISTINCT user_id) AS unique_users_count,
    COUNT(*) AS total_actions_count
FROM
    email_events
GROUP BY
    action
ORDER BY
    action;


