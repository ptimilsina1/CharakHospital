use charak;

select * from charak_data;
desc charak_data;

# find the top 3 doctors of departments with max visits in 2078.
SELECT *
FROM (
	SELECT *, DENSE_RANK() OVER (PARTITION BY Department ORDER BY visit_count DESC) AS drn
	FROM (
		SELECT COUNT(*) AS visit_count, Doctor, Department
		FROM charak_data
		GROUP BY Doctor, Department
		ORDER BY visit_count DESC
	) AS A
) AS B
WHERE drn <= 3;
---------------------
#Export data to csv
SELECT 'visit_count', 'Doctor', 'Department', 'drn'
UNION
SELECT *
FROM (
	SELECT *, DENSE_RANK() OVER (PARTITION BY Department ORDER BY visit_count DESC) AS drn
	FROM (
		SELECT COUNT(*) AS visit_count, Doctor, Department
		FROM charak_data
		GROUP BY Doctor, Department
		ORDER BY visit_count DESC
	) AS A
) AS B
WHERE drn <= 3
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/charak_visit_counts.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';


SHOW VARIABLES LIKE 'secure_file_priv';
create table charak_visit (visit_count int, Doctor varchar(100),Department varchar(100));
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/charak_visit_counts.csv'
INTO TABLE charak_visit
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from charak_visit;

# Find the patient name which starts with s,p,a and ends with a,i
select * from charak_data where Patient_Name regexp '^[SPA].*[ai]$';

#find first and last name from the patient name
select substring_index(Patient_Name,' ',1) as first_name,substring_index(Patient_Name,' ',-1) as last_name from charak_data;

# Find the hourly visits for 2077 of month 5 (Bhadra) where visits greater than 50
select  year(Date_Time) as visit_year,month(Date_Time) as visit_month,hour(Date_Time) as visit_hour,count(*) as visit_counts from charak_data 
where year(Date_Time) = 2077 and month(Date_Time) = 5
group by visit_year,visit_month,visit_hour having visit_counts >50 order by visit_counts desc ;

# Get real age in year 61Y >> 61, 60M >> 5 year 365D >>1 so on
select *, REGEXP_SUBSTR(Age,'[0-9]+') as age_num, REGEXP_SUBSTR(Age,'[a-z]+') as age_str ,
            case when REGEXP_SUBSTR(Age,'[a-z]+')='Y' then REGEXP_SUBSTR(Age,'[0-9]+')
                 when REGEXP_SUBSTR(Age,'[a-z]+')='M' then round(REGEXP_SUBSTR(Age,'[0-9]+')/12)
                 when REGEXP_SUBSTR(Age,'[a-z]+')='D' then round(REGEXP_SUBSTR(Age,'[0-9]+')/365)
                 else REGEXP_SUBSTR(Age,'[0-9]+') end as real_age from charak_data;

# make age_gender field and group it to get counts yearly

#power bi practice query
                 
select count(*) as visit_counts,Doctor, Department,year(Date_Time),month(Date_Time),Gender,Appointment_Type,age_category from 
	(select *, 
         case when real_age<=5 then '0-5'
              when real_age>5 and real_age <=15 then '5-15'
              when real_age > 15 and real_age <= 60 then '15-60'
              when real_age>60 then '60+'
              end as age_category from 
                   (select *, REGEXP_SUBSTR(Age,'[0-9]+') as age_num, REGEXP_SUBSTR(Age,'[a-z]+') as age_str ,
                        case when REGEXP_SUBSTR(Age,'[a-z]+')='Y' then REGEXP_SUBSTR(Age,'[0-9]+')
                             when REGEXP_SUBSTR(Age,'[a-z]+')='M' then round(REGEXP_SUBSTR(Age,'[0-9]+')/12)
                             when REGEXP_SUBSTR(Age,'[a-z]+')='D' then round(REGEXP_SUBSTR(Age,'[0-9]+')/365)
                             else REGEXP_SUBSTR(Age,'[0-9]+') end as real_age from charak_data)
                             as A) as B 
                                    where year(Date_Time) is not null
group by Doctor, Department,year(Date_Time),month(Date_Time),Gender,Appointment_Type, age_category;


#next way:

select count(*) visit_count, Appointment_Type, age_category,Department,Doctor,District,Gender,year(Date_Time) as visit_year, month(Date_Time) as visit_month from
			(
             select *, case when real_age<5 then '0-5'
					    when real_age>=5 and real_age <=15 then '5-15'
                        when real_age>=16 and real_age <=60 then '16-60'
                        when real_age>60 then '60+'
                        end as age_category from 
                        (
                        select *,case when substr(Age,-1)='Y' then regexp_substr( Age, '[0-9]+')
					   when substr(Age,-1)='M' then round(regexp_substr( Age, '[0-9]+')/12)
                       when substr(Age,-1)='D' then round(regexp_substr( Age, '[0-9]+')/365)
						else regexp_substr( Age, '[0-9]+') end as real_age from charak_data
                         ) as A
                         
            ) as B 
            group by  Appointment_Type,age_category,Department,Doctor,District,Gender,visit_year, visit_month ;



----------------------------
select count(*) visit_count, Appointment_Type, age_category,Department,Doctor,District,Gender,year(Date_Time) as visit_year, nepali_month  from
			(
             select *, case when real_age<5 then '0-5'
					    when real_age>=5 and real_age <=15 then '5-15'
                        when real_age>=16 and real_age <=60 then '16-60'
                        when real_age>60 then '60+'
                        end as age_category from 
                        (
                        select *,case when substr(Age,-1)='Y' then regexp_substr( Age, '[0-9]+')
					   when substr(Age,-1)='M' then round(regexp_substr( Age, '[0-9]+')/12)
                       when substr(Age,-1)='D' then round(regexp_substr( Age, '[0-9]+')/365)
						else regexp_substr( Age, '[0-9]+') end as real_age,
                        case when month(Date_Time)=1 then 'Baishakh'
                when month(Date_Time)=2 then 'Jestha'   
                when month(Date_Time)=3 then 'Ashad'
                when month(Date_Time)=4 then 'Shrawan'
                when month(Date_Time)=5 then 'Bhadra'
                when month(Date_Time)=6 then 'Ashoj'
                when month(Date_Time)=7 then 'Kartik'
                when month(Date_Time)=8 then 'Mangsir'
                when month(Date_Time)=9 then 'Poush'
                when month(Date_Time)=10 then 'Magh'
                when month(Date_Time)=11 then 'Falgun'
                when month(Date_Time)=12 then 'Chaitra'
                end as nepali_month from charak_data
                         ) as A
                         
            ) as B 
            group by  Appointment_Type,age_category,Department,Doctor,District,Gender,visit_year, nepali_month ;


select count(*) visit_count, Appointment_Type, age_category,Department,Doctor,District,Gender,year(Date_Time) as visit_year, month_name from
			(
             select *, case when real_age<5 then '0-5'
					    when real_age>=5 and real_age <=15 then '5-15'
                        when real_age>=16 and real_age <=60 then '16-60'
                        when real_age>60 then '60+'
                        end as age_category from 
                        (
                        select *,case when substr(Age,-1)='Y' then regexp_substr( Age, '[0-9]+')
					   when substr(Age,-1)='M' then round(regexp_substr( Age, '[0-9]+')/12)
                       when substr(Age,-1)='D' then round(regexp_substr( Age, '[0-9]+')/365)
						else regexp_substr( Age, '[0-9]+') end as real_age from charak_data  
						join nep_month  on month(Date_Time)= month_id
                         ) as A
                         
            ) as B 
            group by  Appointment_Type,age_category,Department,Doctor,District,Gender,visit_year, month_name  ;




                                 
                                 
	


