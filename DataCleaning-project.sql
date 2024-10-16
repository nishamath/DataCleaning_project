-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


select * from layoffs limit 10;

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens

-- 1)-- Remove Duplicates
-- 2) Standardize the data
-- 3.Null values or blank values
-- 4) Remove any columns

create table layoff_stagingnew1
like layoffs;


select * from layoff_stagingnew1;

insert layoff_stagingnew1
select *
from layoffs;


-- lets identify duplicates --

select * from layoffs;

select * from layoff_stagingnew1;

select *,
row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off,`date`,stage,funds_raised_millions)as row_num
from layoff_stagingnew1;

with duplicate_cte1 as
(
select *,
row_number() over(
partition by company,location,industry,total_laid_off, percentage_laid_off,`date`,stage,funds_raised_millions)as row_num
from layoff_stagingnew1
)
select *
from duplicate_cte1
where row_num >1;

select * from layoff_stagingnew1
where company='Accolade';   -- we want to remove duplicate and keep one --





 

-- deleting duplicates-- 

with duplicate_cte as
(
select *,
row_number() over(
partition by company,location,total_laid_off,percentage_laid_off,`date`,stage,
country,funds_raised_millions)as row_num
from layoff_stagingnew1
)
delete 
from duplicate_cte
where row_num >1;  -- cannot delete saying cte connot update --



create table `layoffs_staging2` (
`company` text,
`location` text,
`industry` text,
`total_laid_off` int default null,
`percentage_laid_off` text,
`date` text,
`stage` text,
`country` text,
`funds_raised_millions` int default null,
`row_num` int
)engine=innodb default charset=utf8mb4 collate=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2 where row_num>1;

insert into layoffs_staging2
select *,
row_number() over(
partition by company,location,
total_laid_off,percentage_laid_off,`date`,stage,
country,funds_raised_millions)as row_num
from layoff_stagingnew1;



delete
from layoffs_staging2 where row_num>1;

select * 
from layoffs_staging2;

-- standardiszing data --

select company,(trim(company))
from layoffs_staging2;


update layoffs_staging2
set company=trim(company);




select distinct industry
from layoffs_staging2 order by 1;


select * from layoffs_staging2 
where industry like 'crypto%';

update layoffs_staging2
set industry='crypto'
where industry like 'crypto%';


select distinct location
from layoffs_staging2 order by 1;

select distinct country
from layoffs_staging2 order by 1;


select * from
layoffs_staging2
where country like 
'United States%' order by 1;

select distinct country,trim(trailing '.'from country)
from layoffs_Staging2
order by 1;


update layoffs_staging2
set country = trim('.' from country)
where country like "United States%";


select * from layoffs_staging2;
 
 
select `date`

 from layoffs_staging2;
 
 
 
 
 select `date`,
 str_to_date(`date`,'%m/%d/%Y')

 from layoffs_staging2;
 
 
 update layoffs_staging2
 set `date` = str_to_date(`date`,'%m/%d/%Y');
 
 
 alter table layoffs_staging2
 modify column `date`  DATE;
 
 select *
 from layoffs_staging2
 where total_laid_off  is null
 and percentage_laid_off is null;
 
 
 select distinct industry 
 from layoffs_staging2
 where industry is null 
 or industry = '';
 
 
 select * 
 from layoffs_staging2
 where
 company ='Airbnb';
 
 
 select *
 from layoffs_staging2 t1
 join layoffs_staging2 t2
   on t1.company=t2.company
   and t1.location=t2.location
where (t1.industry is null  or t1.industry='')
and t2.industry is not null;


update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
set t1.industry=t2.industry
where (t1.industry is null or t1.industry='')
and t2.industry is not null;



select * from layoffs_staging2;


alter table layoffs_staging2
drop column row_num;



 
 