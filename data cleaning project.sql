-- Data  cleaning
/*1.remove duplicates
2.Standardize the data
3. Null or blank values
4.Remove any columns that are not useful*/
SELECT *
FROM layoffs;
/*creating a table similar to layoffs table*/
CREATE TABLE layoffs_staging
LIKE layoffs;
SELECT *
FROM layoffs_staging;
INSERT layoffs_staging
SELECT *
FROM layoffs;
/*removing duplicates with the help of
cte*/
SELECT *
FROM layoffs_staging;


with duplicate_cte as(
     SELECT *,
     row_number() over( partition by company, location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
     FROM layoffs_staging )

select * 
from duplicate_cte
where row_num > 1;
select * 
from layoffs_staging
where company = 'Casper';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,`row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
select * 
from layoffs_staging2;
insert  into layoffs_staging2
SELECT *,
     row_number() over( partition by company, location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
     FROM layoffs_staging ;
     delete
     from layoffs_staging2
     where row_num > 1;
-- standardizing data
 select *
 from layoffs_staging2;
 select company, TRIM(company)
 from layoffs_staging2;
 update layoffs_staging2
 set company = TRIM(company);
 select distinct industry
 from layoffs_staging2
 order by 1;
 update layoffs_staging2
 set industry = 'crypto'
 where industry like 'crypto%';
 update layoffs_staging2
 set country = 'United States'
  where country like 'United States%';
  select distinct country
  from layoffs_staging2
  order by 1;
  update layoffs_staging2
  set `date` = str_to_date(`date`,'%m/%d/%Y');
  alter table layoffs_staging2
  modify column `date` date;
  select *
  from layoffs_staging2
  where total_laid_off is null
  and percentage_laid_off is null;
  update layoffs_staging2
  set industry = null
  where industry = '';
  select *
  from layoffs_staging2
  where industry is null
  or industry = '';
  select *
  from layoffs_staging2
  where company = 'Airbnb';
  select t1.industry,t2.industry
  from layoffs_staging2 as t1
  join layoffs_staging2 as t2
   on t1.company = t2.company
   where t1.industry is null 
   and t2.industry is not null;
   update layoffs_staging2 as t1
   join layoffs_staging2 as t2
   on t1.company = t2.company
   set t1.industry = t2.industrylayoffs_staging2layoffs_staging
   where t1.industry is null 
   and t2.industry is not null;
   select *
  from layoffs_staging2
  where total_laid_off is null
  and percentage_laid_off is null;
  delete
  from layoffs_staging2
  where total_laid_off is null
  and percentage_laid_off is null;
  select * 
  from layoffs_staging2;
  alter table layoffs_staging2  
 drop column row_num; 