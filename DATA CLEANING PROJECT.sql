-- Data Cleaning

-- This is one of the project that I gone through with 'Alex The Analyst'
-- With remarks and my first ever project on SQL

-- Data Set: https://github.com/AlexTheAnalyst/MySQL-YouTube-Series/blob/main/layoffs.csv

--  *Importing Data
--   1. Creating New Schema
--   2. Then import the table using 'Table Data Import Wizard' named 'layoffs'

-- Standard routine for DATA CLEANING
-- Step 0 Create a Staging
-- Step 1 Remove Duplicates
-- Step 2 Standardize the Data
-- Step 3 Null or blank values
-- Step 4 Remove Any Columns (sometimes you should not do this)



-- Step 0
-- First to make sure, we might not want to directly interact with the raw data
-- It's better to create STAGING TABLE for us to interact with the data

-- CREATE A TABLE COPYING ALL THE COLUMNS FROM THE RAW DATA
CREATE TABLE layoffs_staging
LIKE world_layoff.layoffs;

-- CHECK TO SEE THE RAW DATA
SELECT *
FROM layoffs;

-- CHECK TO SEE IF THE TABLE IS CREATED, AND IF THE COLUMNS IS CORRECT
SELECT *
FROM layoffs_staging;

-- INSERT THE DATA FROM THE RAW TABLE TO THE STAGING TABLE
INSERT layoffs_staging
SELECT *
FROM layoffs;




-- Step 1
-- Look for duplicates which is exactly the same, and we need to get rid of it
-- To make sure the data will be accurate in future use

-- FIRST, WE ASSIGNED A ROW NUMBER TO EACH ROW. AS WE USE THE WINDOWS FUNCTION 'OVER,PARTITION BY' 
-- IT WILL GROUP THE VALUES THAT ARE THE SAME AND IT WILL REFLECT IN THE ROW NUMBER 
SELECT *, 
ROW_NUMBER() 
OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- BY MAKING SURE THE QUERIES WORK CORRECTLY, 
-- WE CREATE A CTE TABLE WHICH LET US EASIER TO INTERACT WITH THE TALBE AND GET RID OF THE DUPLICATES
-- AFTER CREATING A CTE TABLE, WE CHECK IF THERE'S ANY ROW_NUM IS > 1 WITH THE WHERE CLAUSE TO DEFINE IF THERE'S A DUPLICATE DATA
WITH duplicate_CTE AS
(
SELECT *, 
ROW_NUMBER() 
OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) 
AS row_num 
FROM layoffs_staging
)
SELECT *
FROM duplicate_CTE
WHERE row_num >1;

-- WE FOUND 5 ROWS THAT RETURN A VALUE OF 2 IN THE ROW_NUM COLUMN
-- SO WE DECIDED TO TAKE A CLOSER LOOK, TO SEE IF THE DATA IS REALLY HAVING A DUPLICATE IN IT
-- WE CHOSE THE COMPANY 'CASPER'
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- AFTER TAKING A CLOSER LOOK IN 'CASPER', WE FIGURE OUT THERE MIGHT BE THE SAME COMPANY LAYING OFF IN DIFFERENT DATE
-- WHICH MADE SOME OF THE DATA HAPPENS TO BE NOT A DUPLICATE DATA
-- SO WE DECIDED TO MODIFY OUR QUERIES TO MAKE SURE WE PARITION BY EVERYTHING IN THE COLUMNS
-- AND DELETE IT USING A SUBQUERIES
WITH duplicate_CTE AS
(
SELECT *, 
ROW_NUMBER() 
OVER(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

----------------------------------------------------------------------------------------
 
-- AND THIS IS ANOTHER SOLUTION OF GETTING RID OF THE DUPLICATE USING ANOTHER METHOD
-- THIS TIME WE CREATED ANOTHER STAGING TABLE WITH COPYING layoffs_staging and CREATING STATEMENT
-- WE ALSO ADDED A COLUMN NAMED `row_num` AND GIVEN THE COLUMN A INTERGER DATA TYPE FOR US TO FILTER THE DUPLICATES LATER ON
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- WE THEN CHECKED IF THE TABLE IS CREATED SUCESSFULLY
SELECT *
FROM layoffs_staging2;

-- AFTER MAKING SURE THE TABLE IS READY, 
-- WE INSERT THE DATA FROM THE RAW DATA TABLE AND GIVING IT A ROW NUM USING 'OVER,PARTITION BY'
INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() 
OVER(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs;

-- WE CHECK ONCE AGAIN TO SEE IF OUR PREVIOUS QUERIES ACT CORRECTLY BY DOUBLE CHECKING IF DATA WE GET IS SAME AS THE ONE WE GOT FROM SOLUTION ONE
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- AFTER MAKING SURE EVERYTHING IS ALRIGHT, WE DELETED ALL THE DUPLICATES DATA WHICH THEIR RESPECTIVE ROW_NUM EQUALS TO 2 OR GREATER
DELETE
FROM layoffs_staging2
WHERE row_num >= 2;



-- Step 2 Standardizing data

-- AS WE ALREADY SAW THERE'S SOME PROBLEM IN THE FIRST FEW COMPANIES NAME
-- WE DECIDED TO FIX IT FIRST BY USING TRIM TO GET RID OF THE SPACE IN FRONT OF THE COMPANY NAME
-- WE FIRST TEST THE QUERIES AND SEE IF IT WORKS
SELECT company, (TRIM(company))
FROM layoffs_staging2;

-- THEN WE UPDATE THE TABLE WITH UPDATE COMMANDS
UPDATE layoffs_staging2
SET company = TRIM(company);

----------------------------------------------------------------------------------------

-- AFTER THAT WE DECIDED TO LOOK INTO THE INDUSTRY COLUMN, 
-- AND ORDER IT PROPERLY TO MAKE SURE WE CAN LOOK IT CLEARLY
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- WE DISCOVERED THERE ARE BOTH 'CRYPTO' AND 'CRYPTO CURRENCY' IN THE INDUSTRY COLUMN WHICH SHOULD BE THE SAME THING
-- WE UPDATED THE TABLE ONCE AGAIN SETTING EVERYTHING THAT STARTS WITH 'CRYPTO' IN THE INDUSTRY INTO JUST 'CRYPTO'
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

----------------------------------------------------------------------------------------

-- WE THEN LOOKED INTO THE COUNTRY COLUMN, AND FOUND THERE WERE TWO 'UNITED STATES' BUT ONE WITH A '.' AT THE END OF IT
-- THEN WE USED TRIM AGAIN, BUT THIS TIME WITH TRAILING TO SPECIFY WE ARE GETTING RID OF THE '.' AT THE END OF THE VALUE
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- AFTER TESTING WE UPDATE THE TABLE
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

----------------------------------------------------------------------------------------

-- THEN WE TOOK A CLOSER LOOK INTO DATE
SELECT `date`
FROM layoffs_staging2;

-- WE FOUND OUT THE DATE COLUMN IS NOT STORING THE DATA IN DATE FORMAT, BUT TEXT FORMAT
-- SO WE FIRST CHANGE THE STRING BACK INTO THE DATE with STR_TO DATE
UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

-- AND THEN CHANGE THE STORING TYPE BACK TO DATE
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- STEP 3 NULL or blank values

-- WE USE A WHERE CLAUSE TO CHECK WHICH OF THOSE DATA ARE NULL AND BLANK
SELECT *
FROM layoffs_staging2
WHERE industry is NULL
OR industry = '';

-- THEN WE TOOK A CLOSER LOOK INTO 'AIRBNB', 
-- AND IT HAPPENS TO BE ANOTHER AIRBNB IN ANOTHER ROW THAT HAVE THE SAME LOCATION AS THE ONE WHO HAS A BLANK VALUE IN THE INDUSTRY COLUMN
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- SO WE TRY TO POPULARIZE THE BLANK IF THERE'S ANY OTHER ROW THAT HAVE THE SAME COMPANY AND THE INDUSTRY HAS AN ACTUAL VALUE IN IT
-- WE TRY TO USE SELF JOIN TO LET THE ONE WITH 'NULL' OR BLANK VALUE TAKING OVER BY THE ONE WHICH ACTUALLY HAS VALUE IN IT, IN THIS CASE IS 'TRAVEL' FOR AIRBNB
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- BUT THE QUERIES DID NOT HAPPEN CORRECTLY AND THE PROBLEM IS NOT SOLVED
-- SO WE DECIDED TO CHANGE ALL THE BLANK VALUE INTO NULL TO MAKE THINGS EASIER
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- THEN WE USE THE SAME QUERIES AGAIN BUT THIS TIME ONLY NULL, INSTEAD OF NULL AND BLANK
-- AND IT WORKS PERFECTLY
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- WE CHECK IF THERE'S ANYTHING LEFT, AND 'Bally's Interactive' IS THE ONLY DATA THAT REMAINS NULL IN THE INDUSTRY COLUMN
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

-- SO WE TRIED TO FIND IF THERE'S ANY OTHER COMPANY THAT NAMED BALLY TO SEE IF WE CAN FIX THE PROBLEM, BUT IT HAPPENS TO BE NO.
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_staging2;



-- STEP 4 Remove Columns and Rows

-- WE TOOK A CLOSER LOOK INTO 'TOTAL_LAID_OFF' AND 'PERCENTAGE_LAID_OFF' AS THERE HAS THE MOST OF NULLS
-- AND AS THESE TWO COLUMNS IS SHARING SOMEWHAT A SAME KIND OF DATA, IF THEY ARE BOTH NULL, IT CANNOT HELP US IN THE NEXT STAGE OF DATA ANALYSING
-- WHICH MEANS WE BETTER DELETE THEM AND MAKE OUR TABLE GOOD AND CLEAN 
 
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- SINCE THESE TWO COLUMNS IS SHARING SOMEWHAT A SAME KIND OF DATA, IF THEY ARE BOTH NULL, IT CANNOT HELP US IN THE NEXT STAGE OF DATA ANALYSING
-- WHICH MEANS WE BETTER DELETE THEM AND MAKE OUR TABLE GOOD AND CLEAN 
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- CHECK ONCE MORE TO SEE IF THERE'S ANY CHANGES WE WANT TO MAKE
SELECT *
FROM layoffs_staging2;

-- DELETE DATA THAT WE CANNOT REALLY USE IN THE FUTURE STAGES
ALTER TABLE layoffs_staging2
DROP column row_num;

-- FINAL CHECK
SELECT *
FROM layoffs_staging2;
