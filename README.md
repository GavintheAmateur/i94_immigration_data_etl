# I94 Immigration Data ETL  

## Summary  
This project is to clean the i94 form data from the US National Tourism and Trade Office for future analysis. The data is in sas7bdat format, and partitioned by month.  Below is a data dictionary for this dataset:  

## Project Structure:
output/: the directory for output files.
dl.cfg: config file. need to add AWS credentials there to connect to AWS  
etl.py: the script to process data  
I94_SAS_Labels_Descriptions.SAS: description file for the dataset, containing data dictionary for some fields.  
immigration_data_sample.csv: a sample of the dataset in 1000 rows.  

## Data Dictionary   
- I94YR - 4 digit year
- I94MON - Numeric month
- I94CIT & I94RES - This format shows all the valid and invalid codes for processing
- I94PORT - This format shows all the valid and invalid codes for processing
- ARRDATE is the Arrival Date in the USA. It is a SAS date numeric field that a 
   permament format has not been applied.  Please apply whichever date format 
   works for you.
- I94MODE - There are missing values as well as not reported (9)
- I94ADDR - There is lots of invalid codes in this variable and the list below 
   shows what we have found to be valid, everything else goes into 'other'
- DEPDATE is the Departure Date from the USA. It is a SAS date numeric field that 
   a permament format has not been applied.  Please apply whichever date format 
   works for you.
- I94BIR - Age of Respondent in Years
- I94VISA - Visa codes collapsed into three categories
- COUNT - Used for summary statistics
- DTADFILE - Character Date Field - Date added to I-94 Files - CIC does not use
- VISAPOST - Department of State where where Visa was issued - CIC does not use
- OCCUP - Occupation that will be performed in U.S. - CIC does not use
- ENTDEPA - Arrival Flag - admitted or paroled into the U.S. - CIC does not use
- ENTDEPD - Departure Flag - Departed, lost I-94 or is deceased - CIC does not use
- ENTDEPU - Update Flag - Either apprehended, overstayed, adjusted to perm residence - CIC does not use
- MATFLAG - Match flag - Match of arrival and departure records
- BIRYEAR - 4 digit year of birth
- DTADDTO - Character Date Field - Date to which admitted to U.S. (allowed to stay until) - CIC does not use
- GENDER - Non-immigrant sex
- INSNUM - INS number
- AIRLINE - Airline used to arrive in U.S.
- ADMNUM - Admission Number
- FLTNO - Flight number of Airline used to arrive in U.S.
- VISATYPE - Class of admission legally admitting the non-immigrant to temporarily stay in U.S.


## Addressing Other Scenarios  
The write up describes a logical approach to this project under the following scenarios:

#### The data was increased by 100x.
Spark is distributive and s3 storage is elastic, so we can add more nodes and storage to process it.  

#### The data populates a dashboard that must be updated on a daily basis by 7am every day.
Data pipeline tools such as Airflow need to be introduced in this case. I didn't use airflow here because the data comes in monthly and we can handle it by running the script manually monthly.  

#### The database needed to be accessed by 100+ people.
I don't think S3 has a limit on visitor amount, so this won't be a prolem.

## Defending Decisions

#### Clearly state the rationale for the choice of tools and technologies for the project. 
I used pandas to process the small descriptive data as pandas is light and neat;  
I used spark to process the large immigration dataset and it's born for big data and it allows reading from and writing to multiple sources;  
I didn't use airflow as the data is updated monthly, and I assume the dataset is more likely for professionals to analysis, not for real time public display, so we can run it manually with one command when it's needed.   
I didn't use redshift as it's expensive. If there's no need to access the cleaned data in real time, we can simply store the results in cheap S3 buckets.  

#### Propose how often the data should be updated and why.
The immigration data comes in monthly, so we can process it monthly.  
