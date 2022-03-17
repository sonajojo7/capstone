# Data Engineering Capstone Project
## Project Summary

The objective of this project was to make analysis on the immigration events based on some datasets obtained. An ETL was created for I-94 immigration, global land temperature and US demographics datasets. This was used to get an idea of immigration patterns to US. 



## Data and Code

All the data for this project were loaded into S3 as well as to the project workspace. 

In addition to the data files, the project workspace included program files :

1. *etl.py* reads data from S3, processes that data using Spark and writes them back to S3
2. *functions.py* and *utility.py* - these modules contains the functions for creating fact and dimension tables and cleaning.
3. *config.cfg* - contains configuration that allows the ETL pipeline to access AWS EMR cluster.
4. *Exploratory analysis.ipynb* - jupyter notebook that was used for exploring and assessing the datasets
5. *Capstone Project.ipynb* - jupyter notebook that was used for building the ETL pipeline.

## The project follows the following steps:

- Step 1: Scope the Project and Gather Data
- Step 2: Explore and Assess the Data
- Step 3: Define the Data Model
- Step 4: Run ETL to Model the Data
- Step 5: Complete Project Write Up

## Step 1: Scope the Project and Gather Data

### Project Scope
To create the analytics database, the following steps will be carried out:

- Use Spark to load the data into dataframes.
- Exploratory data analysis of I94 immigration dataset, demographics dataset and global land temperatures by city dataset  to identify missing values     and strategies for data cleaning.
- Perform data cleaning functions on all the datasets.
- Create dimension tables.
- Create immigration calendar dimension table from I94 immigration dataset, this table links to the fact table through the arrival date field.
- Create country dimension table from the I94 immigration and the global temperatures dataset. The global land temperatures data was aggregated at        country level. The table links to the fact table through the country of residence code allowing analysts to understand correlation between country of   residence climate and immigration to US states.
- Create usa demographics dimension table from the us cities demographics data. This table links to the fact table through the state code field.
- Create fact table from the clean I94 immigration dataset and the visa_type dimension.
- The technology used in this project is Amazon S3, Apache Sparke. Data will be read and staged from the customers repository using Spark.

## Step 2: Explore and Assess the Data
 
 Exploratory analysis.ipynb is the Jupyter notebook where datasets were loaded and explored to find missing values and duplicates and strategize on how to create the ETL
 
## Step 3: Define the Data Model


<img src="Data model.png">

### I. Conceptual Data Model

### Database schema

- The country dimension table is made up of data from the global land temperatures by city and the immigration datasets. The combination of these two  datasets allows analysts to study correlations between global land temperatures and immigration patterns to the US.

- The us demographics dimension table comes from the demographics dataset and links to the immigration fact table at US state level. This dimension would allow analysts to get insights into migration patterns into the US based on demographics as well as overall population of states.It gives an insight on which states most immigrants are drawn towards

- The immigration calendar dimension table formed from the immigration datasets points towards arrival time of the immigrants

- The visa type dimension table comes from the immigration datasets and links to the immigaration making use of the visa_type_key.

- The immigration fact table is the fact table of the data model. This table's data comes from the immigration data sets and contains keys that links to the dimension tables. 

### II. Mapping Out Data Pipelines 

 **The pipeline steps are as follows:**

- Run functions.py and utility.py in the console to enable the functions for cleaning the datasets and creating the tables
- Load the datasets
- Cleaning the I94 Immigration data to create Spark dataframe for each month
- Create visa_type dimension table
- Create calendar dimension table
- Extract clean global temperatures data
- Create country dimension table
- Create immigration fact table
- Load demographics data
- Clean demographics data
- Create demographic dimension table

## Step 4: Run Pipelines to Model the Data

### I. Create the data model

Data dictionary is provided in the jupyter notebook.

### II. Running the ETL pipeline
 
 The ETL pipeline is defined in the etl.py script, and this script uses the utility.py and functions.py modules to create a pipeline that creates final tables which are stored in parquet form.

