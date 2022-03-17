import pandas as pd
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_unixtime, to_timestamp
from pyspark.sql.functions import col, hour, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import monotonically_increasing_id
import utility
import functions

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY_ID'] = config['KEYS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

  

def process_immigration_data(spark, output_data, immigration_file, temperature_file, mapping_file):
    """Process the immigration data input file and creates fact table and calendar, visa_type and country dimension tables.
    Parameters:
    -----------
    spark (SparkSession): spark session instance
    output_data (string): output file path
    file_name (string): immigration input file name
    mapping_file (pandas dataframe): dataframe that maps country codes to country names
    temperature_file (string): global temperatures input file name
    """
    # get the file path to the immigration data
     

    # read immigration data file
    immigration_df = spark.read.format('com.github.saurfang.sas.spark').load(immigration_file)

    # clean immigration spark dataframe
    immigration_df = utility.clean_spark_immigration_data(immigration_df)

    # create visa_type dimension table
    visatype_df = functions.create_visa_type_dimension_table(immigration_df, output_data)

    # create calendar dimension table
    calendar_df = functions.create_immigration_calendar_dimension(immigration_df, output_data)

    # get global temperatures data
    temp_df = process_global_land_temperatures(spark,  temperature_file)

    # create country dimension table
    dim_df = functions.create_country_dimension_table(spark, immigration_df, temp_df, output_data, mapping_file)

    # create immigration fact table
    fact_df = functions.create_immigration_fact_table(spark, immigration_df, output_data)
    
    
def process_demographics_data(spark, output_data, file_name):
    """Process the demographics data and create the usa_demographics_dim table
    Parameters:
    -----------
    spark (SparkSession): spark session instance
    output_data (string): output file path
    file_name (string): usa demographics csv file name"""
    

    # load demographics data
    
    demographics_df = spark.read.csv(file_name, inferSchema=True, header=True, sep=';')

    # clean demographics data
    new_demographics_df = utility.clean_spark_demographics_data(demographics_df)

    # create demographic dimension table
    df = functions.create_demographics_dimension_table(new_demographics_df, output_data)


def process_global_land_temperatures(spark, file_name):
    """Process the global land temperatures data and return a dataframe
    Parameters:
    -----------
    spark (SparkSession): spark session instance
    file_name (string): global land temperatures csv file name
    output_data (string): output file path
    """
    # load data
   
    temperature_df = spark.read.csv(file_name, header=True, inferSchema=True)

    # clean the temperature data
    new_temperature_df = utility.clean_spark_temperature_data(temperature_df)

    return new_temperature_df


def main():
    spark = create_spark_session()
    #input_data = "s3://capstoneprojectdata/"
    #output_data = "s3://capstoneprojectdata/"
    
    output_data = "tables/"

    immigration_file_name = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    temperature_file_name = '../../data2/GlobalLandTemperaturesByCity.csv'
    usa_demographics_file_name = "us-cities-demographics.csv"

    
    # load the i94 to country mapping data
    mapping_file = spark.read.csv('i94.csv',header=True, inferSchema=True)

    process_immigration_data(spark,output_data, immigration_file_name, temperature_file_name, mapping_file)

    process_demographics_data(spark,output_data, usa_demographics_file_name)


if __name__ == "__main__":
    main()



