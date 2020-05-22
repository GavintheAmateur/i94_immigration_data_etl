import configparser
import datetime as dt
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import datetime as dt

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        :returns a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def process_desciption_data(desc_file, output_path):
    # adapted from: https://knowledge.udacity.com/questions/125439
    with open(desc_file) as f:
        f_content = f.read()
    f_content = f_content.replace('\t', '')

    def code_mapper(file, idx):
        f_content2 = f_content[f_content.index(idx):]
        f_content2 = f_content2[:f_content2.index(';')].split('\n')
        f_content2 = [i.replace("'", "") for i in f_content2]
        dic = [i.split('=') for i in f_content2[1:]]
        dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
        return dic

    # only save countries,ports and states to csv files independently;

    dict_countries = code_mapper(f_content, "i94cntyl")
    df = pd.DataFrame(list(dict_countries.items()), columns=['code', 'country'])
    df.to_csv(output_path + 'country_codes.csv',index=False)
    dict_ports = code_mapper(f_content, "i94prtl")
    df = pd.DataFrame(list(dict_ports.items()), columns=['code', 'port'])
    df.to_csv(output_path + 'port_codes.csv',index=False)
    dict_states = code_mapper(f_content, "i94addrl")
    df = pd.DataFrame(list(dict_states.items()), columns=['code', 'state'])
    df.to_csv(output_path + 'states_codes.csv',index=False)

    # map visa and mode code to text directly later when processing i94 data since their texts are short
    # and the numeric codes are not meaningful themselves.
    # dict_visa_categories = {'1': 'Business','2': 'Pleasure','3': 'Student'}
    # dict_modes = {'1': 'Air','2': 'Sea','3': 'Land','4': 'Not reported'}


def process_immigration_data(spark, sas_file, output_path):
    """
    :param spark: spark session
    :param sas_file: sas file to process
    :param output_path: path to save output to
    :return: None
    """
    # 1. read data
    df_immi = spark.read.format('com.github.saurfang.sas.spark').load(sas_file)

    # 2. cleanse data

    # 2.a replace mode and visa code with text
    dict_modes = {1: 'Air', 2: 'Sea', 3: 'Land', 4: 'Not reported'}
    convert_mode = udf(lambda x: dict_modes.get(int(x)) if x in dict_modes.keys() else None)
    df_immi = df_immi.withColumn("arrival_mode", convert_mode(df_immi.i94mode))

    dict_visa_categories = {1: 'Business', 2: 'Pleasure', 3: 'Student'}
    convert_visa = udf(lambda x: dict_visa_categories.get(int(x)) if x in dict_visa_categories.keys() else None)
    df_immi = df_immi.withColumn("visa_category", convert_visa(df_immi.i94visa))

    # 2.b convert sas date to normal date
    convert_sas_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    df_immi = df_immi.withColumn("arrival_date", convert_sas_date(df_immi.arrdate))
    df_immi = df_immi.withColumn("departure_date", convert_sas_date(df_immi.depdate))

    # 2.c convert other columns in proper type in sql.
    df_immi.createOrReplaceTempView("staging_immigration")
    df_immi_clean = spark.sql("""
                                        select 
                                                cast(i.i94yr as int) as year,
                                                cast(i.i94mon as int) as month,
                                                cast(i.i94cit as int) as birth_country_code,
                                                cast(i.i94res as int) as residence_country_code,
                                                i.i94port as port_code,
                                                i.arrival_date,
                                                i.arrival_mode,
                                                i.i94addr as state_code,
                                                i.departure_date,
                                                cast(i.i94bir as int) as age,
                                                i.visa_category,
                                                cast(i.count as int) as count,
                                                i.dtadfile as date_added,
                                                i.visapost as visa_issuing_post,
                                                i.occup as occupation,
                                                i.entdepa as arrival_flag,
                                                i.entdepd as departure_flag,
                                                i.entdepu as update_flag,
                                                i.matflag as match_arrival_departure_fag,
                                                cast (i.biryear as int) as birth_year,
                                                i.dtaddto as allowed_date,
                                                i.insnum as ins_number,
                                                i.airline as airline,
                                                cast (i.admnum as int) as admission_number,
                                                i.fltno as flight_number,
                                                i.visatype as visa_type
                                            from staging_immigration i
                                        """)
    df_immi_clean.show()

    # 3. save data to parquet file
    df_immi_clean.write.mode("overwrite").partitionBy("year", "month").parquet(output_path + 'immigrations')


def main():
    spark = create_spark_session()

    input_path = "../../"
    output_path = "./output/"

    desc_file = input_path + 'data/I94_SAS_Labels_Descriptions.SAS'
    immigration_file = input_path + 'data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    process_desciption_data(desc_file, output_path)
    process_immigration_data(spark, immigration_file, output_path)


if __name__ == "__main__":
    main()
