from pyspark.sql import SparkSession
from pyspark.sql.types import NullType
import os
import datetime
import argparse
import logging
import csv
import pandas as pd


def MakeASpark():
    spark = SparkSession.\
                 builder.\
                 appName("Transform_stock_data").\
                 config('spark.sql.session.timeZone', 'Asia/Seoul').\
                 getOrCreate()
    return spark

def ProcessingData(spark, input_file, output_file, agg_output_path):
    readcsv = spark.read.csv(input_file, inferSchema=True, header=True)
    readcsv.createOrReplaceTempView('stock_dataset')

    logdf = spark.sql("""
        SELECT CASE WHEN COUNT(1) > 0 THEN COUNT(1)
                    ELSE 0
                END AS CNT
          FROM stock_dataset
    """)
    rows = logdf.first()[0]
    if rows == 0:
        raise Exception("There is no row inserted")

    dfagg = spark.sql("""
                    SELECT basDt,
                           sum(clpr) as clpr,
                           sum(mkp) as mkp,
                           sum(hipr) as hipr,
                           sum(lopr) as lopr,
                           current_timestamp() as update,
                           '' as modified
                      FROM stock_dataset
                     GROUP BY mrktCtg, basDt
                """)

    # 2. BigQuery 데이터 추출
    df = spark.sql("""
                    SELECT  itmsNm,
                            mrktCtg,
                            clpr,
                            vs,
                            fltRt,
                            mkp,
                            hipr,
                            lopr,
                            trqu,
                            trPrc,
                            lstgStCnt,
                            mrktTotAmt,
                            current_timestamp() as update,
                            '' as modified,
                            to_date(cast(basDt as string), 'yyyyMMdd') as basDt
                      FROM stock_dataset """)
    
    
    dfagg\
    .coalesce(1)\
    .write\
    .option('header', True)\
    .format("csv")\
    .mode("overwrite")\
    .save(agg_output_path)

    df\
    .coalesce(1)\
    .write\
    .option('header', True)\
    .format("csv")\
    .mode("overwrite")\
    .save(output_file)

    return rows

def LoggingData(input_path, rows, today):
    date = datetime.datetime.strptime(today, '%Y%m%d')
    year = date.year
    path = os.path.join(input_path, f'log_{year}.csv')
    row = pd.DataFrame({'year' : [year], 'num_of_rows' : [rows]})
    try:
        tmpdf = pd.read_csv(path)
        df = pd.concat([tmpdf, row], axis = 1)
        df.to_csv(path)
    except:
        df = row
        df.to_csv(path)
    logging.info(f'The number of rows : {rows}')

def main():
    parser = argparse.ArgumentParser()
    # -- : Optional, (non) -- : required
    parser.add_argument("input_folder_path", type=str, help="Directory where csv files stored")
    parser.add_argument("output_folder_path", type=str, help="Directory where parquet files being saved")
    parser.add_argument("log_path", type=str, help="Directory where parquet files being saved")
    parser.add_argument("input_file", type = str, help = "File name what should be processed")
    parser.add_argument("output_file", type = str, help = "File name what after processing")
    parser.add_argument("today", type=str, help="Where is a date being triggered", default=datetime.datetime.today().strftime('%Y%m%d'))
    args = parser.parse_args()

    input_folder_path = args.input_folder_path
    output_folder_path = args.output_folder_path
    log_path = args.log_path
    input_file = args.input_file
    output_file = args.output_file
    today = args.today
    input_file = input_file.format(today)
    output_file = output_file.format(today)
    input_path = os.path.join(input_folder_path, input_file)
    output_path = os.path.join(output_folder_path, output_file)
    agg_output_path = os.path.join(output_folder_path, 'agg_'+output_file)

    spark = MakeASpark()
    rows = ProcessingData(spark, input_path, output_path, agg_output_path)
    LoggingData(log_path, rows, today)

if __name__ == '__main__':
    main()
