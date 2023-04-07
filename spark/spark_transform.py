import os
import datetime
import argparse
import logging
import pandas as pd
from pyspark.sql import SparkSession


def MakeASpark():
    spark = SparkSession.\
                 builder.\
                 appName("Transform_stock_data").\
                 config('spark.sql.session.timeZone', 'Asia/Seoul').\
                 getOrCreate()
    return spark

def ProcessingData(spark, folder_path, raw_stock_csv, folder_transformed, folder_aggregated):
    readcsv = spark.read.csv(os.path.join(folder_path,\
                                          raw_stock_csv)\
                            , inferSchema=True\
                            , header=True)
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
#    .coalesce(1)\ 확인차 테스트 용. 
    .write\
    .option('header', True)\
    .format("csv")\
    .mode("overwrite")\
    .save(os.path.join(folder_path, folder_aggregated))

    df\
#    .coalesce(1)\
    .write\
    .option('header', True)\
    .format("csv")\
    .mode("overwrite")\
    .save(os.path.join(folder_path, folder_transformed))
    return rows

def LoggingData(log_path, rows):
    today = datetime.datetime.today()
    date = datetime.datetime.strptime(today, '%Y%m%d')
    year = date.year
    path = os.path.join(log_path, f'log_{year}.csv')
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
    parser.add_argument("folder_path", type=str, help="Directory where csv files stored")
    parser.add_argument("raw_stock_csv", type=str, help="Directory where csv files stored")
    parser.add_argument("folder_transformed", type=str, help="Directory where csv files stored")
    parser.add_argument("folder_aggregated", type=str, help="Directory where csv files stored")
    parser.add_argument("--log_path", type=str, help="Directory where csv files stored", default=None)
    parser.add_argument("--mode", type=str, help="Directory where parquet files being saved", default = 'only_trans')

    args = parser.parse_args()
    mode = args.mode
    folder_path = args.folder_path
    raw_stock_csv = args.raw_stock_csv
    folder_transformed = args.folder_transformed
    folder_aggregated = args.folder_aggregated
    log_path = args.log_path
    if log_path == None:
        paths = folder_path.split('/')
        paths = paths[:-1] + ['logfiles']
        log_path = '/'.join(paths)

    spark = MakeASpark()
    rows = ProcessingData(spark, folder_path,\
                          raw_stock_csv, folder_transformed,\
                          folder_aggregated)
    if mode == 'add_log':
        LoggingData(log_path, rows)

if __name__ == '__main__':
    main()
