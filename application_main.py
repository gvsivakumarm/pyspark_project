import sys
from lib import utilities,datareader_fnc,datamanupilication_fnc
from pyspark.sql.functions import *
from lib.logger import Log4j


if __name__ == '__main__':
    if len(sys.argv)<2:
        print("please provide env") 
        sys.exit(-1)

    job_run_env = sys.argv[1]

    print("creating spark session")

    spark = utilities.createspark_session(job_run_env)

    logger = Log4j(spark)

    logger.warn("Created Spark Session")

    taxi_jan_22 = datareader_fnc.data_reader_jan(spark,job_run_env)

    taxi_feb_22 = datareader_fnc.data_reader_feb(spark,job_run_env)

    print(taxi_feb_22.count())

    filters_df = datamanupilication_fnc.filters(taxi_feb_22)

    print(filters_df.count())

    farecollected = datamanupilication_fnc.farecollected(filters_df)

    farecollected.show(20)

    logger.info("this is the end of main")

    #testing