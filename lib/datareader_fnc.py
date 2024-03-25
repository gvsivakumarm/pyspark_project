from lib.confreader_fnc import get_app_conf
from lib.utilities import createspark_session

def data_reader_jan(spark,env):
    functioncaller = get_app_conf(env)
    jan = functioncaller["greentaxi.jan22"]
    return spark.read\
    .format("parquet")\
    .load(jan)

def data_reader_feb(spark,env):
    functioncaller = get_app_conf(env)
    jan = functioncaller["greentaxi.feb22"]
    return spark.read\
    .format("parquet")\
    .load(jan)
