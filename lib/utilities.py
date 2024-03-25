from lib.confreader_fnc import get_pyspark_conf
from pyspark.sql import SparkSession

def createspark_session(env):
    return SparkSession.builder\
    .config(conf = get_pyspark_conf(env))\
    .master("local[2]")\
    .getOrCreate()