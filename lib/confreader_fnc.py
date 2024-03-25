import configparser
from pyspark import SparkConf


def get_app_conf(env):
    conf = configparser.ConfigParser()
    conf.read("configs/application.conf")
    app_confs = {}
    for (key,val) in conf.items(env):
        app_confs[key] = val
    return app_confs

def get_pyspark_conf(env):
    conf = configparser.ConfigParser()
    conf.read("configs/pyspark.conf")
    pyspark_conf = SparkConf()
    for (key,val) in conf.items(env):
        pyspark_conf.set(key,val)
    return pyspark_conf

    
