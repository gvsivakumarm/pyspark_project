from pyspark.sql.functions import *
from pyspark.sql.types import *


def filters(datframe):
    modified_1_green = datframe.drop("ehail_fee","trip_type")
    modified_2_green = modified_1_green.filter(col("lpep_dropoff_datetime").between("2022-01-01","2023-01-01"))
    modified_3_green = modified_2_green.filter((col("total_amount")>0) & (col("total_amount").isNotNull()))
    modified_4_green = modified_3_green.select("lpep_pickup_datetime","lpep_dropoff_datetime","PULocationID","DOLocationID","passenger_count","trip_distance","fare_amount",
                                           "extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount","payment_type","congestion_surcharge",
                                           "RatecodeID","store_and_fwd_flag","VendorID")
    modified_5_green = modified_4_green.withColumn("payment_type", modified_4_green["payment_type"].cast(LongType()))\
                                        .withColumn("Taxi_type",lit("Green_taxi"))\
                                        .withColumn("airport_fee",lit(0.0))\
                                        .withColumnRenamed("lpep_pickup_datetime","tpep_pickup_datetime")\
                                        .withColumnRenamed("lpep_dropoff_datetime","tpep_dropoff_datetime")
    modified_6_green = modified_5_green.withColumn("Month",date_format("tpep_pickup_datetime","MMMM"))\
                                        .withColumn("day_of_week",date_format("tpep_pickup_datetime", "EEEE"))\
                                        .withColumn("time",when(((date_format("tpep_pickup_datetime","HH:MM:SS") > "06:00:00") & (date_format("tpep_pickup_datetime","HH:MM:SS") < "11:59:59")), 'Morning')
                                                            .when(((date_format("tpep_pickup_datetime","HH:MM:SS") > "12:00:00") & (date_format("tpep_pickup_datetime","HH:MM:SS") < "15:59:59")), 'Noon')
                                                            .when(((date_format("tpep_pickup_datetime","HH:MM:SS") > "16:00:00") & (date_format("tpep_pickup_datetime","HH:MM:SS") < "19:59:59")), 'evening')
                                                            .when(((date_format("tpep_pickup_datetime","HH:MM:SS") > "20:00:00") & (date_format("tpep_pickup_datetime","HH:MM:SS") < "23:59:59")), 'Night')
                                                            .when(((date_format("tpep_pickup_datetime","HH:MM:SS") > "00:00:00") & (date_format("tpep_pickup_datetime","HH:MM:SS") < "3:59:59")), 'Mid_night')
                                                            .when(((date_format("tpep_pickup_datetime","HH:MM:SS") > "4:00:00") & (date_format("tpep_pickup_datetime","HH:MM:SS") < "5:59:59")), 'Early_morning')
                                                            .otherwise("D"))
    return modified_6_green
                                

def farecollected(datframe):
    fare = datframe.groupBy("Month").agg(sum("total_amount").alias("each_month_total"))
    return fare

def filter(dataframe,type):
    return dataframe.filter(col("VendorID") == format(type))