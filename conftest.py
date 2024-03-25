import pytest
from lib.utilities import createspark_session

@pytest.fixture
def spark():
    "Creates spark session"
    spark_session = createspark_session("LOCAL")
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_result(spark):
    schema = "month string,each_month_total float"
    return spark.read\
    .format("csv")\
    .schema(schema)\
    .load("data/test/expected.csv")