from lib.datareader_fnc import *
from lib.confreader_fnc import get_app_conf
from lib.datamanupilication_fnc import *
from lib.datareader_fnc import *
import pytest
from pyspark.sql import *

@pytest.mark.reader()
def test_datareader(spark):
    jan_count = data_reader_jan(spark,"LOCAL").count()
    assert jan_count == 62495

@pytest.mark.reader()
def test_datareader2(spark):
    feb_count = data_reader_feb(spark,"LOCAL").count()
    assert feb_count == 69399

@pytest.mark.configs()
def test_appconfreader():
    config = get_app_conf("LOCAL")
    assert config["greentaxi.jan22"] == "data/greentaxi_jan22.parquet"

@pytest.mark.transformations()
def test_function(spark,expected_result):
    test_df = data_reader_jan(spark,"LOCAL")
    test_filter = filters(test_df)
    final_tested = farecollected(test_filter)
    assert final_tested.collect() == expected_result.collect()



@pytest.mark.parametrize(
        "types,count",
        [(1,9405),
        (2,53090)]
)
def test_onspecific_samefnc(spark,types,count):
    df = data_reader_jan(spark,"LOCAL")
    counter = filter(df,types).count()
    assert count == counter
