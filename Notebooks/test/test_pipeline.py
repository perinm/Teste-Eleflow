import pytest
from main import convert_date_column
from pyspark.sql.types import DateType

pytestmark = pytest.mark.usefixtures("df")


def test_convert_date_column(df):
    assert convert_date_column("DATA INICIAL", df).schema[0].dataType is DateType()