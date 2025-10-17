print("Running test module")

from concordance_lib import create_spine_conc
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("testSession").getOrCreate()

print("Test finished")
