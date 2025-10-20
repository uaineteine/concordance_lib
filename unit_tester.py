print("Running test module")

from conclib import create_spine_conc, SpineProduct, SpineProductVersion, LinkageProjects
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("testSession").getOrCreate()

#make spine product
prd_info = SpineProduct("air", SpineProductVersion(8, 1))
prjids = LinkageProjects.get_linkage_projects_in_range(1, 20)
prod = create_spine_conc(1, prjids, prd_info, spark)

print("Test finished")
