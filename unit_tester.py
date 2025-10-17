print("Running test module")

from concordance_lib import create_spine_conc, SpineProduct, SpineProductVersion, LinkageProjects
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("testSession").getOrCreate()

#make spine product
ver = SpineProductVersion(8, 1)
prjids = LinkageProjects.get_linkage_projects_in_range(1, 20)
prod = create_spine_conc(1, prjids, SpineProduct("air", ver), spark)

print("Test finished")
