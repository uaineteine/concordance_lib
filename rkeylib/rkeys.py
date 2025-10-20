from conclib import get_path

import 
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import coalesce

def get_rkey_path() -> str:
    """Return specifically the rkey path in the system"""
    
    path = get_path()
    
    return f"{path}/rkeys"

#prehashed files must include:
#RKEY_SPINE_HASH for hash value
def read_rkey_prehash(spine_version:int, sparkSession:SparkSession) -> DataFrame:
    """
    Read in the rkey prehash data for a specific spine version

    Args:
        spine_version (int): The spine version to read the prehash for
    """
    path = f"{get_rkey_path()}/rkey_spine_v{spine_version}.parquet"

    if not dbutils.fs.ls(path):
        print(f"Path does not exist: {path}")
        print(f"Skipping version {spine_version}")
        return None
    
    #imples else
    df = sparkSession.read.parquet(path)

    if "SYNTHETIC_AEUID" in df.columns:
        df = df.drop("SYNTHETIC_AEUID")
    else:
        raise ValueError("Expected column SYNTHETIC_AEUID not found in rkey prehash data. Check the pre-hash data was created correctly.")
    
    return df
