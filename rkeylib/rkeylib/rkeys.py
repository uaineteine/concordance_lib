from conclib import get_path
from hash_method import method_hash

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

def attach_spine_hash_by_version(
        df: DataFrame,
        sparkSession: SparkSession,
        version: int,
        syn_hash_col: str = "RKEY_SYN_HASH"
) -> DataFrame:
    """
    Attaches the pre-hashed spine hash to the DataFrame by version id.
    If the hash is missing, falls back to the synthetic hash.

    Args:
        df (DataFrame): The DataFrame to attach the spine hash to.
        sparkSession (SparkSession): The Spark session to use.
        version (int): The spine version to attach the hash for.
        syn_hash_col (str, optional): The name of the synthetic hash column. Defaults to "SYNTHETIC_AEUID".
    """
    version_id = f"v{version}"

    spine_id_col = f"SPINE_{version_id.upper()}_ID" #example: SPINE_V4_ID
    output_col = f"RKEY_SPINE{version_id.upper()}" #example: RKEY_SPINE_V4
    # Define the spine hash column name - temporary column for the join
    spine_hash_col = "SPINE_ID_RKEY"

    #implied else where the parquet file exists
    spine_map_df = read_rkey_prehash(version, sparkSession)

    #join on the appropriate spine id column to get the hash
    df = df.join(
        spine_map_df,
        df[spine_id_col] == spine_map_df["SPINE_ID"],
        how="left"
    ).drop("SPINE_ID")

    # Create output column from the hashed value if it exists, otherwise use the synthetic AEUID hash
    df = df.withColumn(
        output_col, coalesce(df[spine_hash_col], df[syn_hash_col])
    )

    # Drop the temporary column from the mapping
    df = df.drop(spine_hash_col)

    return df

def create_rkeys(df:DataFrame, asset_name:str, spine_version_min:int, spine_version_max:int) -> DataFrame:
    """
    Create the rkey spine hashes for the specified spine versions.

    Args:
        df (DataFrame): The DataFrame to create the rkeys for.
        sparkSession (SparkSession): The Spark session to use.
        spine_version_min (int): The minimum spine version to create rkeys for.
        spine_version_max (int): The maximum spine version to create rkeys for. Inclusive.
    Returns:
        DataFrame: The DataFrame with the rkeys attached.
    """
    df = df.withColumn("RKEY_SYN_HASH", df["SYNTHETIC_AEUID"]) #create the synthetic hash rkey column
    rkey_file_syn = f"{asset_name.lower()}_rkey_syn"
    df = method_hash(df, "RKEY_SYN_HASH", "RKEY_SYN_HASH", rkey_file_syn)

    #now attach spine ids
    for v in range(spine_version_min, spine_version_max + 1):
        df = attach_spine_hash_by_version(df, SparkSession, v)

    #drop the synthetic hash column
    df = df.drop("RKEY_SYN_HASH").dropDuplicates()
    print(f"Spine product has {df.count()} rows")

    return df
