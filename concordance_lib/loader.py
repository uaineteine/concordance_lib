from .pathing import *
from .spineprd import SpineProduct

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import countDistinct

def load_ent_map(id_group_num:int, sparkSession: SparkSession) -> DataFrame:
    path = get_maps_path(id_group_num)
    
    print("Reading from: " + path)
    try:
        df = sparkSession.read.parquet(path)
        if "ARTIFACT_ID" in df.columns:
            df = df.drop("ARTIFACT_ID")
        return df
    except Exception as e:
        print(f"Error reading from: {path}")
        return None
    
def _load_projects(id_group_number: int, sparkSession: SparkSession) -> DataFrame:
    proj_dir = get_linkage_path()
    path_proj = f"{proj_dir}/link_projects_{id_group_number}.parquet"
    
    print("Reading from: " + path_proj)
    try:
        df_proj = sparkSession.read.parquet(path_proj)
        df_proj = df_proj.select("PROJECT_ID", "SPINE_VERSION_ID")
    except Exception as e:
        print(f"Error reading from: {path_proj}")
        df_proj = None
    
    return df_proj

def _load_results(id_group_number: int, sparkSession: SparkSession) -> DataFrame:
    res_dir = get_linkage_path()
    path_res = f"{res_dir}/link_results_{id_group_number}.parquet"

    print("Reading from: " + path_res)
    try:
        df_res = sparkSession.read.parquet(path_res)
        df_res = df_res.select("PROJECT_ID", "SPINE_VERSION_ID")
    except Exception as e:
        print(f"Error reading from: {path_res}")
        df_res = None

    return df_res

def load_linkage_results(id_group_number: int, sparkSession: SparkSession) -> DataFrame:
    
    df_proj = _load_projects(id_group_number, sparkSession)
    df = _load_results(id_group_number, sparkSession)

    # Optionally join the two if both were read successfully
    if df is not None and df_proj is not None:
        df = df.join(df_proj, on="PROJECT_ID", how="right")

    return df

def load_spine_product(spine_prd:SpineProduct, sparkSession:SparkSession) -> DataFrame:
    path = spine_prd.get_spine_path()
    
    return sparkSession.read.parquet(path)

def create_spine_conc(idGroup:int, projectIDs: list[int], spine_prd:SpineProduct, sparkSession:SparkSession) -> DataFrame:
    proj_df = load_linkage_results(idGroup, sparkSession)
    
    #filter for targetted project IDs
    proj_df = proj_df.filter(proj_df["PROJECT_ID"].isin(projectIDs))
    
    #filter for spine version ID
    proj_df = proj_df.filter(proj_df["SPINE_VERSION_ID"] == spine_prd.spine_version.spine_version)
    
    #rename the linkage projects columns
    proj_df = proj_df.withColumnRenamed("FROM_SYN_AEUID", "SYNHETIC_AEUID")\
                     .withColumnRenamed("TO_SPINE_ID", "SPINE_ID")
    proj_df = proj_df.select("SYNTHETIC_AEUID", "SPINE_ID")
    
    #load an existing spine product
    spine_df = load_spine_product(spine_prd, sparkSession)
    spine_df = spine_df.select("SYNTHETIC_AEUID", "SPINE_ID")
    
    #append these in union
    combined_df = spine_df.unionByName(proj_df).dropDuplicates()
    
    multi_spineid = combined_df.groupBy("SYNTHETIC_AEUID").agg(countDistinct("SPINE_ID").alias("spine_id_linked_count"))
    n_violations = multi_spineid.filter("spine_id_linked_count>1").count()
    
    if n_violations > 0:
        raise ValueError(f"Found {n_violations} violations of spine ID linkage.")
    
    return combined_df
    