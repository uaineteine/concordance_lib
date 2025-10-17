from .pathing import *
from .spineprd import SpineProduct

from pyspark.sql import SparkSession, DataFrame

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

