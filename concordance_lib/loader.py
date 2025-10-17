from .pathing import *

from pyspark.sql import SparkSession, DataFrame

def load_linkage_results(id_group_number: int, sparkSession: SparkSession) -> DataFrame:
    projdir = get_linkage_path()

    path_proj = f"{projdir}/link_projects_{id_group_number}.parquet"
    path_res = f"{projdir}/link_results_{id_group_number}.parquet"

    # Read the smaller project file first
    print("Reading from: " + path_proj)
    try:
        df_proj = sparkSession.read.parquet(path_proj)
        df_proj = df_proj.select("PROJECT_ID", "SPINE_VERSION_ID")
    except Exception as e:
        print(f"Error reading from: {path_proj}")
        df_proj = None

    # Now read the larger results file
    print("Reading from: " + path_res)
    try:
        columns_to_drop = ["STATUS_ID", "LINK_FLAG"]
        df = sparkSession.read.parquet(path_res)
        for col in columns_to_drop:
            if col in df.columns:
                df = df.drop(col)
    except Exception as e:
        print("Error reading from: " + path_res)
        df = None

    # Optionally join the two if both were read successfully
    if df is not None and df_proj is not None:
        df = df.join(df_proj, on="PROJECT_ID", how="left")

    return df
