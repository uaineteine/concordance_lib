from .pathing import get_spine_path

import os

class SpineProductVersion:
    def __init__(self, spine_version:int, spine_revision:int = -1):
        self.spine_version = spine_version
        
        if spine_revision == -1: #use latest
            self.latest = True
        else:
            self.latest = False
        self.spine_revision = spine_revision

class SpineProduct:
    def __init__(self, asset_name, spine_version:SpineProductVersion):
        self.spine_version = spine_version
        self.asset_name = asset_name
        
    def get_spine_product_name(self) -> str:
        def_str = "f{asset_name}_spine_v{spine_version}_{spine_revision}"
        
        def_str = os.environ.get("CONCLIB_PRD_NAME", def_str)
        spine_ver = def_str.format(asset_name=self.asset_name, spine_version=self.spine_version.spine_version, spine_revision=self.spine_version.spine_revision)
        
        return spine_ver
    
    def get_spine_path(self) -> str:
        fname = f"{self.get_spine_product_name()}.parquet"
        
        return f"{get_spine_path()}/{fname}"
    