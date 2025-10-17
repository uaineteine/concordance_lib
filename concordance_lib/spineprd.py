from .pathing import get_spine_path

class SpineVersion:
    def __init__(self, spine_version:int, spine_revision:int = -1):
        self.spine_version = spine_version
        
        if spine_revision == -1: #use latest
            self.latest = True
        else:
            self.latest = False
        self.spine_revision = spine_revision

class SpineProduct:
    def __init__(self, asset_name, spine_version:SpineVersion):
        self.spine_version = spine_version
        self.asset_name = asset_name
        
    def get_spine_product_name(self) -> str:
        return f"{self.asset_name}_spine_v{self.spine_version.spine_version}_{self.spine_version.spine_revision}.parquet"
    
    def get_spine_path(self) -> str:
        fname = self.get_spine_product_name()
        
        return f"{get_spine_path()}/{fname}"
    