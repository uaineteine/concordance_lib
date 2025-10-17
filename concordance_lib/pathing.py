import os

def check_path_variable_set() -> bool:
    """Check if the CONCLIB_PATH environment variable is set"""
    return "CONCLIB_PATH" in os.environ

def get_path():
    """Return the default path for the system"""
    def_path = "concordances"

    path = os.getenv("CONCLIB_PATH", def_path)
    
    return path

def get_maps_path():
    """Return specifically the maps path in the system"""

    path = get_path()
    
    return f"{path}/entity_maps"

def get_spine_path():
    """Return specifically the spine path in the system"""

    path = get_path()

    return f"{path}/spine"

def get_linkage_path():
    """Return specifically the linkage path in the system"""

    path = get_path()

    return f"{path}/linkage"

def get_rkey_path():
    """Return specifically the rkey path in the system"""
    
    path = get_path()
    
    return f"{path}/rkeys"
