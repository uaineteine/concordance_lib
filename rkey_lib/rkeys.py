from pathing import get_path

def get_rkey_path() -> str:
    """Return specifically the rkey path in the system"""
    
    path = get_path()
    
    return f"{path}/rkeys"
