package_version = "2.0.0"

def version_check(compareversion:str) -> bool:
    """Return a bool of a version compared to the package one"""
    return compareversion == package_version
