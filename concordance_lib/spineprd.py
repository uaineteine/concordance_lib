class spineVersion:
    def __init__(self, spine_version:int, spine_revision:int = -1):
        self.spine_version = spine_version
        
        if spine_revision == -1: #use latest
            self.latest = True
        else:
            self.latest = False
        self.spine_revision = spine_revision
        
