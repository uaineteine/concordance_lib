import numpy as np

class LinkageProjects:

    @staticmethod
    def get_linkage_projects_in_range(starting_project, end_project):
        """Create an array of IDs by a number range"""
        ar = np.arange(starting_project, end_project + 1)
        return ar.tolist()
