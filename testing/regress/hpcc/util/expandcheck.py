import os

class ExpandCheck:

    @staticmethod
    def dir_exists(path, require=False):
        path = os.path.abspath(path)
        if not os.path.exists(path) and require:
            raise IOError("REQUIRED DIRECTORY NOT FOUND. " + path)

        return(path)

