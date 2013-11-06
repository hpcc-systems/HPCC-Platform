import os
import logging


class ExpandCheck:

    @staticmethod
    def dir_exists(path, require=False):
        logging.debug("dir_exists(path: %s, require: %d", path, require)
        path = os.path.abspath(path)
        if not os.path.exists(path):
            if require:
                logging.debug("Path: %s not found and it is required!" ,path)
                try:
                    os.mkdir(path)
                except:
                    raise IOError("REQUIRED DIRECTORY NOT FOUND. " + path)
            else:
                logging.info( "DIRECTORY NOT FOUND. " + path)

        return(path)

