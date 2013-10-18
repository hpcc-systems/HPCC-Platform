import pprint
from xml.etree.ElementTree import parse

class Exclusion:
    def __init__(self, config='exclusion.xml'):
        self.config = config
        self.tree = parse(config)
        # print self.tree

    def checkException(self, cluster, test):
        result = False
        for B in self.tree.findall('cluster'):
            _cluster = B.attrib['name'].strip()
            #print _cluster
            if cluster == _cluster:
                for E in B.findall('exclude'):
                    _test = E.text
                    _test = _test.strip()
                    #print "[",test, "] -> [",_test, "]"
                    if test == _test:
                        result = True

        return(result)
          

