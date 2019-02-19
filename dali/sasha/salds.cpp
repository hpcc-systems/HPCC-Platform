#include "platform.h"
#include "jlib.hpp"
#include "jlog.ipp"
#include "jptree.hpp"
#include "jmisc.hpp"
#include "jstring.hpp"

#include "saserver.hpp"
#include "salds.hpp"

// MORE TBD


class CLargeDataStore: public CInterface, implements ILargeDataStore
{
    // needs to be made thread safe when properly implemented

    StringAttr LdsBaseDir;

    void initBase()
    {
        if (!LdsBaseDir.get()) {
            StringBuffer basedir;
            const char *ldsrootdir="LDS";
            IPropertyTree *ldsprops = serverConfig->queryPropTree("LDS");
            if (ldsprops&&ldsprops->hasProp("@rootdir"))
                ldsrootdir = ldsprops->queryProp("@rootdir");
            StringBuffer dataPath;
            if (getConfigurationDirectory(serverConfig->queryPropTree("Directories"),"data","sasha",serverConfig->queryProp("@name"),dataPath)) 
                ldsrootdir=dataPath.str();
            if (!isAbsolutePath(ldsrootdir)) {
                char cpath[_MAX_DIR];
                if (!GetCurrentDirectory(_MAX_DIR, cpath)){
                    OERRLOG("CLargeDataStore::initBase: Current directory path too big, bailing out");
                    throwUnexpected();
                }
                basedir.append(cpath);
                if (*ldsrootdir)
                    addPathSepChar(basedir);
            }
            while (*ldsrootdir) {
                if (isPathSepChar(*ldsrootdir)) {
                    if (ldsrootdir[1])
                        basedir.append(PATHSEPCHAR);
                }
                else
                    basedir.append(*ldsrootdir);
                ldsrootdir++;
            }
            LdsBaseDir.set(basedir.str(),basedir.length());
        }
    }

public:
    IMPLEMENT_IINTERFACE;
    StringBuffer &getLdsPathRoot(const char *relpath, StringBuffer & res)
    {   // no trailing separator
        initBase();
        res.append(LdsBaseDir);
        if (!isPathSepChar(*relpath))
            addPathSepChar(res);
        while (*relpath) {
            if (isPathSepChar(*relpath))
                res.append(PATHSEPCHAR);
            else
                res.append(*relpath);
            relpath++;
        }
        return res;
    }


    virtual ILdsConnection *connect(const char *ldspath)
    {
        return NULL; // TBD
    }

    virtual ILdsConnectionIterator *getIterator(const char *wildldspath)
    {
        return NULL; // TBD
    }

} LargeDataStore;

ILargeDataStore &queryLargeDataStore()
{
    return LargeDataStore;
}


StringBuffer &getLdsPath(const char *relpath, StringBuffer & res)
{   // server side
    LargeDataStore.getLdsPathRoot(relpath,res);
    recursiveCreateDirectory(addPathSepChar(res).str());        
    return res;
}








