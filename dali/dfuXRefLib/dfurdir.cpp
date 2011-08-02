/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#include "jliball.hpp"
#include "mpbase.hpp"
#include "rmtfile.hpp"

#define REMOVE_DUPLICATE_DIRS
//#define LOGERRORS

typedef MapStringTo<bool> ExistsHT;


//---------------------------------------------------------------------------

class XREFDirectoryBuilder
{

    IPropertyTree *lookupDir(IPropertyTree *root,const char *dir)
    {
        IPropertyTree *parent;
        StringBuffer pdir;
        const char *tail = splitDirTail(dir,pdir);
        if (pdir.length()&&isPathSepChar(pdir.charAt(pdir.length()-1)))
            pdir.setLength(pdir.length()-1);
        if (pdir.length()==0)
            parent = root;
        else 
            parent = lookupDir(root,pdir.str());
        if (!parent)
            return NULL;
        StringBuffer mask;
        mask.appendf("directory[@name=\"%s\"]",tail);
        IPropertyTree *ret = parent->queryPropTree(mask.str());
        if (!ret) {
            ret = parent->addPropTree("directory", createPTree(false));
            ret->setProp("@name", tail);
        }
        return ret;
    }

public:


    void rootDirectory(const char * directory, INode * node, IPropertyTree * result, unsigned short port)
    {
        //DBGLOG("XREFDirectoryBuilder::rootDirectory %s",directory);
        RemoteFilename rfn;
        rfn.setPath(node->endpoint(),directory);
        if (port)
            rfn.setPort(port);
        Owned<IFile> dir = createIFile(rfn);
        if (!port) {
            unsigned retrycount = 0;
            loop {
                try {
                    dir->isDirectory(); // kludge to force connection
                    break;
                }
                catch (IException *e) {
                    if (retrycount++==10)
                        throw;
                    EXCLOG(e, "XREFDirectoryBuilder");
                    e->Release();
                    Sleep(retrycount*retrycount*500);
                }
            }
        }
        StringBuffer path;
        const char * tag = "directory";
        IPropertyTree * dirTree = result->addPropTree(tag, createPTree(false));
        dirTree->setProp("@name", directory);
        walkDirectory(dir,"", dirTree);
    }

    void resetParent(IPropertyTree *parent,__int64 parentfsz)
    {
        if (parent) 
            parent->setPropInt64("@size", parentfsz);
    }

    void setParent(IPropertyTree *parent,__int64 &parentfsz)
    {
        if (parent) 
            parentfsz = parent->getPropInt64("@size", 0);
        else 
            parentfsz = 0;
    }

    void walkDirectory(IFile *dir, const char * path, IPropertyTree * directory)
    {
        Owned<IDirectoryIterator> iter = dir->directoryFiles(NULL,true,true);
        StringBuffer parentname;
        const char * tailname;
        IPropertyTree *parent=NULL;
        __int64 parentfsz = 0;
        StringBuffer lastparentname;
        StringBuffer mask;
        StringBuffer time;
        StringBuffer fname;
        StringBuffer lowfname;
#ifdef _WIN32
        ExistsHT existsHT;
#endif
        ForEach(*iter) {
            iter->getName(fname.clear());
            lowfname.clear().append(fname).toLowerCase();
#ifdef _WIN32
            bool *eb = existsHT.getValue(lowfname.str());
            if (eb)
                continue;   // Windows kludge
            bool b=true;
            existsHT.setValue(lowfname.str(), b);
#endif
            if (iter->isDir()) {
                // just looking up will create
                parentname.clear().append(fname.str());
                if (parentname.length()&&isPathSepChar(parentname.charAt(parentname.length()-1)))
                    parentname.setLength(parentname.length()-1);
                if (parentname.length()!=0) {
                    resetParent(parent,parentfsz);
                    parent = lookupDir(directory,parentname.str());
                    lastparentname.clear().append(parentname.str());
                    setParent(parent,parentfsz);

                }
            }
            else {
                tailname = splitDirTail(fname.str(),parentname.clear());
                if (parentname.length()&&isPathSepChar(parentname.charAt(parentname.length()-1)))
                    parentname.setLength(parentname.length()-1);
                if (parentname.length()==0) {
                    resetParent(parent,parentfsz);
                    parent = directory;
                    lastparentname.clear();
                    setParent(parent,parentfsz);
                }
                else {
                    if (!parent||(strcmp(parentname.str(),lastparentname.str())!=0)) {
                        resetParent(parent,parentfsz);
                        parent = lookupDir(directory,parentname.str());
                        lastparentname.clear().append(parentname.str());
                        setParent(parent,parentfsz);
                    }
                }
                mask.clear().appendf("file[@name=\"%s\"]",tailname);
                IPropertyTree *entry = parent->addPropTree("file", createPTree(false));
                __int64 fsz = iter->getFileSize();
                entry->setPropInt64("@size", fsz);
                parentfsz += fsz;
                CDateTime dt;
                iter->getModifiedTime(dt);
                dt.getString(time.clear());
                entry->setProp("@modified",time.str());
                entry->setProp("@name", tailname);
            }
        }
        resetParent(parent,parentfsz);
    }       
};

IPropertyTree *getDirectory(const char * directory, INode * node, unsigned short port)
{
    //DBGLOG("IPropertyTree * getDirectory");
    unsigned retries;
    loop {
        StringAttr nextDir;
        try {
            Owned<IPropertyTree> dirTree = createPTree("machine",false);
            StringBuffer url;
            node->endpoint().getIpText(url);
            dirTree->setProp("@ip", url.str());

            XREFDirectoryBuilder builder;
            const char * cur = directory;
            loop
            {
                const char * sep = strchr(cur, ';');
                if (sep)
                    nextDir.set(cur, sep-cur);
                else
                    nextDir.set(cur);
                builder.rootDirectory(nextDir, node, dirTree, port);
            
                if (!sep)
                    break;

                cur = sep+1;        
            }

            return LINK(dirTree.get());
        }
        catch (IException *e) {
            retries++;
            if (retries==10)
                throw;
            StringBuffer s("getDirectory of ");
            if (nextDir)
                s.append(nextDir);
            else
                s.append(directory);
            if (node) {
                s.append(" on ");
                node->endpoint().getUrlStr(s);
            }
            if (port) 
                s.append(" port ").append(port);
            
            EXCLOG(e, s.str());
            e->Release();
            Sleep(retries*retries*500);
        }
    }
}
