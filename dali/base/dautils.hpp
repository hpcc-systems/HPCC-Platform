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

#ifndef DAUTILS_HPP
#define DAUTILS_HPP


#include "jiface.hpp"
#include "jstring.hpp"
#include "jsocket.hpp"
#include "jfile.hpp"
#include "jlog.hpp"
#include "mpbase.hpp"

#ifndef da_decl
#define da_decl __declspec(dllimport)
#endif

interface IRemoteConnection;
interface IProperties;

enum DfsXmlBranchKind
{
    DXB_File,
    DXB_SuperFile,
    DXB_Collection,
    DXB_Scope
};


class da_decl CMultiDLFN;

class da_decl CDfsLogicalFileName
{
    StringAttr lfn;
    unsigned tailpos;
    unsigned localpos; // if 0 not foreign
    StringAttr cluster; 
    CMultiDLFN *multi;   // for temp superfile
    bool external;
    bool allowospath;
public:
    CDfsLogicalFileName();
    ~CDfsLogicalFileName();

    void set(const char *lfn);
    void set(const CDfsLogicalFileName &lfn);
    bool setValidate(const char *lfn,bool removeforeign=false); // checks for invalid chars
    void set(const char *scopes,const char *tail);
    bool setFromMask(const char *partmask,const char *rootdir=NULL);
    void clear();
    bool isSet() const;
    void setForeign(const SocketEndpoint &daliip,bool checklocal);
    void clearForeign();
    void setExternal(const char *location,const char *path);
    void setExternal(const SocketEndpoint &dafsip,const char *path);
    void setExternal(const RemoteFilename &rfn);
    void setQuery(const char *location,const char *query);
    void setQuery(const SocketEndpoint &rfsep,const char *query);
    void setCluster(const char *cname); 
    StringBuffer &getCluster(StringBuffer &cname) const { return cname.append(cluster); }

    const char *get(bool removeforeign=false) const;
    StringBuffer &get(StringBuffer &str,bool removeforeign=false) const;
    const char *queryTail() const;
    StringBuffer &getTail(StringBuffer &buf) const;

    StringBuffer &getScopes(StringBuffer &buf,bool removeforeign=false) const;
    unsigned numScopes(bool removeforeign=false) const; // not including tail
    StringBuffer &getScope(StringBuffer &buf,unsigned idx,bool removeforeign=false) const;
    bool isForeign() const;
    bool isExternal() const { return external; }
    bool isMulti() const { return multi!=NULL; };
    bool isQuery() const; // e.g. RIFS SQL

    StringBuffer &makeScopeQuery(StringBuffer &query, bool absolute=true) const; // returns xpath for containing scope
    StringBuffer &makeFullnameQuery(StringBuffer &query, DfsXmlBranchKind kind, bool absolute=true) const; // return xpath for branch

    bool getEp(SocketEndpoint &ep) const;       // foreign and external
    StringBuffer &getGroupName(StringBuffer &grp) const;    // external only
    bool getExternalPath(StringBuffer &dir, StringBuffer &tail,    // dir and tail can be same StringBuffer
#ifdef _WIN32
                        bool iswin = true,
#else
                        bool iswin = false,
#endif
                        IException **e=NULL) const;     // external only
    bool getExternalFilename(RemoteFilename &rfn) const;

    // Multi routines
    unsigned multiOrdinality() const;
    const CDfsLogicalFileName &multiItem(unsigned idx) const;
    const void resolveWild();  // only for multi
    IPropertyTree *createSuperTree() const;
    void allowOsPath(bool set=true) { allowospath = true; } // allow local OS path to be specified

};


extern da_decl const char * skipScope(const char *lname,const char *scope); // skips a sspecified scope (returns NULL if scope doesn't match)
extern da_decl const char * querySdsFilesRoot();
extern da_decl const char * querySdsRelationshipsRoot();


extern da_decl IPropertyTreeIterator *deserializePartAttrIterator(MemoryBuffer &mb);    // clears mb
extern da_decl MemoryBuffer &serializePartAttr(MemoryBuffer &mb,IPropertyTree *tree); 
extern da_decl IPropertyTree *deserializePartAttr(MemoryBuffer &mb);

extern da_decl void expandFileTree(IPropertyTree *file,bool expandnodes,const char *cluster=NULL); 
     // expands Parts blob in file as well as optionally filling in node IPs
     // if expand nodes set then removes all clusters > 1 (for backward compatibility)
extern da_decl bool shrinkFileTree(IPropertyTree *file); // compresses parts into Parts blob
extern da_decl void filterParts(IPropertyTree *file,UnsignedArray &partslist); // only include parts in list (in expanded tree)





IRemoteConnection *getSortedElements( const char *basexpath, 
                                     const char *xpath, 
                                     const char *sortorder, 
                                     const char *namefilterlo, // if non null filter less than this value
                                     const char *namefilterhi, // if non null filter greater than this value
                                     IArrayOf<IPropertyTree> &results);
interface ISortedElementsTreeFilter
{
    virtual bool isOK(IPropertyTree &tree) = 0;
};

extern da_decl IRemoteConnection *getElementsPaged( const char *basexpath, 
                                     const char *xpath, 
                                     const char *sortorder, 
                                     unsigned startoffset, 
                                     unsigned pagesize, 
                                     ISortedElementsTreeFilter *postfilter, // if non-NULL filters before adding to page
                                     const char *owner,
                                     __int64 *hint,                         // if non null points to in/out cache hint
                                     const char *namefilterlo, // if non null filter less than this value
                                     const char *namefilterhi, // if non null filter greater than this value
                                     IArrayOf<IPropertyTree> &results);

extern da_decl void clearPagedElementsCache();

class da_decl CSDSFileScanner // NB should use dadfs iterators in preference to this unless good reason not to
{
    void processScopes(IRemoteConnection *conn,IPropertyTree &root,StringBuffer &name);
    void processFiles(IRemoteConnection *conn,IPropertyTree &root,StringBuffer &name);

protected:
    bool includefiles;
    bool includesuper;


public:

    virtual void processFile(IPropertyTree &file,StringBuffer &name) {}
    virtual void processSuperFile(IPropertyTree &superfile,StringBuffer &name) {}
    virtual bool checkFileOk(IPropertyTree &file,const char *filename)
    {
        return true;
    }
    virtual bool checkSuperFileOk(IPropertyTree &file,const char *filename)
    {
        return true;
    }
    virtual bool checkScopeOk(const char *scopename)
    {
        return true;
    }
    void scan(IRemoteConnection *conn,  // conn is connection to Files
              bool includefiles=true,
              bool includesuper=false);
              
    bool singlefile(IRemoteConnection *conn,CDfsLogicalFileName &lfn);  // useful if just want to process 1 file using same code



};

extern da_decl const char *queryDfsXmlBranchName(DfsXmlBranchKind kind);
extern da_decl unsigned getFileGroups(IPropertyTree *pt,StringArray &groups,bool checkclusters=false);
extern da_decl unsigned getFileGroups(const char *grplist,StringArray &groups); // actually returns labels not groups
extern da_decl bool isAnonCluster(const char *grp);

interface IClusterFileScanIterator: extends IPropertyTreeIterator
{
public:
    virtual const char *queryName()=0;
};


extern da_decl IClusterFileScanIterator *getClusterFileScanIterator(
                      IRemoteConnection *conn,  // conn is connection to Files
                      IGroup *group,        // only scans file with nodes in specified group
                      bool exactmatch,          // only files that match group exactly (if not true includes base subset or wrapped superset)
                      bool anymatch,            // any nodes match (overrides exactmatch)
                      bool loadbranch);         // whether to load entire branch

class da_decl CheckTime
{
    unsigned start;
    StringBuffer msg;
public:
    CheckTime(const char *s)
    {
        msg.append(s);
        start = msTick();
    }
    ~CheckTime()
    {
        unsigned e=msTick()-start;
        if (e>1000) 
            DBGLOG("TIME: %s took %d", msg.str(), e);
    }
    bool slow() { return ((msTick()-start) > 1000); }
    StringBuffer &appendMsg(const char *s)
    {
        msg.append(s);
        return msg;
    }
};

extern da_decl void getLogicalFileSuperSubList(MemoryBuffer &mb);


interface IDaliMutexNotifyWaiting
{
    virtual void startWait()=0;         // gets notified when starts waiting (after minimum period (1min))
    virtual void cycleWait()=0;
    virtual void stopWait(bool got)=0;  // and when stops
};


interface IDaliMutex: implements IInterface
{
    virtual bool enter(unsigned timeout=(unsigned)-1,IDaliMutexNotifyWaiting *notify=NULL)=0;
    virtual void leave()=0;
    virtual void kill()=0;
};
extern da_decl IDaliMutex  *createDaliMutex(const char *name);

// Called from swapnode and thor
extern da_decl bool checkThorNodeSwap(IPropertyTree *options,   // thor.xml options
                                      const char *failedwuid,   // failed WUID or null if none
                                      unsigned mininterval=0    // minimal interval before redoing check (mins)
                                      ); // if returns true swap needed
// called by swapnode
extern da_decl bool getSwapNodeInfo(IPropertyTree *options,StringAttr &grpname,Owned<IGroup> &grp,Owned<IRemoteConnection> &conn,Owned<IPropertyTree> &info, bool create);

interface IDFSredirection;
extern da_decl IDFSredirection *createDFSredirection(); // only called by dadfs.cpp

extern da_decl void safeChangeModeWrite(IRemoteConnection *conn,const char *name,bool &lockreleased,unsigned timems=INFINITE);

// Local/distributed file wrapper
//===============================

interface IDistributedFile;
interface IFileDescriptor;
interface IUserDescriptor;

interface ILocalOrDistributedFile: extends IInterface
{
    virtual const char *queryLogicalName()=0;
    virtual IDistributedFile * queryDistributedFile()=0;     // NULL for local file
    virtual IFileDescriptor *getFileDescriptor()=0;
    virtual bool getModificationTime(CDateTime &dt) = 0;                        // get date and time last modified (returns false if not set)
    virtual offset_t getFileSize() = 0;

    virtual unsigned numParts() = 0;
    virtual unsigned numPartCopies(unsigned partnum) = 0;
    virtual IFile *getPartFile(unsigned partnum,unsigned copy=0) = 0;
    virtual RemoteFilename &getPartFilename(RemoteFilename &rfn, unsigned partnum,unsigned copy=0) = 0;
    virtual offset_t getPartFileSize(unsigned partnum)=0;   // NB expanded size             
    virtual bool getPartCrc(unsigned partnum, unsigned &crc) = 0;
    virtual bool exists() const = 0;   // if created for writing, this may be false
    virtual bool isExternal() const = 0;
};

extern da_decl ILocalOrDistributedFile* createLocalOrDistributedFile(const char *fname,IUserDescriptor *user,bool onlylocal,bool onlydfs,bool iswrite=false);



#endif
