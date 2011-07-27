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

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jptree.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "jarray.hpp"
#include "jencrypt.hpp"
#include "jregexp.hpp"

#include "daclient.hpp"
#include "dadiags.hpp"
#include "danqs.hpp"
#include "dadfs.hpp"
#include "dasds.hpp"
#include "dautils.hpp"
#include "daaudit.hpp"
#include "daft.hpp"
#include "jptree.hpp"
#include "jlzw.hpp"
#include "jexcept.hpp"

#include "rmtfile.hpp"

#ifdef _WIN32
#include <conio.h>
#else
#define _getch getchar
#define _putch putchar
#endif

#define RECENTCUTOFF 1  // day

extern void coalesceStore();
extern void XmlPatch(const char *inf, char **options);
extern void XmlSize(const char *filename,double pc);
extern void checkExternals(unsigned argc, char **args);
extern void checkClusterCRCs(const char *cluster,bool fix,bool verbose,const char *csvname,const char *singlefilename=NULL);
extern void getTags(const char *filename);
//extern void clusterThorUsage(const char *cluster,unsigned ndays);
//extern void clusterThorUsageLog(const char *logdir,const char *from,const char *to,const char *outcsv);
//extern void PTreeScan(IPropertyTree *root,const char *pattern,StringBuffer &full,StringArray &names,bool mem);
//extern void insertFiles(IPropertyTree *root,IRemoteConnection *conn,IPropertyTree *t);
//extern void testClusterMutex(const char *masterep,const char *gname,unsigned port);
//extern void testXrefFile(const char *fname);
extern void listWorkUnitAssociatedFiles();



static bool noninteractive=false;

void usage(const char *exe)
{
    printf("%s <daliserver> delete <branchxpath>\n", exe);
    printf("%s <daliserver> set <xpath> <value>\n", exe);
    printf("%s <daliserver> get <xpath>\n", exe);
    printf("%s <daliserver> bget <xpath> <dest-file>\n", exe);
    printf("%s <daliserver> xget <xpath> (multi-value tail can have commas)\n", exe);
    printf("%s <daliserver> wget <xpath> (gets all matching xpath)\n",exe); 
    printf("%s <daliserver> add <xpath> <value>\n", exe);
    printf("%s <daliserver> export <branchxpath> <destfile>\n", exe);
    printf("%s <daliserver> import <branchxpath> <srcfile>\n", exe);
    printf("%s <daliserver> importadd <branchxpath> <srcfile>\n", exe);
    printf("%s <daliserver> dfsattr [<user> <password>] \n", exe);
    printf("%s <daliserver> dfscsv [<user> <password>] \n", exe);
    printf("%s <daliserver> dfsfile <logicalname>\n", exe);
    printf("%s <daliserver> lookupfile <logicalname>\n", exe);
    printf("%s <daliserver> lookupgroup <groupname>\n", exe);
    printf("%s <daliserver> lookuptest <logicalname>\n", exe);
    printf("%s <daliserver> replicatemap <logicalname>\n", exe);
    printf("%s <daliserver> dfscluster <logicalname>\n", exe);
    printf("%s <daliserver> filetree <logicalname>\n", exe);
    printf("%s <daliserver> listworkunits [<prop>=<val> <lower> <upper>]\n", exe);
    printf("%s <daliserver> workunittimings <WUID>\n", exe);
    printf("%s <daliserver> cleardfu\n\n", exe);
    printf("%s <daliserver> sdsexists  <sdspath>         (sets return code)\n",exe);
    printf("%s <daliserver> dfsexists  <lname>           (sets return code)\n",exe);
    printf("%s <daliserver> xlatsubnet <xlat-str>   -- eg 10.150.10=10.210.10 \n",exe);
    printf("%s <daliserver> auditlog <fromdate> <todate> <match>\n",exe);
    printf("%s <daliserver> crossclusteread <fromdate> <todate>\n",exe);
    printf("%s <daliserver> checksuperfile <superfilename>\n",exe);
    printf("%s <daliserver> maketestfile <logical-file-name> <cluster> <total-size> <rec-size>\n",exe);
    printf("%s <daliserver> fixtilde   -- fix files that were created with leading '~'\n",exe);
    printf("%s <daliserver> checkclustercrcs <cluster> <outcsv> [verbose]\n",exe);
    printf("%s <daliserver> listredirection\n",exe);
    printf("%s <daliserver> addredirection  <pattern> <replacement> [<pos>]\n",exe);
    printf("%s <daliserver> lookupredirection  <lfn>\n",exe);
    printf("%s coalesce\n", exe);
    printf("%s xmlsize <xmlfile> <per-cent-limit>\n", exe);
    printf("%s xmlpatch <xmlfile> [attrsizes <threshold>] [output <filename>] [fixutf8]\n", exe);
    printf("%s xmlformat <xmlfilein> <xmlfileout>\n", exe);
    printf("%s xmlscan <xmlfile> <pattern>\n", exe);
    printf("%s xmlcompare <xmlfile1> <xmlfile2> <pattern>\n", exe);
    printf("%s extchk [-r] [-t]\n", exe);
}

unsigned testNextId()
{
    static atomic_t nextid;
    return (unsigned)atomic_add_exchange(&nextid,1);
}

#if 0
bool isEmptyTree(IPropertyTree *t)
{
    if (!t)
        return true;
    if (t->numUniq())
        return false;
    Owned<IAttributeIterator> ai = t->getAttributes();
    if (ai->first())
        return false;
    const char *s = t->queryProp(NULL);
    if (s&&*s)
        return false;
    return true;
}
#endif

bool onlyNamePtree(IPropertyTree *t)
{
    if (!t)
        return true;
    if (t->numUniq()) {
//printf("*** numUniq\n");
        return false;
    }
    Owned<IAttributeIterator> ai = t->getAttributes();
    if (ai->first()) {
        if (strcmp(ai->queryName(),"@name")!=0) {
//printf("*** qn=%s\n",ai->queryName());
            return false;
        }
        if (ai->next()) {
//printf("*** next qn=%s\n",ai->queryName());
            return false;
        }
    }
    const char *s = t->queryProp(NULL);
    if (s&&*s) {
//printf("*** qp=%s\n",s);
        return false;
    }
    return true;
}

class CFileReader
{
    Linked<IFileIO> fio;
    MemoryAttr mba;
    size32_t maxlinesize;
    size32_t buffsize;
    char *buf;
    size32_t lbsize;
    char *p;
    bool eof;
    offset_t fpos;
public:
    CFileReader(IFileIO * _in,size32_t _maxlinesize=1024,size32_t _buffsize=0x10000)
        : fio(_in)
    {
        maxlinesize = _maxlinesize;
        buffsize = _buffsize;
        buf = (char *)mba.allocate(buffsize+maxlinesize+1);
        lbsize = 0;
        p=NULL;
        fpos = 0;
        eof = false;
    }

    char* nextLine(size32_t &lnsize)
    {
        if (lbsize<maxlinesize) {
            if (!eof) {
                if (lbsize&&(p!=buf)) 
                    memmove(buf,p,lbsize);
                p = buf;
                size32_t rd = fio->read(fpos,buffsize,buf+lbsize);
                if (rd==0) {
                    eof = true;
                    if (lbsize==0)
                        return NULL;
                    if (buf[lbsize-1]!='\n')
                        buf[lbsize++] = '\n';               // terminate unfinished line
                }
                else {
                    lbsize += rd;
                    fpos += rd;
                }
            }
            else if (lbsize==0)
                return NULL;
        }
        size32_t len = 0;
        char *ret = p;
        while ((len<maxlinesize)&&(p[len]!='\n'))
            len++;
        p[len] = 0;
        lnsize = len;
        len++;
        lbsize-=len;
        p+=len;
        return ret;
    }

};


class CNoSuperAttrIterator: public CInterface, implements IDFAttributesIterator
{
    Linked<IDFAttributesIterator> iter;
    Linked<IUserDescriptor> udesc;
public:
    IMPLEMENT_IINTERFACE;
    CNoSuperAttrIterator(IDFAttributesIterator *_iter,IUserDescriptor* _udesc)
        : iter(_iter), udesc(_udesc)
    {
    }
    bool match()
    {
        const char *name = iter->query().queryProp("@name");
        if (!name||!*name)
            return false;
        Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(name);
        if (!file)
            return false;
        return !file->isSubFile();
    }
    bool first()
    {
        if (!iter->first())
            return false;
        while (!match())
            if (!iter->next())
                return false;
        return true;
    }
    bool next()
    {
        do {
            if (!iter->next())
                return false;
        } while (!match());
        return true;
    }
    bool isValid() { return iter->isValid(); }
    IPropertyTree  & query() { return iter->query(); }
};

void dfsAttr(const char *dali,const char *username,const char *password)
{
    Owned<INode> foreigndali;
    if (dali&&*dali&&(*dali!='*')) {
        SocketEndpoint ep(dali,DALI_SERVER_PORT);
        foreigndali.setown(createINode(ep));
    }
    Owned<IUserDescriptor> udesc;
    if (username&&*username) {
        udesc.setown(createUserDescriptor());
        udesc->set(username,password);
    }
    unsigned start = msTick();
    IDFAttributesIterator *iter = queryDistributedFileDirectory().getDFAttributesIterator("*",true,true,foreigndali,udesc);
//  Owned<IDFAttributesIterator> iter = createSubFileFilter(queryDistributedFileDirectory().getDFAttributesIterator("*",true,true,foreigndali,udesc),udesc,true);
    printf("Time taken = %dms\n",msTick()-start);
    if (iter) {
        if (iter->first()) {
            do {
                IPropertyTree &attr=iter->query();
                //printf("file %s is %"CF"d bytes\n",attr.queryProp("@name"),attr.getPropInt64("@size",-1));
                StringBuffer str;
                toXML(&attr, str);
                printf("%s\n",str.str());
            } while (iter->next());
        }
    }
}


void dfsCsv(const char *dali,const char *username,const char *password)
{

    const char *fields[] = {
        "name","group","directory","partmask","modified","job","owner","workunit","numparts","size","recordCount","recordSize",NULL
    };

    Owned<INode> foreigndali;
    if (dali&&*dali&&(*dali!='*')) {
        SocketEndpoint ep(dali,DALI_SERVER_PORT);
        foreigndali.setown(createINode(ep));
    }
    Owned<IUserDescriptor> udesc;
    if (username&&*username) {
        udesc.setown(createUserDescriptor());
        udesc->set(username,password);
    }
    unsigned start = msTick();
    IDFAttributesIterator *iter = queryDistributedFileDirectory().getDFAttributesIterator("*",true,false,foreigndali,udesc);
    unsigned i;
    for (i=0;fields[i];i++) {
        if (i>0)
            printf(",");
        printf("\"%s\"",fields[i]);
    }
    printf("\n");
    if (iter) {
        StringBuffer aname;
        StringBuffer vals;
        ForEach(*iter) {
            IPropertyTree &attr=iter->query();
            for (i=0;fields[i];i++) {
                aname.clear().append('@').append(fields[i]);
                const char *val = attr.queryProp(aname.str());
                if (i>0)
                    printf(",");
                vals.clear();
                if (val)
                    while (*val) {
                        if (*val!=',')
                            vals.append(*val);
                        val++;
                    }
                printf("%s",vals.str());
            }
            printf("\n");
        }
    }
}

/*
void dfsFileList(const char *cluster)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Files", myProcessSession(), 0, 100000);
    if (!conn) {
        ERRLOG("cannot connect to /Files");
        return;
    }
    Owned<IGroup> group = queryNamedGroupStore().lookup(cluster);
    if (!group) {
        ERRLOG("cannot find cluster %s",cluster);
        return;
    }
    Owned<IClusterFileScanIterator> iter = getClusterFileScanIterator(conn,group,false,false,false);
    ForEach(*iter) {
        printf("%s\n",iter->queryName());
    }
}
*/


void dfsPartList()
{
    Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator("*",false);
    ForEach(*iter) {
        Owned<IDistributedFilePartIterator> piter = iter->query().getIterator();
        ForEach(*piter) {
            IDistributedFilePart &part = piter->query();
            StringBuffer eps;
            StringBuffer fname;
            for (unsigned c=0;c<part.numCopies();c++) {
                part.queryNode(c)->endpoint().getUrlStr(eps.clear());
                RemoteFilename rfn;
                part.getFilename(rfn,c);
                rfn.getRemotePath(fname.clear());
                printf("%s\n",fname.str());
            }
        }
    }
}

void checkWU(const char *wuid,bool &online,bool &archived)
{
    online = false;
    if (wuid&&*wuid) {
        StringBuffer path;
        path.append("WorkUnits/").append(wuid);
        Owned<IRemoteConnection> wuconn = querySDS().connect(path.str(), myProcessSession(), 0, 5*60*1000);  
        online = wuconn.get()!=NULL;
    }
}

offset_t getCompressedSize(IDistributedFile *file)
{  // this should be parallel!
    if (!file)
        return (offset_t)-1;
    offset_t ret = (offset_t)file->queryProperties().getPropInt64("@compressedSize",-1);
    if (ret==(offset_t)-1) {
        try {
            ret = 0;
            Owned<IDistributedFilePartIterator> piter = file->getIterator();
            ForEach(*piter) {
                IDistributedFilePart &part = piter->query();
                offset_t sz = (offset_t)-1;
                for (unsigned c=0;c<part.numCopies();c++) {
                    RemoteFilename rfn;
                    part.getFilename(rfn,c);
                    try {
                        Owned<IFile> file = createIFile(rfn);
                        sz = file->size();
                    }
                    catch (IException *e) {
                        StringBuffer tmp("getCompressedSize(1): ");
                        rfn.getPath(tmp);
                        EXCLOG(e,tmp.str());
                        sz = (offset_t)-1;
                        e->Release();
                    }
                    if (sz!=(offset_t)-1)
                        break;
                }
                if (sz==(offset_t)-1) {
                    ret = (offset_t)-1;
                    break;
                }
                ret += sz;
            }
        }
        catch (IException *e) {
            EXCLOG(e,"getCompressedSize");
            ret = (offset_t)-1;
            e->Release();
        }
    }
    return ret;
}

void dfsFileList(const char *cluster)
{
    if (stricmp(cluster,"*")==0)
        cluster = NULL;
    Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator("*",false);
    printf("Name,%s,Parts,Size,PhysSize,Date,BlkCmp,RowCmp,Key,Persist,Owned,Result,GlobSpill,RecSize,Format,Tag,WUID,"
          "WUonline,"/*"Warchived,"*/
          "owner,job\n",cluster?"":"Cluster,");
    ForEach(*iter) {
        StringBuffer name;
        iter->getName(name);
        if (name.length()==0)
            continue;
        try {
            IDistributedFile &file = iter->query();
            if  (cluster&&(file.findCluster(cluster)==NotFound)) {
                PROGLOG("Ignoring: %s",name.str());
                continue;
            }
            PROGLOG("Processing: %s",name.str());
            offset_t sz = file.getFileSize(true,false);
            CDateTime dt;
            file.getModificationTime(dt);
            StringBuffer dts;
            dt.getDateString(dts);
            IPropertyTree &pt = file.queryProperties();
            int bc = pt.getPropInt("@blockCompressed",0);
            int rc = pt.getPropInt("@rowCompressed",0);
            offset_t csz;
            if (bc||rc)
                csz = getCompressedSize(&file);
            else
                csz = sz;
            size32_t rs = pt.getPropInt("@recordSize",-1);
            bool iskey = isFileKey(pt);
            bool persist = pt.getPropBool("@persist");
            bool owned = pt.getPropBool("@owned");
            bool result = pt.getPropBool("@result");
            bool globalspills = ((memcmp(name.str(),".::a",4)==0)&&strstr(name.str(),"_w2007"))||(memcmp(name.str(),"spills::",8)==0);
            StringBuffer format;
            pt.getProp("@format",format);
            StringBuffer rowTag;
            pt.getProp("@rowTag",rowTag);
            StringBuffer wuid;
            bool wuidonline=false;
            bool wuidarchived=false;
            if (pt.getProp("@workunit",wuid)&&wuid.length()) 
                checkWU(wuid.str(),wuidonline,wuidarchived);
            StringBuffer owner;
            pt.getProp("@owner",owner);
            StringBuffer job;
            pt.getProp("@job",job);
            StringBuffer clustname(",");
            printf("\"%s\"%s,%d,%"I64F"d,%"I64F"d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%s,\"%s\",%s,"
                "%d,"/*"%d,"*/
                "\"%s\",\"%s\"\n",
                name.str(),cluster?"":file.getClusterName(0,clustname).str(),file.numParts(),
                sz,csz,dts.str(),bc,rc,iskey?1:0,persist?1:0,owned?1:0,result?1:0,globalspills?1:0,
                rs,format.str(),rowTag.str(),wuid.str(),
                wuidonline?1:0, /*wuidarchived?1:0, */
                owner.str(),job.str());
        }
        catch (IException *e) {
            EXCLOG(e,name.str());
        }
    }
}


IPropertyTree *getCachedFileTree(const char *lname,const INode *foreigndali,IUserDescriptor *user=NULL, unsigned cacheexpirymins=60, unsigned timeout=FOREIGN_DALI_TIMEOUT)
{
    if (!foreigndali||foreigndali->isHost())
        queryDistributedFileDirectory().getFileTree(lname,foreigndali,user,timeout);
    CDfsLogicalFileName lfn;
    lfn.set(lname);
    StringBuffer cachename("/Roxie/FileCache/foreign::");
    foreigndali->endpoint().getUrlStr(cachename);
    cachename.append("::").append(lfn.get());
    Owned<IRemoteConnection> conn = querySDS().connect(cachename,myProcessSession(),RTM_CREATE_QUERY|RTM_LOCK_WRITE, timeout);
    if (!conn) 
        throw MakeStringException(DFSERR_ForeignDaliTimeout, "getFileTree: Timeout connecting to ", cachename.str());

    MemoryBuffer mb;
    IPropertyTree &root = *conn->queryRoot();
    CDateTime now;
    StringBuffer ds;
    if (root.getProp("@date",ds)&&ds.length()) {
        CDateTime dt;
        dt.setString(ds.str());
        dt.adjustTime(cacheexpirymins);
        now.setNow();
        if (dt.compare(now)>0) { // its ok
            conn->changeMode(RTM_LOCK_READ,INFINITE);
            root.getPropBin(NULL,mb);
            return createPTree(mb);
        }
    }
    Owned<IPropertyTree> tree = queryDistributedFileDirectory().getFileTree(lname,foreigndali,user,timeout);
    tree->serialize(mb);
    root.setPropBin(NULL,mb.length(),mb.toByteArray());
    now.setNow();
    now.getString(ds.clear());
    root.setProp("@date",ds);
    return tree.getClear();
};



void dfsCluster(const char *lname)
{
    StringBuffer str;
    CDfsLogicalFileName lfn;
    lfn.set(lname);
    Owned<IUserDescriptor> userDesc = createUserDescriptor();
    userDesc->set("nhicks","h1ck5");
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname,userDesc);
    if (file) {
        unsigned nc= file->numClusters();
        StringBuffer cname;
        for (unsigned i=0;i<nc;i++) {
            file->getClusterName(0,cname.clear());
            printf("cluster[%d] for '%s' is '%s'\n",i,lfn.get(),cname.str());
        }
    }
}

void dfsFile(const char *lname,const char *dali, bool lookup)
{
    Owned<INode> foreigndali;
    if (dali&&*dali) {
        SocketEndpoint ep(dali,DALI_SERVER_PORT);
        foreigndali.setown(createINode(ep));
    }
    StringBuffer str;
    CDfsLogicalFileName lfn;
    Owned<IUserDescriptor> userDesc = createUserDescriptor();
    userDesc->set("example","password");
    lfn.set(lname);
    if (!lookup&&!lfn.isExternal()) {
        Owned<IPropertyTree> tree = queryDistributedFileDirectory().getFileTree(lname,foreigndali); //,userDesc);
    //  Owned<IPropertyTree> tree = getCachedFileTree(lname,foreigndali);
        if (!tree) {
            printf("%s not found\n",lname);
            return;
        }
        toXML(tree, str);
        printf("%s\n",str.str());
    }
    else if (!dali||!*dali) {
        Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname,userDesc);
        if (file) {
//**
            Owned<IFileDescriptor> fdesc = file->getFileDescriptor();
            printf("compressed = %s\n", fdesc->isCompressed()?"true":"false");
            Owned<IPropertyTree> t = createPTree("File");
            fdesc->serializeTree(*t);
            toXML(t, str.clear());
            printf("%s\n",str.str());
//**
            Owned<IDistributedFilePartIterator> piter = file->getIterator();
            printf("Parts:\n");
            ForEach(*piter) {
                IDistributedFilePart &part = piter->query();
                StringBuffer eps;
                StringBuffer fname;
                for (unsigned c=0;c<part.numCopies();c++) {
                    part.queryNode(c)->endpoint().getUrlStr(eps.clear());
                    RemoteFilename rfn;
                    part.getFilename(rfn,c);
                    rfn.getLocalPath(fname.clear());
                    printf("%s,%s\n",eps.str(),fname.str());
                }
            }
        }
    }
}


void fileCompressionRatio (const char *lname)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname);
    StringBuffer out;
    out.appendf("File %s ",lname);
    if (file) {
        bool compressed = file->isCompressed();
        if (!compressed)
            out.append("not ");
        out.append("compressed, ");
        offset_t size = file->getFileSize(true,false);
        if (size==(offset_t)-1)
            out.appendf("size not known");
        else if (compressed) {
            out.appendf("expanded size %"I64F"d, ",size);
            offset_t csize = getCompressedSize(file);
            if (csize==(offset_t)-1)
                out.append("compressed size unknown");
            else {
                out.appendf("compressed size %"I64F"d",csize);
                if (csize)
                    out.appendf(", Ratio %.2f:1 (%%%d)",(float)size/csize,(unsigned)(size*100/csize));
            }
        }
        else
            out.appendf("not compressed, size %"I64F"d",size);
    }
    else
        out.appendf("File %s not found",lname);
    printf("%s\n",out.str());
}

void lookuptest(const char *lname)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname);
    if (file)
        printf("%s lookup suceeded\n", lname);
}

void printDesc(IFileDescriptor *desc)
{
    if (desc) {
        StringBuffer tmp1;
        StringBuffer tmp2;
        unsigned n = desc->numParts();
        printf("  numParts = %d\n",n);
//      Owned<IGroup> group = desc->getGroup(0);
//      printf("  group(0) = %d,%s,...,%s\n",group->ordinality(),group->queryNode(0).endpoint().getUrlStr(tmp1.clear()).str(),group->queryNode(group->ordinality()-1).endpoint().getUrlStr(tmp2.clear()).str());
//      group.setown(desc->getGroup(1));
//      printf("  group(1) = %d,%s,...,%s\n",group->ordinality(),group->queryNode(0).endpoint().getUrlStr(tmp1.clear()).str(),group->queryNode(group->ordinality()-1).endpoint().getUrlStr(tmp2.clear()).str());
        unsigned i;
        for (i=0;i<n;i++) {
            unsigned copy;
            for (copy = 0;copy<desc->numCopies(i);copy++) {
                if (desc->isMulti(i)) {
                    RemoteMultiFilename rmfn;
                    desc->getMultiFilename(i,copy,rmfn);
                    ForEachItemIn(j,rmfn) {
                        RemoteFilename &rfn = rmfn.item(j);
                        printf("  file (%d,%d,%d) = %s\n",copy,i,j,rfn.getRemotePath(tmp1.clear()).str());
                    }
                }
                else {
                    RemoteFilename rfn;
                    desc->getFilename(i,copy,rfn);
                    printf("  file (%d,%d) = %s\n",copy,i,rfn.getRemotePath(tmp1.clear()).str());
                }
            }
        }
    }
}
void dfsDesc(const char *lname,const char *dali)
{
    Owned<INode> foreigndali;
    if (dali&&*dali) {
        SocketEndpoint ep(dali,DALI_SERVER_PORT);
        foreigndali.setown(createINode(ep));
    }
    printf("================================\n%s:\n",lname);
    Owned<IFileDescriptor> desc = queryDistributedFileDirectory().getFileDescriptor(lname,foreigndali);
    StringBuffer tmp1;
    StringBuffer tmp2;
    while (desc.get()) {
        printDesc(desc);
        if (!lname||(dali&&*dali))
            break;
        Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname);
        desc.setown(file->getFileDescriptor());
        printf("================================\n%s:\n",lname);
        lname = NULL;
    }
}

void dfsMultiDesc(const char *lname,unsigned num)
{
    printf("================================\n%s:\n",lname);
    Owned<IFileDescriptor> desc;
    StringBuffer tmp1;
    StringBuffer tmp2;
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname);
    if (file) {
        Owned<IFileDescriptor> fdesc = file->getFileDescriptor();
        desc.setown(createMultiCopyFileDescriptor(fdesc,num));
        printDesc(desc);
    }
}

IFileDescriptor *createRoxieSlavesFileDescriptor(const char *cluster, const char *lfn)
{
    Owned<IRemoteConnection> envconn = querySDS().connect("/Environment",myProcessSession(),RTM_LOCK_READ, INFINITE);
    if (!envconn) {
        DBGLOG("Could not connect to %s",lfn);
        return NULL;
    }
    Owned<IPropertyTree> envroot = envconn->getRoot();
    StringBuffer grpname(cluster);
    Owned<IGroup> grp = queryNamedGroupStore().lookup(grpname.str());
    if (!grp) {
        ERRLOG("Logical group %s not found",grpname.str());
        return NULL;
    }
    Owned<IFileDescriptor> ret = createFileDescriptor();
    unsigned width = grp->ordinality();
    for (unsigned i=0;i<width;i++) {
        StringBuffer filename;
        StringBuffer dirxpath;
        dirxpath.appendf("Software/RoxieCluster[@name=\"%s\"]/RoxieSlaveProcess[@channel=\"%d\"]/@dataDirectory",cluster,i+1);
        const char * dir = envroot->queryProp(dirxpath.str());
        if (!dir) {
            ERRLOG("dataDirectory not specified");
            return NULL;
        }
        makePhysicalPartName(lfn,i+1,width,filename,false,DFD_OSdefault,dir);
        RemoteFilename rfn;
        rfn.setPath(grp->queryNode(i).endpoint(),filename.str());
        ret->setPart(i,rfn,NULL);
    }
    return ret.getClear();
}

void testRoxieFile(const char *cluster, const char *lfn)
{
    Owned<IFileDescriptor> desc = createRoxieSlavesFileDescriptor(cluster,lfn);
    if (desc.get())
        printDesc(desc);
    else
        ERRLOG("Could not create file descriptor");
}

#define DALI_FILE_LOOKUP_TIMEOUT (1000*60*1)  // 1 minute

void dfsCheck(const char *lname,const char *dali)
{
    Owned<INode> foreigndali;
    if (dali&&*dali) {
        SocketEndpoint ep(dali,DALI_SERVER_PORT);
        foreigndali.setown(createINode(ep));
    }
    Owned<IFileDescriptor> desc = queryDistributedFileDirectory().getFileDescriptor(lname,foreigndali);
    StringBuffer tmp1;
    StringBuffer tmp2;
    if (desc.get()) {
        unsigned n = desc->numParts();
        unsigned i;
        for (i=0;i<n;i++) {
            RemoteFilename rfn;
            desc->getFilename(i,0,rfn);
            Owned<IFile> file = createIFile(rfn);
            if (!file||!file->exists()) {
                RemoteFilename rfn2;
                desc->getFilename(i,1,rfn2);
                file.setown(createIFile(rfn2));
                if (!file||!file->exists()) {
                    printf("FAILED %s: could not find file(%d) %s \n",lname,i,rfn.getRemotePath(tmp1.clear()).str());
                    return;
                }
            }
        }
        printf("OK %s\n",lname);
    }
    else
        printf("Could not load descriptor for %s\n",lname);
}

void dfsSize(const char *lname)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname);
    if (file)
        printf("SIZE(%s)=%"I64F"d\n",lname,file->getFileSize(true,false));
}

void dfsCompressed(const char *lname)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname);
    if (file) {
        bool b;
        bool c = file->isCompressed(&b);
        if (c)
            printf("COMPRESSED(%s)=%s\n",lname,b?"BLOCKED":"UNBLOCKED");
    }
}

void dfsSuperParents(const char *lname)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname);
    if (file) {
        Owned<IDistributedSuperFileIterator> iter = file->getOwningSuperFiles();
        ForEach(*iter) {
            printf("%s,%s\n",iter->query().queryLogicalName(),lname);
        }
    }
}

void unlinkSubFile(const char *lname)
{
    loop {
        Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname);
        if (file) {
            Owned<IDistributedSuperFileIterator> iter = file->getOwningSuperFiles();
            if (!iter->first()) 
                break;
            IDistributedSuperFile *sf = &iter->query();
            if (sf->removeSubFile(lname,false,false))
                printf("removed %s from %s\n",lname,sf->queryLogicalName());
            else
                printf("FAILED to remove %s from %s\n",lname,sf->queryLogicalName());
        }
    }
}

void listFileClusters()
{
    Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator("*",false);
    ForEach(*iter) {
        StringBuffer lfname;
        iter->getName(lfname);
        unsigned start = msTick();
        StringArray clust;
        if (queryDistributedFileDirectory().getFileClusterNames(lfname.str(), clust)==GFCN_Normal)
            printf("%d, %s, %s\n",msTick()-start,clust.ordinality()?clust.item(0):"",lfname.str());
    }
}

void testCompressedFileSizes()
{
    Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator("*",false);
    ForEach(*iter) {
        StringBuffer lfname;
        iter->getName(lfname);
        IDistributedFile &file = iter->query();
        bool blocked;
        if (file.isCompressed(&blocked)&&blocked) {
            IDistributedFilePart &part = file.queryPart(0);
            RemoteFilename rfn;
            part.getFilename(rfn);
            offset_t fse = part.getFileSize(true,false);
            Owned<IFile> f = createIFile(rfn);
            offset_t fsp = f->size();
            printf("%s,%"I64F"d,%"I64F"d,%s\n",lfname.str(),fse,fsp,(fse<fsp)?"LT":((fse==fsp)?"EQ":"OK"));
        }
    }
}

void dfsClusterFiles(const char *grpname)
{
    // ** TBD multi cluster
    ERRLOG("NB Does not handle multi cluster files!");
    Owned<IGroup> grp=queryNamedGroupStore().lookup(grpname);
    if (!grp)
        throw MakeStringException(-1,"group name not found");
    Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator("*",false);
    ForEach(*iter) {
        StringBuffer lfname;
        iter->getName(lfname);
        try {
            IDistributedFile &file=iter->query();
            StringBuffer clustname;
            file.getClusterName(0,clustname);
            if (strcmp(clustname,grpname)!=0) {
                Owned<IDistributedFilePart> part = file.getPart(0);
                if (!grp->queryNode(0).equals(part->queryNode()))
                    continue;
            }
            CDateTime dt;
            file.getModificationTime(dt);
            StringBuffer csvline;
            bool b;
            csvline.append(lfname).append(',').append(clustname).append(',').append(file.numParts())
                   .append(',').append(file.getFileSize(true,false)).append(',');
                    dt.getString(csvline).append(',')
                   .append(file.queryProperties().queryProp("@owner")).append(',')
                   .append(file.queryProperties().queryProp("@job")).append(',')
                   .append(file.queryProperties().queryProp("@workunit")).append(',');
            int rs = file.queryProperties().getPropInt("@recordSize",-1);
            if (rs>0)
                csvline.append(rs);
            else
                csvline.append(file.queryProperties().queryProp("@format"));
            csvline.append(',').append(file.isCompressed(&b)?(b?"BLOCK COMPRESSED":"UNBLOCKED COMPRESSED"):"");
            bool firstparent=true;
            Owned<IDistributedSuperFileIterator> iter = file.getOwningSuperFiles();
            ForEach(*iter) {
                if (firstparent)
                    csvline.append(',');
                else
                    csvline.append('+');
                csvline.append(iter->query().queryLogicalName());
            }
            printf("%s\n",csvline.str());
        }           
        catch (IException *e) {
            EXCLOG(e,lfname.str());
            continue;
        }
    }
}

void fileAttrList()
{
    Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator("*",false);
    ForEach(*iter) {
        StringBuffer lfname;
        iter->getName(lfname);
        Owned<IAttributeIterator> aiter = iter->query().queryProperties().getAttributes();
        ForEach(*aiter) 
            printf("%s,%s,%s\n",lfname.str(),aiter->queryName(),aiter->queryValue());
    }
}


void fileTree(const char *lfn)
{
    CDfsLogicalFileName lname;
    lname.set(lfn);
    StringBuffer query;
    lname.makeFullnameQuery(query, DXB_File, true);
    Owned<IRemoteConnection> conn = querySDS().connect(query.str(),myProcessSession(),RTM_LOCK_READ, INFINITE);
    if (!conn) {
        lname.makeFullnameQuery(query.clear(), DXB_SuperFile, true);
        conn.setown(querySDS().connect(query.str(),myProcessSession(),RTM_LOCK_READ, INFINITE));
    }
    if (!conn) {
        DBGLOG("Could not connect to %s",lfn);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    StringBuffer str;
    toXML(root, str);
    printf("%s\n",str.str());
    Owned<IPropertyTree> tree2 = createPTree(root);
    expandFileTree(tree2,false);
    toXML(tree2, str.clear());
    printf("%s\n",str.str());
#if 0
    printf("---\n");
    setAllocHook(true);
    Owned<IPropertyTree> tree2 = createPTree(root);
    conn->close();
    setAllocHook(false);
    shrinkFileTree(tree2);
    setAllocHook(true);
    Owned<IPropertyTree> tree3 = createPTree(tree2);
    setAllocHook(false);
    toXML(tree3, str.clear());
    printf("---\n");
    printf("%s\n",str.str());
    expandFileTree(tree3,false);
    toXML(tree3, str.clear());
    printf("---\n");
    printf("%s\n",str.str());
#endif

}



int dfsExists(const char *lname)
{
    return queryDistributedFileDirectory().exists(lname)?0:1;
    //Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname);
    //return file.get()?0:1;
}

void dfsModified(const char *lname)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname);
    if (file) {
        CDateTime dt;
        if (file->getModificationTime(dt)) {
            StringBuffer s;
            dt.getString(s);
            printf("%s,%s\n",s.str(),lname);
        }
    }
}

void replicateMap(const char *lname)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname);
    if (!file) {
        printf("File %s not found\n",lname);
        return;
    }
    Owned<IDistributedFilePartIterator> pi = file->getIterator();
    unsigned pn=1;
    StringBuffer ln;
    ForEach(*pi) {
        ln.clear().appendf("%d: ",pn);
        Owned<IDistributedFilePart> part = &pi->get();
        for (unsigned int i=0; i<part->numCopies(); i++) {
            RemoteFilename rfn;
            part->getFilename(rfn,i);
            if (i)
                ln.append(", ");
            rfn.getRemotePath(ln);
        }
        printf("%s\n",ln.str());
        pn++;
    }
}


void testReplicate(const char *lfn)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lfn);
    if (!file) {
        printf("File %s not found\n",lfn);
        return;
    }
    file->enqueueReplicate();
}

struct CGroupEntry : public CInterface
{
    StringAttr name;
    unsigned count;
    bool done;
};
typedef CGroupEntry *CGroupEntryPtr;

typedef MapStringTo<CGroupEntryPtr> CGroupMap;





void checkSuperFileLinkage()
{
    StringArray superowner;
    StringArray superowned;
    StringArray fileowned;
    StringArray fileowner;
    class cfilescan2: public CSDSFileScanner
    {
        StringArray &superowner;
        StringArray &superowned;
        StringArray &fileowned;
        StringArray &fileowner;

        void processFile(IPropertyTree &file,StringBuffer &name)
        {
            Owned<IPropertyTreeIterator> iter = file.getElements("SuperOwner");
            ForEach(*iter) {
                IPropertyTree &sfile = iter->query();
                fileowned.append(name.str());
                fileowner.append(sfile.queryProp("@name"));
            }
        }
        void processSuperFile(IPropertyTree &file,StringBuffer &name)
        {
            unsigned numsub = file.getPropInt("@numsubfiles");
            unsigned n = 0;
            Owned<IPropertyTreeIterator> iter = file.getElements("SubFile");
            ForEach(*iter) {
                IPropertyTree &sfile = iter->query();
                superowner.append(name.str());
                superowned.append(sfile.queryProp("@name"));
                n++;
            }
            if (n!=numsub)
                PROGLOG("FAILED mismatchedsub(%d,%d)",numsub,n);
            processFile(file,name); // superfile is a file too!
        }
    public:

        cfilescan2(StringArray &_superowner,StringArray &_superowned,StringArray &_fileowned,StringArray &_fileowner)
            : superowner(_superowner),superowned(_superowned),fileowned(_fileowned),fileowner(_fileowner)
        {
        }

    } filescan(superowner,superowned,fileowned,fileowner);

    Owned<IRemoteConnection> conn = querySDS().connect("/Files", myProcessSession(),0, 100000);
    filescan.scan(conn,true,true);
    ForEachItemIn(i1,superowner) {
        bool ok = false;
        ForEachItemIn(i2,fileowned) {
            if ((stricmp(superowner.item(i1),fileowner.item(i2))==0)&&(stricmp(superowned.item(i1),fileowned.item(i2))==0)) {
                ok = true;
                break;
            }
        }
        if (!ok)
            PROGLOG("FAILED super(%s,%s)",superowner.item(i1),superowned.item(i1));
    }       
    ForEachItemIn(i3,fileowned) {
        bool ok = false;
        ForEachItemIn(i4,superowner) {
            if ((stricmp(superowner.item(i4),fileowner.item(i3))==0)&&(stricmp(superowned.item(i4),fileowned.item(i3))==0)) {
                ok = true;
                break;
            }
        }
        if (!ok)
            PROGLOG("FAILED file(%s,%s)",fileowned.item(i3),fileowner.item(i3));
    }       

}



void checkDir2()
{
    Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator("*",true);
    ForEach(*iter) {
        IDistributedFile &file=iter->query();
        StringBuffer name;
        iter->getName(name);
        printf("%s  %s\n",name.str(),file.queryDefaultDir());
    }
}

void fixDates()
{
    Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator("*",true);
    ForEach(*iter) {
        IDistributedFile &file=iter->query();
        StringBuffer lfname;
        iter->getName(lfname);
        try {
            PROGLOG("Processing %s",lfname.str());
            Owned<IDistributedFilePartIterator> pi = file.getIterator();
            unsigned pn=1;
            bool locked = false;
            unsigned numok = 0;
            ForEach(*pi) {
                Owned<IDistributedFilePart> part = &pi->get();
                CDateTime ltime;
                const char *mdate = part->queryProperties().queryProp("@modified");
                if (!mdate||!*mdate)
                    continue;
                ltime.setString(mdate);
                for (unsigned int i=0; i<part->numCopies(); i++) {
                    RemoteFilename rfn;
                    part->getFilename(rfn,i);
    #if 1
                    try {
                        Owned<IFile> f = createIFile(rfn);
                        if (f) {
                            CDateTime ptime;
                            if (f->getTime(NULL,&ptime,NULL)) {
                                if (!ltime.equals(ptime)) {
                                    StringBuffer ptimestr;
                                    ptime.getString(ptimestr);
                                    int dif = ltime.getSimple()-ptime.getSimple();
                                    if ((dif>=-2)&&(dif<=2)) {
                                        if (!locked) {
                                            file.lockProperties();
                                            locked = true;
                                            PROGLOG("Modifying %s",lfname.str());
                                        }
                                        part->lockProperties().setProp("@modified",ptimestr.str());
                                        part->unlockProperties();
                                    }
                                    else {
                                        StringBuffer fname;
                                        rfn.getRemotePath(fname);
                                        StringBuffer ltimestr;
                                        ltime.getString(ltimestr);
                                        ERRLOG("%s[%d]=%s %s=%s\n",lfname.str(),pn,ltimestr.str(),fname.str(),ptimestr.str());
                                    }
                                }
                                else
                                    if (!locked)
                                        numok++;
                            }
                            break;
                        }
                        else {
                            StringBuffer fname;
                            rfn.getRemotePath(fname);
                            ERRLOG("Cannot open %s",fname.str());
                        }
                    }
                    catch (IException *e) {
                        StringBuffer fname;
                        rfn.getRemotePath(fname);
                        EXCLOG(e,fname.str());
                    }
    #else
                    StringBuffer fname;
                    rfn.getRemotePath(fname);
                    if (strstr(fname.str(),"\\.\\")) {
                        ERRLOG("!!! %s",fname.str());
                    }
                    numok++;
    #endif
                }       

                pn++;
                if (numok==5)       
                    break;
            }
            if (locked)
                file.unlockProperties();
        }
        catch (IException *e) {
            EXCLOG(e,lfname.str());
        }
    }
}

void checkSize(const char *name)
{
    UNIMPLEMENTED;
#if 0
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(name);
    if(!df)
        throw MakeStringException(0,"Could not find file %s",name);

    offset_t size=queryDistributedFileSystem().getSize(df,false,false);
    
    printf("file size %"CF"d\n",size);
    return;
    Owned<IDistributedFilePartIterator> pi = df->getIterator();
    unsigned pn=0;
    ForEach(*pi)
    {
        Owned<IDistributedFilePart> part = &pi->get();

        for (unsigned int i=0; i<part->numCopies(); i++)
        {
            StringBuffer b;
            part->queryNode(i)->endpoint().getUrlStr(b);
            offset_t size=queryDistributedFileSystem().getSize(part);
            printf("part[%d,%d] size %"CF"d\n",pn,i,size);
            pn++;
        }
    }
#endif
}

void checkScopes(const char *name,bool recursive)
{
    Owned<IDFScopeIterator> iter = queryDistributedFileDirectory().getScopeIterator(name,recursive);
    CDfsLogicalFileName dlfn;
    StringBuffer s;
    ForEach(*iter) {
        CDfsLogicalFileName dlfn;
        dlfn.set(iter->query(),"x");
        dlfn.makeScopeQuery(s.clear(),true);
        printf("%s=%s\n",iter->query(),s.str());
        Owned<IRemoteConnection> conn = querySDS().connect(s.str(),myProcessSession(),RTM_LOCK_READ, INFINITE);
        if (!conn)  
            DBGLOG("Could not connect to '%s' using %s",iter->query(),s.str());
        else if (!conn->queryRoot()->hasProp("File[1]")&&!conn->queryRoot()->hasProp("SuperFile[1]")&&!conn->queryRoot()->hasProp("Scope[1]")) {
            if (onlyNamePtree(conn->queryRoot())) {
                printf("EMPTY %s\n",iter->query());
            }
        }
    }
}

void cleanScopes()
{
    loop {
        unsigned deleted = 0;
        Owned<IDFScopeIterator> iter = queryDistributedFileDirectory().getScopeIterator(NULL,true);
        CDfsLogicalFileName dlfn;
        StringBuffer s;
        ForEach(*iter) {
            CDfsLogicalFileName dlfn;
            dlfn.set(iter->query(),"x");
            dlfn.makeScopeQuery(s.clear(),true);
            //printf("%s=%s\n",iter->query(),s.str());
            Owned<IRemoteConnection> conn = querySDS().connect(s.str(),myProcessSession(),RTM_LOCK_READ, INFINITE);
            if (!conn) 
                DBGLOG("Could not connect to '%s' using %s",iter->query(),s.str());
            else if (!conn->queryRoot()->hasProp("File[1]")&&!conn->queryRoot()->hasProp("SuperFile[1]")&&!conn->queryRoot()->hasProp("Scope[1]")) {
                if (onlyNamePtree(conn->queryRoot())) {
                    printf("Deleted %s\n",iter->query());
                    deleted++;
                    conn->close(true);
                }
            }
        }
        if (deleted==0)
            break;
    }
}

void printWorkunits(const char *test,const char *min, const char *max)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/", myProcessSession(), 0, 5*60*1000);  
    Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("WorkUnits/*");
    ForEach(*iter) {
        IPropertyTree &e=iter->query();
        if (test&&*test) {
            const char *tval = strchr(test,'=');
            if (!tval) {
                printf("ERROR missing '=' in %s\n",test);
                return;
            }
            StringBuffer prop;
            if (*test!='@')
                prop.append('@');
            prop.append(tval-test,test);
            tval++;
            const char *val = e.queryProp(prop.str());
            if (!val||(strcmp(val,tval)!=0))
                continue;
            if (min &&(strcmp(e.queryName(),min)<0))
                continue;
            if (max &&(strcmp(e.queryName(),max)>0))
                continue;
        }
            printf("%s\n",e.queryName());
        if (0) { // printownedres) {
            Owned<IPropertyTreeIterator> results = e.getElements("Results/Result");
            ForEach(*results) {
                StringBuffer name;
                results->query().getProp("@tempFilename",name);
                if (name.length()) {
                    //name.toLowerCase();
                    StringBuffer mask;
                    mask.append("Files/File[@name=\"").append(name).append("\"]/@kind");
                    unsigned t = e.getPropInt(mask.str(),-1);
                    printf("  FILE '%s' type = %d\n",name.str(),t);
                }
            }

        }
    }
}

static const char *splitWUID(const char *wuid,StringBuffer &head)
{
    while (*wuid&&(*wuid!='-')) {
        head.append((char)toupper(*wuid));
        wuid++;
    }
    return wuid;
}

static void splitWUIDpath(const char *wuid,StringBuffer &path)
{
    StringBuffer head;
    splitWUID(wuid,head);
    if (head.length()) {
        if (path.length())
            path.append('/');
        path.append(head);
    }
}


void pruneProtectedWorkunits()
{
    const char * WU_ARCHIVE_PATH="//10.173.72.206/c$/LDS/Archive/WorkUnits";
    Owned<IRemoteConnection> conn = querySDS().connect("/", myProcessSession(), 0, 5*60*1000);  
    Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("WorkUnits/*");
    StringBuffer path;
    ForEach(*iter) {
        IPropertyTree &e=iter->query();
        if (e.getPropBool("@protected")) {
            const char *wuid = e.queryName();
            if (wuid&&*wuid) {
                path.clear().append(WU_ARCHIVE_PATH);
                splitWUIDpath(wuid,path);
                path.append('/').append(wuid).append(".xml");
                Owned<IFile> file = createIFile(path.str());
                bool backedup = file&&file->exists();
                const char * state = e.queryProp("@state");
                const char * job = e.queryProp("@jobName");
                const char * owner = e.queryProp("@submitID");
                PROGLOG("PROTWU,%s,%s,\"%s\",\"%s\",\"%s\"",wuid,backedup?"BACKEDUP":"NOTBACKEDUP",
                                 state?state:"",job?job:"",owner?owner:"");
                if ((wuid[0]!='W')||!backedup)
                    continue;
                bool unprot = (strcmp(wuid,"W2007")<0);
                if (strcmp(wuid,"W2008")<0) {
                    if (!state||(strcmp(state,"completed")!=0))
                        unprot = true;
                    if (!job||!*job)
                        unprot = true;
                }
                if (unprot) {
                    PROGLOG("UNPROT,%s",wuid);
                    e.setPropBool("@protected",false);
                    conn->commit();
                }
            }
        }
    }
}

static const char *splitpath(const char *path,StringBuffer &head,StringBuffer &tmp)
{
    if (path[0]!='/')
        path = tmp.append('/').append(path).str();
    return splitXPath(path, head);
}

static const char *remLeading(const char *s)
{
    if (*s=='/')
        s++;
    return s;
}

int sdsExists(const char *xpath, StringBuffer &out)
{
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(xpath,head,tmp);
    if (!tail)
        return 1;
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),RTM_LOCK_WRITE, INFINITE);
    if (!conn) 
        return 1;
    Owned<IPropertyTree> root = conn->getRoot();
    if (!root->hasProp(tail))
        return 1;
    Owned<IPropertyTree> child = root->getPropTree(tail);
    if (child)  {
        out.append(head.str()).append('/').append(child->queryName());
        unsigned idx = root->queryChildIndex(child);
        if (idx==0) {
            // see if 1 exists
            StringBuffer tmp2;
            StringBuffer tmp3;
            tmp2.append(child->queryName()).append("[1]");
            tmp3.append(child->queryName()).append("[2]");
            if (root->hasProp(tmp2.str())&&!root->hasProp(tmp3.str()))
                idx = NotFound;
        }
        if (idx!=NotFound)
            out.append('[').append(idx+1).append(']');
    }
    else
        out.append(xpath);
    return 0;
}
    
void deleteBranch(const char *path,bool backup)
{
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),RTM_LOCK_WRITE, INFINITE);
    if (!conn) {
        DBGLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    Owned<IPropertyTree> child = root->getPropTree(tail);
    if (!child) {
        DBGLOG("Couldn't find %s/%s",head.str(),tail);
        return;
    }
    if (backup) {
        CDateTime dt;
        dt.setNow();
        StringBuffer bakname;
        dt.getString(bakname);
        unsigned i;
        for (i=0;i<bakname.length();i++)
            if (bakname.charAt(i)==':')
                bakname.setCharAt(i,'_');
        bakname.append(".bak");
        DBGLOG("Saving backup of %s/%s to %s",head.str(),tail,bakname.str());
        Owned<IFile> f = createIFile(bakname.str());
        Owned<IFileIO> io = f->open(IFOcreate);
        Owned<IFileIOStream> fstream = createBufferedIOStream(io);
        toXML(child, *fstream);         // formatted (default)
    }
    root->removeTree(child);
    child.clear();
    root.clear();
    conn->commit();
    conn->close();
}

void deleteFileBranch(const char *path,bool backup)
{
    const char *savepath = path;
    CDfsLogicalFileName lfn;
    StringBuffer lfnpath;
    if ((strstr(path,"::")!=NULL)&&!strchr(path,'/')) {
        lfn.set(path);
        lfn.makeFullnameQuery(lfnpath,DXB_File);
        path = lfnpath.str();
    }
    else 
        DBGLOG("%s isn't file!",path);
    Owned<IRemoteConnection> conn = querySDS().connect(remLeading(path),myProcessSession(),RTM_LOCK_READ, INFINITE);
    if (!conn) {
        if (lfnpath.length()) {
            lfn.makeFullnameQuery(lfnpath.clear(),DXB_SuperFile);
            path = lfnpath.str();
            conn.setown(querySDS().connect(remLeading(path),myProcessSession(),RTM_LOCK_READ, INFINITE));
        }
        if (!conn) {
            DBGLOG("Could not connect to %s",savepath);
            return;
        }
    }
    conn.clear();
    deleteBranch(path,backup);
}


void printBadWorkUnits()
{
    StringArray todelete;
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/", myProcessSession(), 0, 5*60*1000);  
        Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("WorkUnits/*");
        ForEach(*iter) {
            IPropertyTree &e=iter->query();
            const char *name = e.queryName();
            if (e.getPropBool("@protected")) {
                const char *state = e.queryProp("@state");
                if ((!state||(stricmp(state,"completed")!=0))&&(name[4]<='4')) {
                    e.setPropBool("@protected",false);
                    PROGLOG("UNPROTECTED(%s): %s",state?state:"",name);
                }
                else
                    PROGLOG("PROTECTED(%s): %s",state?state:"",name);
            }
            else if (e.getPropBool("@archiveError")) {
                PROGLOG("ERROR: %s",name);
                todelete.append(name);
            }
            else if (name[4]<='4')
                PROGLOG("OLD: %s",name);
        }
    }
    ForEachItemIn(i,todelete) {
        const char *name = todelete.item(i);
        try {
            StringBuffer path;
            path.append("/WorkUnits/").append(name);
            deleteBranch(path.str(),true);
            path.clear().append("/GraphProgress/").append(name);
            deleteBranch(path.str(),true);
        }
        catch (IException *e) {
            EXCLOG(e,name);
        }

    }
}



void setProp(const char *path,const char *val)
{
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),RTM_LOCK_WRITE, INFINITE);
    if (!conn) {
        DBGLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    StringBuffer oldv;
    StringBuffer newv;
    root->getProp(tail,oldv);
    root->setProp(tail,val);
    conn->commit();
    root->getProp(tail,newv);
    DBGLOG("Changed %s from '%s' to '%s'",path,oldv.str(),newv.str());
    conn->close();
}


void addProp(const char *path,const char *val)
{
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),RTM_LOCK_WRITE, INFINITE);
    if (!conn) {
        DBGLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    StringBuffer oldv;
    StringBuffer newv;
    root->addProp(tail,val);
    conn->commit();
    DBGLOG("Added %s value '%s'",path,val);
    conn->close();
}


void getProp(const char *path)
{
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),RTM_LOCK_READ, INFINITE);
    if (!conn) {
        DBGLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    StringBuffer val;
    root->getProp(tail,val);
    DBGLOG("Value of %s is: '%s'",path,val.str());
    conn->close();
}

void delProp(const char *path)
{
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),RTM_LOCK_WRITE, INFINITE);
    if (!conn) {
        DBGLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    StringBuffer val;
    root->getProp(tail,val);
    root->removeProp(tail);
    DBGLOG("Value of %s was: '%s'",path,val.str());
    conn->close();
}

void wgetProp(const char *path)
{
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),RTM_LOCK_READ, INFINITE);
    if (!conn) {
        DBGLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements(tail);
    unsigned n = 0;
    ForEach(*iter) {
        n++;
        const char *s = iter->query().queryName();
        DBGLOG("%d,%s",n,s);
    }
    conn->close();
}


void getPropBin(const char *path,const char *outfn)
{
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),RTM_LOCK_READ, INFINITE);
    if (!conn) {
        DBGLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    MemoryBuffer val;
    root->getPropBin(tail,val);
    Owned<IFile> f = createIFile(outfn);
    Owned<IFileIO> io = f->open(IFOcreate);
    io->write(0,val.length(),val.toByteArray());
    conn->close();
}

void xgetProp(const char *path)
{
    if (!path||!*path)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect("/",myProcessSession(),RTM_LOCK_READ, INFINITE);
    if (!conn) {
        DBGLOG("Could not connect to /");
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    StringBuffer head;
    StringBuffer tmp;
    const char *props=splitpath(path,head,tmp);
    const char *s = head.str();
    if (*s=='/')
        s++;
    Owned<IPropertyTreeIterator> it = root->getElements(s);
    if (it->first()) {
        unsigned idx = 0;
        do {
            idx++;
            StringBuffer res;
            res.append(idx).append(',');
            s = props;
            loop {
                const char *e = strchr(s,',');
                if (e&&e[1]) {
                    StringBuffer prop(e-s,s);
                    it->query().getProp(prop.str(),res);
                    s = e+1;
                    res.append(',');
                }
                else {
                    it->query().getProp(s,res);
                    break;
                }
            }

            printf("%s\n",res.str());
        } while (it->next());
    }
    conn->close();
}



void _export_(const char *path,const char *dst,bool safe=false)
{
    const char *savepath=path;
    CDfsLogicalFileName lfn;
    StringBuffer lfnpath;
    if ((strstr(path,"::")!=NULL)&&!strchr(path,'/')) {
        lfn.set(path);
        lfn.makeFullnameQuery(lfnpath,DXB_File);
        path = lfnpath.str();
    }
    else if (strchr(path+((*path=='/')?1:0),'/')==NULL)
        safe = true;    // all root trees safe
    Owned<IRemoteConnection> conn = querySDS().connect(remLeading(path),myProcessSession(),safe?0:RTM_LOCK_READ, INFINITE);
    if (!conn) {
        if (lfnpath.length()) {
            lfn.makeFullnameQuery(lfnpath.clear(),DXB_SuperFile);
            path = lfnpath.str();
            conn.setown(querySDS().connect(remLeading(path),myProcessSession(),safe?0:RTM_LOCK_READ, INFINITE));
        }
        if (!conn) {
            DBGLOG("Could not connect to %s",savepath);
            return;
        }
    }
    Owned<IPropertyTree> root = conn->getRoot();
    Owned<IFile> f = createIFile(dst);
    Owned<IFileIO> io = f->open(IFOcreate);
    Owned<IFileIOStream> fstream = createBufferedIOStream(io);
    toXML(root, *fstream);          // formatted (default)
    DBGLOG("Branch %s saved in '%s'",path,dst);
    conn->close();
}



void import(const char *path,const char *src,bool add)
{
    Owned<IFile> iFile = createIFile(src);
    Owned<IFileIO> iFileIO = iFile->open(IFOread);
    if (!iFileIO)
    {
        DBGLOG("Could not open to %s",src);
        return;
    }
    size32_t sz = iFile->size();
    StringBuffer xml;
    iFileIO->read(0, sz, xml.reserve(sz));
    Owned<IPropertyTree> branch = createPTreeFromXMLString(xml.str());
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    if (!add) {
        Owned<IRemoteConnection> bconn = querySDS().connect(remLeading(path),myProcessSession(),RTM_LOCK_READ|RTM_SUB, INFINITE);
        if (bconn) {
            Owned<IPropertyTree> broot = bconn->getRoot();
            StringBuffer bakname(src);
            bakname.append(".bak");
            DBGLOG("Saving backup of %s to %s",path,tail,bakname.str());
            Owned<IFile> f = createIFile(bakname.str());
            Owned<IFileIO> io = f->open(IFOcreate);
            Owned<IFileIOStream> fstream = createBufferedIOStream(io);
            toXML(broot, *fstream);         // formatted (default)
        }
    }
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),0, INFINITE);
    if (!conn) {
        DBGLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    if (!add) {
      Owned<IPropertyTree> child = root->getPropTree(tail);
      root->removeTree(child);
    }
    root->addPropTree(tail,LINK(branch));
    conn->commit();
    DBGLOG("Branch %s loaded from '%s'",path,src);
    conn->close();
    if (*path=='/')
        path++;
    if (strcmp(path,"Environment")==0) {
        DBGLOG("Refreshing cluster groups from Environment");
        initClusterGroups();
    }
}

void importEnv(const char *file)
{
    import("Environment",file,false);
    initClusterGroups();
}


void bimport(const char *path,const char *src,bool add)
{
    Owned<IFile> iFile = createIFile(src);
    Owned<IFileIO> iFileIO = iFile->open(IFOread);
    if (!iFileIO)
    {
        DBGLOG("Could not open to %s",src);
        return;
    }
    size32_t sz = iFile->size();
    StringBuffer xml;
    iFileIO->read(0, sz, xml.reserve(sz));
    Owned<IPropertyTree> branch = createPTree();
    branch->setPropBin("data",xml.length(),(void*)xml.toCharArray());
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    if (!add) {
        Owned<IRemoteConnection> bconn = querySDS().connect(remLeading(path),myProcessSession(),RTM_LOCK_READ|RTM_SUB, INFINITE);
        if (bconn) {
            Owned<IPropertyTree> broot = bconn->getRoot();
            StringBuffer bakname(src);
            bakname.append(".bak");
            DBGLOG("Saving backup of %s to %s",path,tail,bakname.str());
            Owned<IFile> f = createIFile(bakname.str());
            Owned<IFileIO> io = f->open(IFOcreate);
            Owned<IFileIOStream> fstream = createBufferedIOStream(io);
            toXML(broot, *fstream);         // formatted (default)
        }
    }
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),0, INFINITE);
    if (!conn) {
        DBGLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    if (!add) {
      Owned<IPropertyTree> child = root->getPropTree(tail);
      root->removeTree(child);
    }

    root->addPropTree(tail,LINK(branch));
    conn->commit();
    DBGLOG("Branch %s loaded from '%s'",path,src);
    conn->close();
}




void sizebranch(unsigned depth,const char *path,__int64 &intsize,__int64 &extsize)
{
    __int64 is;
    __int64 es;
    if (depth>1) {
        StringBuffer name;
        if (0) {
            try {
                Owned<IRemoteConnection> conn = querySDS().connect(path,myProcessSession(),RTM_SUB, INFINITE);
                Owned<IPropertyTree> root = conn->getRoot();
                root->getProp("@name",name);
                StringBuffer buf;
                toXML(root, buf);           // formatted (default)
                es = buf.length();
            }
            catch (IException *e) {
                StringBuffer s;
                s.appendf("path '%s' name '%s'",path,name.str());
                EXCLOG(e,s.str());
            }
        }
        {
            MemoryBuffer mb;
            mb.append("sdssize").append(path);
            getDaliDiagnosticValue(mb);
            size32_t ret;
            mb.read(ret);
            is = ret;
        }
        if (0) {
            if (name.length())
                printf("%s:%s,%"I64F"d,%"I64F"d\n",path,name.str(),is,es);
            else
                printf("%s,%"I64F"d,%"I64F"d\n",path,is,es);
        }
        else {
            if (name.length())
                printf("%s:%s,%"I64F"d\n",path,name.str(),is);
            else
                printf("%s,%"I64F"d\n",path,is);
        }
        fflush(stdout);
    }
    else {
        is = 0;
        es = 0;
        StringBuffer name;
        Owned<IRemoteConnection> conn = querySDS().connect(path?path:"",myProcessSession(),0, INFINITE);
        Owned<IPropertyTree> root = conn->getRoot();
        root->getProp("@name",name);
        Owned<IPropertyTreeIterator> it = root->getElements("*");
        StringBuffer last;
        unsigned lastcount = 1;
        ForEach(*it) {
            StringBuffer s;
            {
                IPropertyTree &child = it->query();
                if (path) 
                    s.append(path).append('/');
                s.append(child.queryName());
                if (strcmp(last.str(),s.str())==0) {
                    lastcount++;
                }
                else {
                    last.clear().append(s);
                    lastcount = 1;
                }
                s.append('[').append(lastcount).append(']');
            }
            sizebranch(depth+1,s.str(),is,es);
        }
        if (name.length())
            printf("%s:%s,%"I64F"d,%"I64F"d\n",path?path:"/",name.str(),is,es);
        else
            printf("%s,%"I64F"d,%"I64F"d\n",path?path:"/",is,es);
        fflush(stdout);

    }
    intsize += is;
    extsize += es;
}

void branchdir(const char *path)
{
    StringBuffer name;
    Owned<IRemoteConnection> conn = querySDS().connect(path?path:"",myProcessSession(),0, INFINITE);
    Owned<IPropertyTree> root = conn->getRoot();
    root->getProp("@name",name);
    Owned<IPropertyTreeIterator> it = root->getElements("*");
    StringBuffer last;
    unsigned lastcount = 1;
    ForEach(*it) {
        StringBuffer s;
        IPropertyTree &child = it->query();
        if (path) 
            s.append(path).append('/');
        s.append(child.queryName());
        if (strcmp(last.str(),s.str())==0) {
            printf("[%d]\n",lastcount);
            lastcount++;
        }
        else {
            if (lastcount>1)
                printf("[%d]\n",lastcount);
            else if (last.length())
                printf("\n");
            last.clear().append(s);
            lastcount = 1;
        }
        printf("%s",s.str());
    }
    if (lastcount>1)
        printf("[%d]\n",lastcount);
    else if (last.length())
        printf("\n");
    fflush(stdout);
}


void clearDFURecovery()
{
    CDateTime cutoff;
    cutoff.setNow();
    cutoff.adjustTime(-24*60*2);
    Owned<IRemoteConnection> conn = querySDS().connect("DFU/RECOVERY",myProcessSession(),0, INFINITE);
    Owned<IPropertyTree> root = conn->getRoot();
    Owned<IPropertyTreeIterator> it = root->getElements("job");
    IArrayOf<IPropertyTree> todelete;
    ForEach(*it) {
        IPropertyTree &e=it->query();
        StringBuffer time_started;
        if (e.getProp("@time_started",time_started)) {
            CDateTime dt;
            dt.setString(time_started.str());
            if (dt.compare(cutoff)<0) {
                e.Link();
                todelete.append(e);
            }
        }
    }
    ForEachItemIn(i,todelete) {
        IPropertyTree &item = todelete.item(i);
        if (root->removeTree(&item))
            printf("deleted\n");
        else
            printf("not deleted\n");
    }
}

static int comptime(IInterface **v1, IInterface **v2) // for bAdd only
{
    IPropertyTree *e1 = (IPropertyTree *)*v1;
    IPropertyTree *e2 = (IPropertyTree *)*v2;
    int r=stricmp(e1->queryProp("@time_started"),e2->queryProp("@time_started"));
    if (r<0)
        return -1;
    return 1;
}

static void countProgress(IPropertyTree *t,unsigned &done,unsigned &total)
{
    total = 0;
    done = 0;
    Owned<IPropertyTreeIterator> it = t->getElements("DFT/progress");
    ForEach(*it) {
        IPropertyTree &e=it->query();
        if (e.getPropInt("@done",0))
            done++;
        total++;
    }
}

void probeQ(const char *qname)
{
    Owned<INamedQueueConnection> con = createNamedQueueConnection(0);
    Owned<IQueueChannel> channel = con->open(qname);
    printf("probe %s = %d\n",qname,channel->probe());
}

void displayDFURecovery()
{
    CDateTime cutoff;
    cutoff.setNow();
    cutoff.adjustTime(-24*60*14);
    Owned<IRemoteConnection> conn = querySDS().connect("DFU/RECOVERY",myProcessSession(),0, INFINITE);
    Owned<IPropertyTree> root = conn->getRoot();
    Owned<IPropertyTreeIterator> it = root->getElements("job");
    IArrayOf<IPropertyTree> tolist;
    ForEach(*it) {
        IPropertyTree &e=it->query();
        StringBuffer time_started;
        if (e.getProp("@time_started",time_started)) {
            CDateTime dt;
            dt.setString(time_started.str());
            if (dt.compare(cutoff)>0) {
                IInterface *val=LINK(&e);
                bool added;
                tolist.bAdd(val,comptime,added);
            }
        }
    }
    ForEachItemIn(i,tolist) {
        IPropertyTree &e = tolist.item(i);
        unsigned done;
        unsigned total;
        countProgress(&e,done,total);
        printf("%s:\n\t %s %s \n\tdone = %d of %d\n",e.queryProp("@time_started"),e.queryProp("@command"),e.queryProp("@command_parameters"),done,total);
    }
}

void fixIN()
{
    Owned<IRemoteConnection> conn = querySDS().connect("Files/Scope[@name=\"thor_data400\"]/Scope[@name=\"in\"]",myProcessSession(),0, INFINITE);
    Owned<IPropertyTree> root = conn->getRoot();
    Owned<IPropertyTreeIterator> it = root->getElements("File");
    ForEach(*it) {
        IPropertyTree &e=it->query();
        const char *dir = e.queryProp("@directory");
        StringBuffer ds(dir);
        ds.toLowerCase();
        if (strcmp(ds.str(),dir)!=0) {
            e.setProp("@directory",ds.str());
            printf("name = '%s' dir = '%s'\n",e.queryProp("@name"),e.queryProp("@directory"));
        }
    }
}

void cleanRootFiles()
{
    Owned<IRemoteConnection> conn = querySDS().connect("Files/Scope[@name=\".\"]",myProcessSession(),0, INFINITE);
    Owned<IPropertyTree> root = conn->getRoot();
    Owned<IPropertyTreeIterator> it = root->getElements("File");
    ForEach(*it) {
        IPropertyTree &e=it->query();
        const char *name = e.queryProp("@name");
        const char *wutail = strstr(name,"__w200");
        if (!wutail)
            wutail = strstr(name,"__W200");
        if (wutail) {
            IPropertyTree *attr = e.queryPropTree("Attr");
            if (attr) {
                const char *wuid = attr->queryProp("@workunit");
                if (wuid&&*wuid) {
                    StringBuffer path;
                    path.append("WorkUnits/").append(wuid);
                    Owned<IRemoteConnection> wuconn = querySDS().connect(path.str(), myProcessSession(), 0, 5*60*1000);  
                    if (!wuconn) {
                        __int64 size = attr->getPropInt64("@size",-1);
                        PROGLOG("Orphan %s - size %"I64F"d",name,size);
                        if (size==0) {
                            bool done=false;
                            try {
                                queryDistributedFileDirectory().removePhysical(name);
                                done = true;
                            }
                            catch (IException *e) {
                                EXCLOG(e,"Deleting");
                                e->Release();
                            }
                            if (!done) {
                                try {
                                    queryDistributedFileDirectory().removeEntry(name);
                                }
                                catch (IException *e) {
                                    EXCLOG(e,"RemoveEntry");
                                    e->Release();
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

void printNonZeroReplicateOffsets()
{
    class cfilescan3 : public CSDSFileScanner
    {

        void processFile(IPropertyTree &file,StringBuffer &name)
        {
            if (file.getPropInt("@replicateOffset",-1)==0)
                file.removeProp("@replicateOffset");
        }

    } filescan;

    Owned<IRemoteConnection> conn = querySDS().connect("/Files", myProcessSession(), 0, 100000);
    filescan.scan(conn);
}



bool xlatNode(const char *xlat,const char *node,StringBuffer &res)
{
    if (!node||!*node||!isdigit(*node))
        return false;
    const char *s = xlat;
    const char *n;
    loop {
        n = node;
        while (*n&&(*s==*n)) {
            s++;
            n++;
        }
        if ((*s=='=')&&((*(s-1)=='.')||(*n=='.')))
            break;
        while (*s!=',') {
            if (*s==0)
                return false;
            s++;
        }
        s++;
    }
    s++;
    while (*s&&(*s!=',')) 
        res.append(*(s++));
    res.append(n);
    s = strchr(res.str(),'.');
    s=s?strchr(s+1,'.'):NULL;
    s=s?strchr(s+1,'.'):NULL;
    return (s&&(atoi(s+1)<=200));   // check for <= 200
}

void findAnonymousClusters(bool fix)
{
    class cfilescan3 : public CSDSFileScanner
    {
        StringArray usedgroups;

        void processFile(IPropertyTree &file,StringBuffer &name)
        {
            PROGLOG("process file %s",name.str());
            StringArray groups;
            getFileGroups(&file,groups);
            unsigned nc = file.getPropInt("@numclusters");
            if (nc&&(nc!=groups.ordinality()))
                ERRLOG("%s groups/numclusters mismatch",name.str());
            ForEachItemIn(i,groups) {
                const char *group = groups.item(i);
                bool mixed = false;
                const char *s = group;
                while (*s) {
                    if ((*s>='A')&&(*s<='Z')) {
                        mixed = true;
                        break;
                    }
                    s++;
                }
                    
                if (isAnonCluster(group)||mixed) {
                    if (mixed)
                        WARNLOG("%s has mixedcase cluster %s",name.str(),group);
                    else
                        WARNLOG("%s has anonymous cluster %s",name.str(),group);
                }
                bool found = false;
                ForEachItemIn(j,usedgroups) 
                    if (strcmp(group,usedgroups.item(j))==0) {
                        found = true;
                        break;
                    }
                if (!found)
                    usedgroups.append(group);
            }
        }
    public:
        void fixgroups(bool fix)
        {
            StringArray unusedgrps;
            StringArray toremove;
            Owned<INamedGroupIterator> giter = queryNamedGroupStore().getIterator();
            StringBuffer group;
            ForEach(*giter) {
                giter->get(group.clear());
                bool found = false;
                ForEachItemIn(j,usedgroups) 
                    if (strcmp(group.str(),usedgroups.item(j))==0) {
                        found = true;
                        break;
                    }
                if (!found)
                    unusedgrps.append(group.str());
            }
            ForEachItemIn(i,unusedgrps) {
                const char *grp = unusedgrps.item(i);
                PROGLOG("Unused: %s",grp);
                if (fix&&(memcmp(grp,"__",2)==0)) 
                    toremove.append(grp);
            }
            giter.clear();
            ForEachItemIn(i2,toremove) {
                queryNamedGroupStore().remove(toremove.item(i2));
                PROGLOG("Removed: %s",toremove.item(i2));
            }
        }
        
    } filescan;

    Owned<IRemoteConnection> conn = querySDS().connect("/Files", myProcessSession(), 0, 100000);
    unsigned start = msTick();
    filescan.scan(conn);
    printf("Time taken = %d\n",msTick()-start);
    filescan.fixgroups(fix);
}

void changeClusterName(const char *oldname,const char *newname)
{
    class cfilescan4 : public CSDSFileScanner
    {

        bool isIP(const char *name,IpAddress &ip,const char *fname)
        {
            // all . : or digit
            const char *s = name;
            if (!s||!*s)
                return false;
            bool failed = false;
            while (*s) {
                if ((*s==',')||(*s=='-'))
                    failed = true;
                if (!isdigit(*s)&&(*s!='.')&&(*s!=':'))
                    return false;
                s++;
            }
            if (failed) {
                WARNLOG("IP range in file %s",fname);
                return false;
            }
            return ip.ipset(name);
        }

        void fixFile(IPropertyTree &file,StringBuffer &name)
        {
            StringArray groups;
            getFileGroups(&file,groups,true);
            bool fixneeded = false;
            SocketEndpoint ip;
            ForEachItemIn(i1,groups) {
                const char *group = groups.item(i1);
                if ((memcmp(oldname,group,oldlen)==0)&&((group[oldlen]==0)||(group[oldlen]=='['))) {
                    fixneeded = true;
                    break;
                }
                if (isIP(group,ip,name.str())) {
                    if (oldgroup->rank(ip)!=RANK_NULL) {
                        fixneeded = true;
                    }
                    break;
                }
            }
            if (fixneeded) {
                StringBuffer tmp;
                if (!newname||!*newname)
                    PROGLOG("Fix needed for %s%s",name.str(),groups.ordinality()>1?" MULTI":"");
                else {
                    StringBuffer s;
                    ForEachItemIn(i2,groups) {
                        const char *group = groups.item(i2);
                        if (memcmp(oldname,group,oldlen)==0) {
                            if (group[oldlen]==0)
                                group = newname;
                            else if (group[oldlen]=='[')
                                group = tmp.clear().append(newname).append(group+oldlen).str();
                        }
                        else if (isIP(group,ip,name.str())) {
                            rank_t nr = oldgroup->rank(ip);
                            if ((nr!=RANK_NULL)&&((unsigned)nr<newgroup->ordinality()))
                                group = newgroup->queryNode(nr).endpoint().getIpText(tmp.clear());
                        }
                        if (s.length())
                            s.append(',');
                        s.append(group);
                    }
                    if (s.length())
                        file.setProp("@group",s.str());
                    unsigned sanity = 100;
                    loop {
                        Owned<IPropertyTreeIterator> iter = file.getElements("Cluster");
                        bool nochange = true;
                        ForEach(*iter) {
                            const char *gn = iter->query().queryProp("@name");
                            if (memcmp(oldname,gn,oldlen)==0) {
                                if (gn[oldlen]==0) {
                                    iter->query().setProp("@name",newname);
                                    nochange = false;
                                    break;
                                }
                                else if (gn[oldlen]=='[') {
                                    iter->query().setProp("@name",tmp.clear().append(newname).append(gn+oldlen).str());
                                    nochange = false;
                                    break;
                                }
                            }
                            else if (isIP(gn,ip,name.str())) {
                                rank_t nr = oldgroup->rank(ip);
                                if ((nr!=RANK_NULL)&&((unsigned)nr<newgroup->ordinality())) {
                                    iter->query().setProp("@name",newgroup->queryNode(nr).endpoint().getIpText(tmp.clear()).str());
                                    nochange = false;
                                    break;
                                }
                            }
                        }
                        if (nochange)
                            break;
                        if (sanity--==0) {
                            ERRLOG("LOOP setting cluster!!");
                            break;
                        }
                        StringBuffer gn;
                        if (iter->query().getProp("Group",gn)&&gn.length()) 
                            WARNLOG("Group %s for %s",gn.str(),name.str());

                    }
                    conn->commit();
                    PROGLOG("Fixed %s",name.str());
                }
            }
        }

        void processFile(IPropertyTree &file,StringBuffer &name)
        {
            printf("process file %s\n",name.str());
            fixFile(file,name);
        }
        //void processSuperFile(IPropertyTree &file,StringBuffer &name)
        //{
        //  printf("process superfile %s\n",name.str());
        //  fixFile(file,name);
        //}
    public:
        const char *oldname;
        size32_t oldlen;
        const char *newname;
        IRemoteConnection *conn;
        Owned<IGroup> oldgroup;
        Owned<IGroup> newgroup;
    } filescan;
    filescan.oldgroup.setown(queryNamedGroupStore().lookup(oldname));
    if (!filescan.oldgroup) {
        ERRLOG("Cannot find group %s\n",oldname);
        return;
    }
    filescan.oldname = oldname;
    if (newname&&*newname) {
        filescan.newgroup.setown(queryNamedGroupStore().lookup(newname));
        if (!filescan.newgroup) {
            ERRLOG("Cannot find new group %s\n",newname);
            return;
        }
    }
    filescan.newname = newname;
    filescan.oldlen = strlen(oldname);
    Owned<IRemoteConnection> conn = querySDS().connect("/Files", myProcessSession(), 0, 100000);
    filescan.conn = conn;
    unsigned start = msTick();
    filescan.scan(conn,true,false);
    printf("Time taken = %d\n",msTick()-start);
}


void compressParts()
{
    class cfilescan5 : public CSDSFileScanner
    {

        void processFile(IPropertyTree &file,StringBuffer &name)
        {
            printf("process file %s\n",name.str());
            if (file.hasProp("Part[2]")) {
                CDateTime dt;
                StringBuffer s;
                if (file.getProp("@modified",s)) {
                    dt.setString(s.str());
                    if (!dt.isNull()) {
                        unsigned year;
                        unsigned month; 
                        unsigned day;
                        dt.getDate(year, month, day);
                        if (year<2008) {
                            PROGLOG("Shrinking %s",name.str());
                            shrinkFileTree(&file);
                            conn->commit();
                        }
                    }
                }
            }
        }
        //void processSuperFile(IPropertyTree &file,StringBuffer &name)
        //{
        //  printf("process superfile %s\n",name.str());
        //  fixFile(file,name);
        //}
    public:
        IRemoteConnection *conn;
    } filescan;
    Owned<IRemoteConnection> conn = querySDS().connect("/Files", myProcessSession(), 0, 100000);
    filescan.conn = conn;
    unsigned start = msTick();
    filescan.scan(conn,true,false);
    printf("Time taken = %d\n",msTick()-start);
}





void changeSubnetInGroups(const char *xlat)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Groups", myProcessSession(), RTM_LOCK_WRITE, 100000);
    IPropertyTree *root = conn->queryRoot();
    Owned<IPropertyTreeIterator> giter = root->getElements("Group");
    StringBuffer map;
    ForEach(*giter) {
        const char *gname = giter->query().queryProp("@name");
        if (!gname)
            continue;
        printf("Process group: %s\n",gname);
        Owned<IPropertyTreeIterator> iter = giter->query().getElements("Node");
        bool changed = false;
        ForEach(*iter) {
            IPropertyTree &item = iter->query();
            if (xlatNode(xlat,item.queryProp("@ip"),map.clear())) {
                item.setProp("@ip",map.str());
                changed = true;
            }

        }
        if (changed)
            printf("Changed group: %s\n",gname);
    }
    conn->commit();
}

            
void nudge(const char *thorname,const char *masterip)
{
    Owned<INamedQueueConnection> conn = createNamedQueueConnection(0); // MORE - security token?
    Owned<IQueueChannel> channel = conn->open(thorname);
    MemoryBuffer mb;
    SocketEndpoint ep(masterip);
    ep.port = 7500;
    Owned<INode> n = createINode(ep);
    SessionId id = querySessionManager().lookupProcessSession(n);
    mb.append(id);
    ep.serialize(mb);
    channel->put(mb);
}
    
void persistsList(const char *grp)
{
    IDFAttributesIterator *iter = queryDistributedFileDirectory().getDFAttributesIterator("*",true,false);
    ForEach(*iter) {
        IPropertyTree &attr=iter->query();
        if ((!grp||!*grp)||(stricmp(grp,attr.queryProp("@group"))==0)) {  // TBD - Handling for multiple clusters?
            if (attr.getPropInt("@persistent",0)) {
                __int64 sz = attr.getPropInt64("@size",-1);
                if (sz>0) {
                    Owned<IDistributedFile> file=queryDistributedFileDirectory().lookup(attr.queryProp("@name"));
                    if (file) {
                        __int64 compressedsz = 0;
                        unsigned n=file->numParts();
                        for (unsigned i=0;i<n;i++) {
                            Owned<IDistributedFilePart> part = file->getPart(i);
                            __int64 csz = part->queryProperties().getPropInt64("@compressedSize",-1);
                            if (csz<0) {
                                compressedsz = -1;
                                break;
                            }
                            compressedsz+=csz;
                        }
                        printf("%s,%"I64F"d,%"I64F"d,%s,%s\n",attr.queryProp("@name"),compressedsz,sz,attr.queryProp("@modified"),attr.queryProp("@owner"));
                    }
                }
            }
        }
    }
}

void dfsGroups()
{
    IDFAttributesIterator *iter = queryDistributedFileDirectory().getDFAttributesIterator("*",true,false);
    ForEach(*iter) {
        IPropertyTree &attr=iter->query();
        printf("%s,%s\n",attr.queryProp("@name"),attr.queryProp("@group"));
    }
}

void dfsUnusedGroups()
{
    IDFAttributesIterator *iter = queryDistributedFileDirectory().getDFAttributesIterator("*",true,false);
    ForEach(*iter) {
        IPropertyTree &attr=iter->query();
        printf("%s,%s\n",attr.queryProp("@name"),attr.queryProp("@group"));
    }
}

typedef MapStringTo<bool> IsSuperFileMap;


void testSuperSubList()
{
    unsigned start = msTick();
    IsSuperFileMap supermap;
    IDFAttributesIterator *iter = queryDistributedFileDirectory().getDFAttributesIterator("*",true,true);
    if (iter) {
        ForEach(*iter) {
            IPropertyTree &attr=iter->query();
            if (attr.hasProp("@numsubfiles")) {
                const char *name=attr.queryProp("@name");
                if (name&&*name) {
                    if (!supermap.getValue(name)) 
                        supermap.setValue(name, true);                  
                }
            }
        }
    }

    StringArray out;
    StringBuffer tmp;
    HashIterator siter(supermap);
    ForEach(siter) {
        const char *supername = (const char *)siter.query().getKey();
        CDfsLogicalFileName lname;
        lname.set(supername);
        StringBuffer query;
        lname.makeFullnameQuery(query, DXB_SuperFile, true);
        Owned<IRemoteConnection> conn = querySDS().connect(query.str(),myProcessSession(),0, INFINITE);
        if (!conn) 
            throw MakeStringException(-1,"Could not connect to %s",lname.get());
        Owned<IPropertyTree> root = conn->getRoot();
        unsigned n=root->getPropInt("@numsubfiles");
        StringBuffer path;
        StringBuffer subname;
        unsigned subnum = 0;
        for (unsigned si=0;si<n;si++) {
            IPropertyTree *sub = root->queryPropTree(path.clear().appendf("SubFile[@num=\"%d\"]",si+1).str());
            if (sub) {
                const char *subname = sub->queryProp("@name");
                if (subname&&*subname) {
                    if (!supermap.getValue(subname)) {
                        tmp.clear().appendf("%s,%s\n",supername,subname);
                        out.append(tmp.str());
                        //printf("%s,%s\n",supername,subname);
                    }
                }
            }
        }
    }
    unsigned end = msTick();
    ForEachItemIn(i,out) {
        printf("%s",out.item(i));
    }
    printf("Time taken = %d",msTick()-start);
}



class CCrcElement : public CInterface
{
public:
    StringAttr filename;
    __int64 size;
    unsigned crc;
    CCrcElement(const char *line)
    {
        // File \ecl\aim_mail_center_clean.d00._1_of_400 size 13432 crc 0xa7cf2ae7 (2815372007)
        crc = 0;
        size = 0;
        if (memcmp(line,"file ",5)==0) {
            line += 5;
            const char * s = strstr(line," size ");
            if (s) {
                StringBuffer lfn(s-line,line);
                lfn.toLowerCase();
                filename.set(lfn);
                line = s+6;
                s = line;
                while (isdigit(*s))
                    s++;
                size = atoi64_l(line,s-line);
                while (*s&&(*s!='('))
                    s++;
                if (*s=='(') {
                    line = s+1;
                    while (*s&&(*s!=')'))
                        s++;
                    crc = (unsigned)atoi64_l(line,s-line);
                    printf("ADD: %s, %"I64F"d, %d\n",filename.get(),size,crc);
                }
            }
        }
    }
    const char *queryFindString() const { return filename.get(); }
};


OwningStringSuperHashTableOf<CCrcElement> crclist;

void load(const char *filename)
{
    Owned<IFile> infile = createIFile(filename);
    Owned<IFileIO> infileio = infile->open(IFOread);
    size32_t filesize = infileio->size();
    char *buf = new char[filesize];
    infileio->read(0,filesize,buf);
    StringBuffer line;
    for (unsigned i=0;i<filesize;) {
        char c=tolower(buf[i++]);
        if (c=='\r') {
            if ((i<filesize)&&(buf[i]=='\n')) 
                i++;
            c = '\n';
        }
        if (c=='\n') {
            if (line.length()) {
                crclist.add(* new CCrcElement(line));
                line.clear();
            }
        }
        else
            line.append(c);
    }
    if (line.length()) {
        crclist.add(* new CCrcElement(line));
    }
    delete[] buf;
}

void crcList(const char *grp,const char *filename)
{

    OwningStringSuperHashTableOf<CCrcElement> crctable;
    Owned<IFile> infile = createIFile(filename);
    Owned<IFileIO> infileio = infile->open(IFOread);
    size32_t filesize = infileio->size();
    char *buf = new char[filesize];
    infileio->read(0,filesize,buf);
    StringBuffer line;
    for (unsigned i=0;i<filesize;) {
        char c=tolower(buf[i++]);
        if (c=='\r') {
            if ((i<filesize)&&(buf[i]=='\n')) 
                i++;
            c = '\n';
        }
        if (c=='\n') {
            if (line.length()) {
                crctable.add(* new CCrcElement(line));
                line.clear();
            }
        }
        else
            line.append(c);
    }
    if (line.length()) {
        crctable.add(* new CCrcElement(line));
    }
    delete[] buf;

    IDFAttributesIterator *iter = queryDistributedFileDirectory().getDFAttributesIterator("*",true,false);
    if (iter) {
        if (iter->first()) {
            StringBuffer lpath;
            do {
                IPropertyTree &attr=iter->query();
                const char *grpa = attr.queryProp("@group");            // TBD - Handling for multiple clusters?
                if ((!grp||!*grp)||(grpa&&(stricmp(grp,grpa)==0))) {
                    const char *name=attr.queryProp("@name");
                    Owned<IDistributedFile> file=queryDistributedFileDirectory().lookup(name);
                    if (file) {
                        __int64 compressedsz = 0;
                        unsigned n=file->numParts();
                        unsigned missing=0;
                        unsigned mismatch = 0;
                        StringBuffer notfound;
                        for (unsigned i=0;i<n;i++) {
                            Owned<IDistributedFilePart> part = file->getPart(i);
                            RemoteFilename rfn;
                            part->getFilename(rfn).getLocalPath(lpath.clear());
                            const char *fn = lpath.str();
                            if (*fn&&(fn[1]==':')) 
                                fn += 2;
                            StringBuffer lfn(fn);
                            lfn.toLowerCase();
                            __int64 sz = part->queryProperties().getPropInt64("@size",-1);
                            unsigned crc;
                            bool hasCrc = part->getCrc(crc);
                            //printf("File %s size %"I64F"d crc 0x%x (%u)\n",fn,sz,crc,crc);
                            CCrcElement *e = crctable.find(lfn.str());
                            if (e) {
                                if (e->size!=sz) 
                                    printf("SIZE MISMATCH %s: %"I64F"d  %"I64F"d\n",lfn.str(),sz,e->size);
                                else if (hasCrc) {
                                    if (e->crc!=crc) {
                                        printf("CRC MISMATCH %s: %u  %u\n",lfn.str(),crc,e->crc);
                                        mismatch++;
                                    }
                                }
                                else {
#if 0
                                    IPropertyTree & up = part->lockProperties();
                                    up.setPropInt("@fileCrc",(int)e->crc);
                                    part->unlockProperties();
#endif
                                    missing++;
                                }
                            }
                            else if (!notfound.length())
                                notfound.append(lfn.str());
                        }
                        if (missing)
                            printf("NO CRC %s: %u parts\n",name,missing);
                        else if (notfound.length())
                            printf("NOT FOUND %s %s\n",name,notfound.str());
                        else if (mismatch==0)
                            printf("OK %s\n", name);

                    }
                }
            } while (iter->next());
        }
    }
}

class CIpItem: public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    IpAddress ip;
    bool ok;
};

class CIpTable: public SuperHashTableOf<CIpItem,IpAddress>
{


public:
    ~CIpTable()
    {
        releaseAll();
    }

    void onAdd(void *)
    {
        // not used
    }

    void onRemove(void *e)
    {
        CIpItem &elem=*(CIpItem *)e;        
        elem.Release();
    }

    unsigned getHashFromElement(const void *e) const
    {
        const CIpItem &elem=*(const CIpItem *)e;        
        return elem.ip.iphash();
    }

    unsigned getHashFromFindParam(const void *fp) const
    {
        return ((const IpAddress *)fp)->iphash();
    }

    const void * getFindParam(const void *p) const
    {
        const CIpItem &elem=*(const CIpItem *)p;        
        return &elem.ip;
    }

    bool matchesFindParam(const void * et, const void *fp, unsigned) const
    {
        return ((CIpItem *)et)->ip.ipequals(*(IpAddress *)fp);
    }

    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(CIpItem,IpAddress);

    bool verifyDaliFileServer(IpAddress &ip)
    {
        CIpItem *item=find(ip);
        if (!item) {
            item = new CIpItem;
            item->ip.ipset(ip);
            item->ok = testDaliServixPresent(ip);
            add(*item);
        }
        return item->ok;
    }

};

class CFileCrcItem: public CInterface
{
public:
    RemoteFilename filename;
    unsigned requiredcrc;
    unsigned crc;
    unsigned partno;
    unsigned copy;
    IMPLEMENT_IINTERFACE;
    bool ok;
    byte flags;
    CDateTime dt;
};

#define FLAG_ROW_COMPRESSED 1
#define FLAG_NO_CRC 2

class CFileList: public CIArrayOf<CFileCrcItem> 
{

public:
    void add(RemoteFilename &filename,unsigned partno,unsigned copy,unsigned crc,byte flags)
    {
        CFileCrcItem *item = new CFileCrcItem();
        item->filename.set(filename);
        item->partno = partno;
        item->copy = copy;
        item->crc = crc;
        item->requiredcrc = crc;
        item->flags = flags;
        append(*item);  
    }


};


void verifyFile(FILE *outf,const char *name,CDateTime *cutoff)
{
    static CIpTable dafilesrvips;
    Owned<IDistributedFile> file=queryDistributedFileDirectory().lookup(name);
    if (!file) {
        PROGLOG("VERIFY: cannot find %s",name);
        return;
    }
    CDateTime filetime;
    if (file->getModificationTime(filetime)) {
        if (cutoff&&(filetime.compare(*cutoff)<=0))
            return;
    }

    IPropertyTree &fileprops = file->queryProperties();
    bool blocked;
    bool rowcompressed = file->isCompressed(&blocked)&&!blocked;
    CFileList list;
    unsigned width = file->numParts();
    unsigned short port = getDaliServixPort();
    try {
        for (unsigned i=0;i<width;i++) {
            Owned<IDistributedFilePart> part = file->getPart(i);
            for (unsigned copy = 0; copy < part->numCopies(); copy++) {
                unsigned reqcrc;
                bool noreq = !part->getCrc(reqcrc);
//              if (reqcrc==(unsigned)-1)
//                  continue;
                SocketEndpoint ep(part->queryNode()->endpoint()); 
                if (!dafilesrvips.verifyDaliFileServer(ep)) {
                    StringBuffer ips;
                    ep.getIpText(ips);
                    PROGLOG("VERIFY: file %s, cannot run DAFILESRV on %s",name,ips.str());
                    return;
                }
                RemoteFilename rfn;
                part->getFilename(rfn,copy);
                rfn.setPort(port); 
                list.add(rfn,i,copy,reqcrc,rowcompressed?FLAG_ROW_COMPRESSED:(noreq?FLAG_NO_CRC:0));
            }
        }
    }
    catch (IException *e)
    {
        StringBuffer s;
        s.appendf("VERIFY: file %s",name);
        EXCLOG(e, s.str());
        e->Release();
        return;
    }
    if (list.ordinality()==0)
        return;
    PROGLOG("VERIFY: start file %s",name);
    file.clear();
    CriticalSection crit;
    class casyncfor: public CAsyncFor
    {
        CFileList  &list;
        CriticalSection &crit;
    public:
        bool ok;
        casyncfor(CFileList  &_list, CriticalSection &_crit)
            : list(_list), crit(_crit)
        {
            ok = true;
        }
        void Do(unsigned i)
        {
            CriticalBlock block(crit);
            CFileCrcItem &item = list.item(i);
            RemoteFilename &rfn = item.filename;
            Owned<IFile> partfile;
            StringBuffer eps;
            try
            {
                partfile.setown(createIFile(rfn));
                // PROGLOG("VERIFY: part %s on %s",partfile->queryFilename(),rfn.queryEndpoint().getUrlStr(eps).str());
                if (partfile) {
                    CriticalUnblock unblock(crit);
                    item.crc = partfile->getCRC();
                    partfile->getTime(NULL,&item.dt,NULL);
                    if ((item.crc==0)&&!partfile->exists()) {
                        PROGLOG("VERIFY: does not exist part %s on %s",partfile->queryFilename(),rfn.queryEndpoint().getUrlStr(eps).str());
                        ok = false;
                    }
                }
                else
                    ok = false;

            }
            catch (IException *e)
            {
                StringBuffer s;
                s.appendf("VERIFY: part %s on %s",partfile->queryFilename(),rfn.queryEndpoint().getUrlStr(eps).str());
                EXCLOG(e, s.str());
                e->Release();
                ok = false;
            }
        }
    } afor(list,crit);
    afor.For(list.ordinality(),400,false,true);
    StringBuffer outs;
    ForEachItemIn(j,list) {
        CFileCrcItem &item = list.item(j);
        item.filename.setPort(0);
        if (outf) {
            StringBuffer rfs;
            const char *text;
            switch (item.flags) {
            case FLAG_ROW_COMPRESSED: text = "compressed"; break;
            case FLAG_NO_CRC: text = "unset"; break;
            default: text = (item.crc==item.requiredcrc)?"ok":"fail";
            }
            StringBuffer lname(name);
            lname.appendf("[%d%s]",item.partno+1,item.copy?":R":"");
            StringBuffer dates;
            item.dt.getString(dates);
            const char *ds=strchr(dates.str(),'.');
            if (ds)
                dates.setLength(ds-dates.str());
            fprintf(outf,"%s,%s,%x,%x,%s,%s\n",lname.str(),item.filename.getRemotePath(rfs).str(),item.crc,item.requiredcrc,dates.str(),text);
        }
        if (item.crc!=item.requiredcrc) {
            StringBuffer rfs;
            if (!outf)
                PROGLOG("VERIFY: FAILED %s (%x,%x) file %s",name,item.crc,item.requiredcrc,item.filename.getRemotePath(rfs).str());
            afor.ok = false;
        }
    }
    if (afor.ok) 
        PROGLOG("VERIFY: OK file %s",name);
    if (outf)
        fflush(outf);
}

void verifyFiles(const char *grp,const char *fname,unsigned days)
{
    CDateTime cutoff;
    if (days) {
        cutoff.setNow();
        cutoff.adjustTime(-60*24*days);
    }

    FILE* out = fopen(fname, "wb+");
    IDFAttributesIterator *iter = queryDistributedFileDirectory().getDFAttributesIterator("*",true,false);
    ForEach(*iter) {
        StringBuffer lpath;
        IPropertyTree &attr=iter->query();
        const char *name=attr.queryProp("@name");
        const char *g=attr.queryProp("@group");
        if ((strcmp("*",grp)==0)||(g&&(strcmp(g,grp)==0)))  // TBD - Handling for multiple clusters?
            verifyFile(out,name,days?&cutoff:NULL);
    }
    fclose(out);
}


inline bool getfld(const char *&s,StringBuffer &out)
{
    while (*s&&(*s!=','))
        out.append(*(s++));
    if (*s)
        s++;
    return out.length()!=0;
}




void fillSDS()
{
    Owned<IRemoteConnection> conn = querySDS().connect("Testing",myProcessSession(),RTM_LOCK_WRITE|RTM_CREATE, INFINITE);
    if (!conn) {
        DBGLOG("Could not connect to %s","Testing");
        return;
    }
    unsigned n=0;
    loop {
        Owned<IPropertyTree> root = conn->getRoot();
        StringBuffer tail("T");
        tail.append(getRandom());
        tail.append("TESTBRANCH");
        tail.append(++n);
        StringBuffer val;
        for (unsigned i=0;i<1000;i++)
            val.append((char)getRandom()%26+'A');
        root->addProp(tail.str(),val.str());
        if (n%100==0)
            conn->commit();
    }
    conn->close();
}

void dumpQueues()
{
    Owned<IRemoteConnection> conn = querySDS().connect("Queues",myProcessSession(),0, INFINITE);
    Owned<IPropertyTree> root = conn->getRoot();
    Owned<IPropertyTreeIterator> it = root->getElements("Queue");
    ForEach(*it) {
        IPropertyTree &e=it->query();
        printf("Queue[%s] count=%d\n",e.queryProp("@name"),e.getPropInt("@count"));
        unsigned i=0;
        Owned<IPropertyTreeIterator> it2 = e.getElements("Item");
        ForEach(*it2) {
            IPropertyTree &e2=it2->query();
            StringBuffer s(e2.queryProp(NULL));
            if (s.str()[0]=='X') {
                StringBuffer sd;
                decrypt(sd,s.str()+1);
                s.clear().append(sd);
            }
            //MemoryBuffer mb;
            //e2.getPropBin(NULL,mb);
            printf("  %-2d: %s\n",++i,s.str());
        }
    }
}


int dfsPerms(const char *obj,const char *username,const char *pword)
{
    const char *s=obj;
    Owned<IUserDescriptor> user = createUserDescriptor();
    user->set(username,pword);
    int perm =0;
    if (strchr(obj,'\\')||strchr(obj,'/')) {
        Owned<IFileDescriptor> fd = createFileDescriptor();
        RemoteFilename rfn;
        rfn.setRemotePath(obj);
        fd->setPart(0, rfn);
        perm = queryDistributedFileDirectory().getFDescPermissions(fd,user,0);
    }
    else {
        perm = queryDistributedFileDirectory().getFilePermissions(obj,user,0);
    }
    printf("perm %s = %d\n",obj,perm);
    return perm;
}

static const char *getNum(const char *s,unsigned &num)
{
    while (*s&&!isdigit(*s))
        s++;
    num = 0;
    while (isdigit(*s)) {
        num = num*10+*s-'0';
        s++;
    }
    return s;
}


void DumpWorkunitTimings(IPropertyTree *wu)
{
    Owned<IFile> file;
    Owned<IFileIO> fileio;
    offset_t filepos;
    const char *basename = "DaAudit.";
    StringBuffer curfilename;
    StringBuffer wuid;
    wu->getName(wuid);
    StringBuffer query;
    StringBuffer sdate;
    StringBuffer name;
    CDateTime dt;
    StringBuffer line;
    const char *submitid = wu->queryProp("@submitID");
    if (!submitid)
        submitid = "";
    const char *cluster = wu->queryProp("@cluster");
    if (!cluster)
        cluster = "";
    Owned<IPropertyTreeIterator> iter = wu->getElements("Timings/Timing");
    ForEach(*iter) {
        if (iter->query().getProp("@name",name.clear())) {
            if ((name.length()>11)&&(memcmp("Graph graph",name.str(),11)==0)) {
                unsigned gn;
                const char *s = getNum(name.str(),gn);
                unsigned sn;
                s = getNum(s,sn);
                if (gn&&sn) {
                    query.clear().appendf("TimeStamps/TimeStamp[@application=\"Thor - graph%d\"]/Started[1]",gn);
                    Owned<IPropertyTreeIterator> iter2 = wu->getElements(query.str());
                    ForEach(*iter2) {
                        if (iter2->query().getProp(NULL,sdate.clear())) {
                            dt.setString(sdate.str());
                            unsigned year;
                            unsigned month;
                            unsigned day;
                            dt.getDate(year,month,day);
                            StringBuffer logname(basename);
                            logname.appendf("%02d_%02d_%02d.log",month,day,year%100);
                            if (strcmp(logname.str(),curfilename.str())!=0) {
                                fileio.clear();
                                file.setown(createIFile(logname.str()));
                                if (!file) 
                                    throw MakeStringException(-1,"Could not create file %s",logname.str());
                                fileio.setown(file->open(IFOwrite));
                                if (!fileio) 
                                    throw MakeStringException(-1,"Could not open file %s",logname.str());
                                filepos = fileio->size();
                                curfilename.clear().append(logname);
                            }
                            dt.getDateString(line.clear());
                            line.append(' ');
                            dt.getTimeString(line);
                            line.append(" ,Timing,ThorGraph").append(cluster).append(',').append(wuid.str()).append(',').append(gn).append(',').append(sn).append(",1,");
                            iter->query().getProp("@duration",line);
                            line.append('\n');
                            fileio->write(filepos,line.length(),line.str());
                            filepos += line.length();
                            break; // only do first
                        }
                    }
                }
            }
        }
    }
}

bool getResponse()
{
    int ch;
    do
    {
        ch = toupper(ch = _getch());
    } while (ch != 'Y' && ch != 'N');
    _putch(ch);
    _putch('\n');
    return ch=='Y' ? true : false;
}

bool doFix()
{
    printf("Fix? (Y/N):");
    return getResponse();
}

void checkSuperFile(const char *lfn,bool fix=true)
{
#ifndef _WIN32
    PROGLOG("checkSuperFile not supported on linux");
#else
    if (strcmp(lfn,"*")==0) {
        class csuperfilescan: public CSDSFileScanner
        {

            virtual bool checkScopeOk(const char *scopename)
            {
                PROGLOG("Processing scope %s",scopename);
                return true;
            }

            void processSuperFile(IPropertyTree &superfile,StringBuffer &name)
            {
                try {
                    checkSuperFile(name.str(),fix);
                }
                catch (IException *e) {
                    EXCLOG(e,"processSuperFiles");
                    e->Release();
                }
            }

        public:
            bool fix;

        } superfilescan;
        superfilescan.fix = fix;

        Owned<IRemoteConnection> conn = querySDS().connect("/Files", myProcessSession(), 0, 100000);
        superfilescan.scan(conn,false,true);
        return;
    }
    bool fixed = false;
    CDfsLogicalFileName lname;
    lname.set(lfn);
    StringBuffer query;
    lname.makeFullnameQuery(query, DXB_SuperFile, true);
    Owned<IRemoteConnection> conn = querySDS().connect(query.str(),myProcessSession(),fix?RTM_LOCK_WRITE:0, INFINITE);
    if (!conn) {
        ERRLOG("Could not connect to %s",lfn);
        PROGLOG("Superfile %s FAILED",lname.get());
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    unsigned n=root->getPropInt("@numsubfiles");
    StringBuffer path;
    StringBuffer subname;
    unsigned subnum = 0;
    unsigned i;
    for (i=0;i<n;i++) {
        loop {
            IPropertyTree *sub2 = root->queryPropTree(path.clear().appendf("SubFile[@num=\"%d\"][2]",i+1).str());
            if (!sub2)
                break;
            StringBuffer s;
            s.appendf("SuperFile %s: corrupt, subfile file part %d is duplicated",lname.get(),i+1);
            ERRLOG("%s",s.str());
            if (!fix||!doFix()) {
                PROGLOG("Superfile %s FAILED",lname.get());
                return;
            }
            root->removeProp(path.str());
        }
        IPropertyTree *sub = root->queryPropTree(path.clear().appendf("SubFile[@num=\"%d\"]",i+1).str());
        if (!sub) {
            StringBuffer s;
            s.appendf("SuperFile %s: corrupt, subfile file part %d cannot be found",lname.get(),i+1);
            ERRLOG("%s",s.str());
            if (!fix||!doFix()) {
                PROGLOG("Superfile %s FAILED",lname.get());
                return;
            }
            fixed = true;
            break;
        }
        sub->getProp("@name",subname.clear());
        CDfsLogicalFileName sublname;
        sublname.set(subname.str());
        if (!sublname.isExternal()&&!sublname.isForeign()) {
            StringBuffer subquery;
            sublname.makeFullnameQuery(subquery, DXB_File, true);
            Owned<IRemoteConnection> subconn = querySDS().connect(subquery.str(),myProcessSession(),fix?RTM_LOCK_WRITE:0, INFINITE);
            if (!subconn) {
                sublname.makeFullnameQuery(subquery.clear(), DXB_SuperFile, true);
                subconn.setown(querySDS().connect(subquery.str(),myProcessSession(),0, INFINITE));
            }
            if (!subconn) {
                ERRLOG("SuperFile %s is missing sub-file file %s",lname.get(),subname.str());
                if (!fix||!doFix()) {
                    PROGLOG("Superfile %s FAILED",lname.get());
                    return;
                }
                root->removeTree(sub);
                for (unsigned j=i+1;j<n; j++) {
                    sub = root->queryPropTree(path.clear().appendf("SubFile[@num=\"%d\"]",j+1).str());
                    if (sub)
                        sub->setPropInt("@num",j);
                }
                i--;
                n--;
                fixed = true;
                continue;
            }
            subnum++;

            Owned<IPropertyTree> subroot = subconn->getRoot();
            Owned<IPropertyTreeIterator> iter = subroot->getElements("SuperOwner");
            StringBuffer pname;
            bool parentok=false;
            ForEach(*iter) {
                iter->query().getProp("@name",pname.clear());
                if (strcmp(pname.str(),lname.get())==0) 
                    parentok = true;
                else {
                    CDfsLogicalFileName sdlname;
                    sdlname.set(pname.str());
                    StringBuffer sdquery;
                    sdlname.makeFullnameQuery(sdquery, DXB_SuperFile, true);
                    Owned<IRemoteConnection> sdconn = querySDS().connect(sdquery.str(),myProcessSession(),0, INFINITE);
                    if (!conn) {
                        WARNLOG("SubFile %s has missing owner superfile %s",sublname.get(),sdlname.get());
                    }
                    // make sure superfile exists
                }
            }
            if (!parentok) {
                WARNLOG("SubFile %s is missing link to Superfile %s",sublname.get(),lname.get());
                ForEach(*iter) {
                    iter->query().getProp("@name",pname.clear());
                    PROGLOG("Candidate %s",pname.str());
                }
            }
        }
        else
            subnum++;
    }
    if (fixed) 
        root->setPropInt("@numsubfiles",subnum);
    i = 0;
    byte fixstate = 0;
    loop {
        bool err = false;
        IPropertyTree *sub = root->queryPropTree(path.clear().appendf("SubFile[%d]",i+1).str());
        if (sub) {
            unsigned pn = sub->getPropInt("@num");
            if (pn>subnum) {
                PROGLOG("SuperFile %s: corrupt, subfile file part %d spurious",lname.get(),pn);
                if (fixstate==0)
                    if (fix&&doFix())
                        fixstate = 1;
                    else
                        fixstate = 2;
                if (fixstate==1) {
                    root->removeTree(sub);
                    fixed = true;
                    i--;
                }
            }
        }
        else
            break;
        i++;
    }
    if (n==0) {
        IPropertyTree *sub = root->queryPropTree("Attr");
        if (!isEmptyPTree(sub)&&!sub->queryProp("description")) {
            if (fix) {
                if (!fixed)
                    PROGLOG("FIX Empty Superfile %s contains non-empty Attr",lname.get());
                root->removeTree(sub);
            }
            else if (sub->getPropInt64("@recordCount")||sub->getPropInt64("@size"))
                PROGLOG("FAIL Empty Superfile %s contains non-empty Attr sz=%"I64F"d rc=%"I64F"d",lname.get(),sub->getPropInt64("@recordCount"),sub->getPropInt64("@size"));

        }
    }
    if (fixed) 
        PROGLOG("Superfile %s FIXED - from %d to %d subfiles",lname.get(),n,subnum);
    else
        PROGLOG("Superfile %s OK - contains %d subfiles",lname.get(),n);
#endif
}


void checkSubFile(const char *lfn,bool fix=true)
{
#ifndef _WIN32
    PROGLOG("checkSubFile not supported on linux");
#else
    if (strcmp(lfn,"*")==0) {
        class csubfilescan: public CSDSFileScanner
        {

            virtual bool checkFileOk(IPropertyTree &file,const char *filename)
            {
                return (file.hasProp("SuperOwner[1]"));
            }

            virtual bool checkSuperFileOk(IPropertyTree &file,const char *filename)
            {
                return (file.hasProp("SuperOwner[1]"));
            }

            virtual bool checkScopeOk(const char *scopename)
            {
                PROGLOG("Processing scope %s",scopename);
                return true;
            }

            void processFile(IPropertyTree &root,StringBuffer &name)
            {
                try {
                    checkSubFile(name.str(),false);
                }
                catch (IException *e) {
                    EXCLOG(e,"processSuperFiles");
                    e->Release();
                }
            }

            void processSuperFile(IPropertyTree &root,StringBuffer &name)
            {
                try {
                    checkSubFile(name.str(),false);
                }
                catch (IException *e) {
                    EXCLOG(e,"processSuperFiles");
                    e->Release();
                }
            }

        public:



        } subfilescan;

        Owned<IRemoteConnection> conn = querySDS().connect("/Files", myProcessSession(), 0, 100000);
        subfilescan.scan(conn,true,true);
        return;
    }
    bool fixed = false;
    CDfsLogicalFileName lname;
    lname.set(lfn);
    StringBuffer query;
    lname.makeFullnameQuery(query, DXB_File, true);
    Owned<IRemoteConnection> conn = querySDS().connect(query.str(),myProcessSession(),fix?RTM_LOCK_WRITE:0, INFINITE);
    if (!conn) {
        lname.makeFullnameQuery(query.clear(), DXB_SuperFile, true);
        conn.setown(querySDS().connect(query.str(),myProcessSession(),fix?RTM_LOCK_WRITE:0, INFINITE));
    }
    if (!conn) {
        ERRLOG("Could not connect to %s",lfn);
        PROGLOG("Subfile %s FAILED",lname.get());
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    Owned<IPropertyTreeIterator> iter = root->getElements("SuperOwner");
    StringBuffer pname;
    bool ok=true;
    ForEach(*iter) {
        iter->query().getProp("@name",pname.clear());
        CDfsLogicalFileName sdlname;
        sdlname.set(pname.str());
        StringBuffer sdquery;
        sdlname.makeFullnameQuery(sdquery, DXB_SuperFile, true);
        Owned<IRemoteConnection> sdconn = querySDS().connect(sdquery.str(),myProcessSession(),0, INFINITE);
        if (!conn) {
            ERRLOG("SubFile %s has missing owner superfile %s",lname.get(),sdlname.get());
            ok = false;
        }
    }
    if (ok)
        PROGLOG("SubFile %s OK",lname.get());
#endif
}

void pruneAttNode(const char *wuname,IPropertyTree &pt,unsigned &total,bool &dirty)
{
    StringBuffer s;
    Owned<IPropertyTreeIterator> iter = pt.getElements("att");  
    ForEach(*iter) {
        IPropertyTree &at=iter->query();
        const char *an = at.queryProp("@name");
        if (an&&(strcmp(an,"ecl")==0)) {
            if (at.getProp("@value",s.clear())) {
                size32_t l = s.length();
                if (l>1024) {
                    total += l-1024;
                    s.setLength(1021);
                    s.append("...");
                    at.setProp("@value",s.str());
                    PROGLOG("%s = %d %d",wuname,l,total);
                    dirty = true;
                }
            }
        }
        IPropertyTree *xt = at.queryPropTree("graph");
        if (xt) {
            Owned<IPropertyTreeIterator> xiter = xt->getElements("node");   
            ForEach(*xiter) 
                pruneAttNode(wuname,xiter->query(),total,dirty);
        }
    }
}

void pruneAttEcl()
{
    unsigned total = 0;
    Owned<IRemoteConnection> conn = querySDS().connect("/WorkUnits", myProcessSession(), 0, 100000);
    Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("*");
    bool started = false;
    ForEach(*iter) {
        IPropertyTree &wu=iter->query();
        if (&wu==NULL)
            continue;
        if (!started) {
            started = true; //stricmp(wu.queryName(),"W20050428-144951")==0;
            if (!started)
                continue;
        }
        Owned<IPropertyTreeIterator> giter = wu.getElements("Graphs/Graph");    
        bool dirty = false;
        ForEach(*giter) {
            IPropertyTree &gt = giter->query();;
            IPropertyTree *xt = gt.queryPropTree("xgmml/graph");
            Owned<IPropertyTreeIterator> xiter = xt->getElements("node");   
            ForEach(*xiter) 
                pruneAttNode(wu.queryName(),xiter->query(),total,dirty);
        }
        if (dirty)
            conn->commit();
    }
    PROGLOG("Total = %d",total);
}

void PrintWUsizes()
{
    unsigned total = 0;
    Owned<IRemoteConnection> conn = querySDS().connect("/WorkUnits", myProcessSession(), 0, 100000);
    Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("*");
    ForEach(*iter) {
        IPropertyTree &wu=iter->query();
        setAllocHook(true);
        IPropertyTree *wucopy=createPTree(&wu);
        unsigned tm = setAllocHook(true);
        wucopy->Release();
        tm -= setAllocHook(false);
        PROGLOG("%s,%d",wu.queryName(),tm);
        total += tm;
    }
    PROGLOG("Total = %d",total);
}

void fixCompSize(const char *lfn,IDistributedFile *file, unsigned trials=0)
{
    if (!isCompressed(file->queryProperties()))
        return;
    Owned<IDistributedFilePartIterator> pi = file->getIterator();
    unsigned pn=1;
    bool locked = false;
    __int64 total = 0;
    IPropertyTree *flockprop = NULL;
    ForEach(*pi) {
        Owned<IDistributedFilePart> part = &pi->get();
        IPropertyTree &prop = part->queryProperties();
        const char *s1 = prop.queryProp("@size");
        const char *s2 = prop.queryProp("@compressedSize");
        if (s1&&s2&&(strcmp(s1,s2)==0)) {
            if (!locked) {
                flockprop = &file->lockProperties();
                locked = true;
            }
            RemoteFilename rfn;
            Owned<IFile> f;
            Owned<IFileIO> fio;
            part->getFilename(rfn,0);
            f.setown(createDaliServixFile(rfn));
            if (f) 
                fio.setown(createCompressedFileReader(f));
            if (!fio) {
                part->getFilename(rfn,1);
                f.setown(createDaliServixFile(rfn));
                if (f) 
                    fio.setown(createCompressedFileReader(f));
            }
            if (!fio) {
                if (locked)
                    file->unlockProperties();
                StringBuffer url;
                rfn.getRemotePath(url);
                ERRLOG("Cannot fix %s[%d] %s",lfn,pn,url.str());
                return;
            }
            __int64 sz = fio->size();
            total += sz;
            IPropertyTree &lockprop = part->lockProperties();
            lockprop.setPropInt64("@size",sz);
            part->unlockProperties();
            //PROGLOG("Changing %s to %"I64F"d",s1,sz);
            trials = 9999;
        }
        else
            if (trials&&(pn>trials))
                break;
        pn++;
    }
    if (locked) {
        flockprop->setPropInt64("@size",total);
        file->unlockProperties();
        PROGLOG("Fixed %s",lfn);
    }
}

void fixCompSizes()
{
    Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator("*",true);
    ForEach(*iter) {
        IDistributedFile &file=iter->query();
        StringBuffer name;
        iter->getName(name);
        PROGLOG("Checking %s\n",name.str());
        fixCompSize(name.str(),&file,10);
    }
}

__int64 trueSuperSize(IDistributedSuperFile *sfile)
{
    Owned<IDistributedFileIterator> iter = sfile->getSubFileIterator(false);
    __int64 tsz = 0;
    ForEach(*iter) {
        IDistributedFile &file=iter->query();
        IDistributedSuperFile *sfile = file.querySuperFile();
        __int64 sz;
        if (sfile) 
            sz = trueSuperSize(sfile);
        else 
            sz = file.getFileSize(false,false);
        if (sz==-1) {
            tsz = -1;
            break;
        }
        tsz += sz;
    }
    return tsz;
}

bool testSuperSize(IDistributedFile &file,const char *name,bool allow)
{
    bool ok = true;
    IDistributedSuperFile *sfile = file.querySuperFile();
    if (sfile) {
        __int64 tsz = trueSuperSize(sfile);
        __int64 sz = file.getFileSize(allow,false);
        if (sz!=tsz) {
            ERRLOG("%s: Superfile Difference %"CF"d to %"CF"d",name,tsz,sz);
            ok = false;
        }
    }
    return ok;
}

void fixSuperSize(const char *name)
{
    PROGLOG("Fixing %s",name);
    CDfsLogicalFileName dlfn;   
    dlfn.set(name);
    StringBuffer query;
    dlfn.makeFullnameQuery(query,DXB_SuperFile,true);
    Owned<IRemoteConnection> fconn;
    try {
        fconn.setown(querySDS().connect(query.str(),queryCoven().inCoven()?0:myProcessSession(),RTM_LOCK_WRITE, 5*60*1000));
    }
    catch (IException *e) {
        StringBuffer err("fixSuperSizes ");
        err.append(query);
        EXCLOG(e, err.str());
        return;
    }
    if (fconn) {
        IPropertyTree *root = fconn->queryRoot();
        root->removeProp("Attr/@size");
        root->removeProp("Attr/@checkSum");
        root->removeProp("Attr/@recordCount");  // recordCount not currently supported by superfiles
    }
    fconn.clear();

}

void fixSuperSizes()
{
    Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator("*",true);
    unsigned n = 0;
    StringArray tofix;
    ForEach(*iter) {
        IDistributedFile &file=iter->query();
        StringBuffer name;
        iter->getName(name);
        if (!testSuperSize(file,name.str(),false)) 
            tofix.append(name.str());
        n++;
        if (n%100==0)
            PROGLOG("Checked %d files",n);          
    }
    iter.clear();
    PROGLOG("Fixing %d files",tofix.ordinality());
    ForEachItemIn(i,tofix)
        fixSuperSize(tofix.item(i));
    ForEachItemIn(j,tofix) {
        const char *name = tofix.item(j);
        Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(name);
        if (testSuperSize(*file,name,true))
            PROGLOG("Fixed %s OK",name);
        else
            PROGLOG("Fixed %s FAILED",name);
    }
}

void testSuperWidths()
{
    Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator("*",true);
    unsigned n = 0;
    StringArray tofix;
    ForEach(*iter) {
        IDistributedFile &file=iter->query();
        bool ok = true;
        IDistributedSuperFile *sfile = file.querySuperFile();
        if (sfile) {        
            StringBuffer name;
            iter->getName(name);
            unsigned width = 0;
            Owned<IDistributedFileIterator> iter = sfile->getSubFileIterator(false);
            ForEach(*iter) {
                IDistributedFile &file=iter->query();
                if (file.querySuperFile()==NULL) {
                    unsigned w = file.numParts();
                    if (width==0)
                        width=w;
                    else if (w!=width)
                        PROGLOG("Failed superfile %s widths %d and %d",name.str(),width,w);
                }
            }
        }
        n++;
        if (n%100==0)
            PROGLOG("Checked %d files",n);          
    }
}

void fixSuperSizeTest(const char *name)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(name);
    if (!testSuperSize(*file,name,false)) {
        file.clear();
        fixSuperSize(name);
    }
}

/*

void fixReplicated()
{
    UNIMPLEMENTED;
    Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator("*",true);
    ForEach(*iter) {
        try {
            IDistributedFile &file=iter->query();
            StringBuffer name;
            iter->getName(name);
            //PROGLOG("Checking %s\n",name.str());
            if (file.queryProperties().hasProp("@replicated")) {
                ERRLOG("%s has Attr/@replicated set",name.str());
                IPropertyTree &pt = file.lockProperties();
                if (pt.getPropBool("@replicated")) 
                    file.setReplicated(true);
                pt.removeProp("@replicated");
                file.unlockProperties();
            }
            else if (file.numCopies()<2) 
                WARNLOG("%s is not replicated",name.str());
        }
        catch (IException *e) {
            EXCLOG(e,"fixReplicated");
        }
    }
}

void listOldClusters()
{
    printf("Files:\n");
    Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator("*",true);
    StringBuffer name;
    ForEach(*iter) {
        try {
            IDistributedFile &file=iter->query();
            iter->getName(name.clear());
            Owned<IDistributedFilePart> pi = file.getPart(0);
            if (pi) {
                IpAddress ip = pi->queryNode()->endpoint().ip;
                if (ip.ip[1]==150) {
                    StringBuffer s;
                    ip.getText(s);
                    printf("%s %s\n",s.str(),name.str());
                }
            }
        }
        catch (IException *e) {
            EXCLOG(e,"listOld");
        }
    }
    printf("Groups:\n");
    Owned<INamedGroupIterator> giter = queryNamedGroupStore().getIterator();
    ForEach(*giter) {
        giter->get(name.clear());
        Owned<IGroup> lgrp = queryNamedGroupStore().lookup(name.str());
        if (lgrp) {
            IpAddress ip = lgrp->queryNode(0).endpoint().ip;
            if (ip.ip[1]==150) {
                StringBuffer s;
                ip.getText(s);
                printf("%s %s\n",s.str(),name.str());
            }
        }
    }

}

*/

void listWuOrphans(bool del)
{
    Owned<IRemoteConnection> wuconn = querySDS().connect("WorkUnits",myProcessSession(),RTM_LOCK_READ, INFINITE);
    if (!wuconn) {
        DBGLOG("Could not connect to WorkUnits");
        return;
    }
    StringArray todelete;
    Owned<IPropertyTree> wuroot = wuconn->getRoot();
    Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator(".::*",true);
    ForEach(*iter) {
        try {
            IDistributedFile &file=iter->query();
            StringBuffer name;
            iter->getName(name);
            //PROGLOG("Checking %s\n",name.str());
            if (file.queryProperties().getPropBool("@owned")) {
                const char *wuid = file.queryProperties().queryProp("@workunit");
                if (wuid&&*wuid) {
                    if (!wuroot->hasProp(wuid)) {
                        WARNLOG("ORPHAN of %s : %s",wuid,name.str());
                        todelete.append(name.str());
                    }
                }
                else
                   ERRLOG("%s has no WUID",name.str());
            }
        }
        catch (IException *e) {
            EXCLOG(e,"listWuOrphaned");
        }
    }
    if (del) 
        for (unsigned i=0;i<todelete.ordinality();i++) {
            PROGLOG("Deleting %s",todelete.item(i));
            queryDistributedFileDirectory().removePhysical(todelete.item(i));
        }
}

void testPagedGet(const char *basexpath,const char *xpath,const char *sortorder,unsigned startoffset,unsigned pagesize)
{
    __int64 hint;
    loop {
        IArrayOf<IPropertyTree> res;
        unsigned start = msTick();
        Sleep(1000*10);
        Owned<IRemoteConnection> conn = getElementsPaged( basexpath, xpath, sortorder, startoffset, pagesize, NULL, "nigel", &hint, NULL, NULL, res);
        PROGLOG("got %d, Time taken = %dms",res.ordinality(),msTick()-start);
        if (res.ordinality()==0)
            break;
        for (unsigned i=0;i<res.ordinality();i++) {
            //PROGLOG("%s",res.item(i).queryName());
        }
        startoffset += res.ordinality();
    }
}

void timeLookup(const char *lname)
{
    clock_t start = clock();
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname);
    printf("Lookup of %s took %dms\n",lname, (unsigned) (clock()-start));
    if (file) {
        start = clock();
        Owned<IFileDescriptor> fdesc = file->getFileDescriptor();
        printf("getFileDescriptor took %dms\n",(unsigned) (clock()-start));
    }
}

void randFill(byte *b,size32_t sz)
{
    while (sz>=4) {
        *(unsigned *)b = getRandom();
        b += 4;
        sz -= 4;
    }
    while (sz) {
        *b = getRandom()%256;
        sz--;
    }
}


void makeTestFile(const char *lname,const char *cluster, offset_t fsize, size32_t rs)
{
    Owned<IGroup> group = queryNamedGroupStore().lookup(cluster);
    if (rs==0)
        rs = 100;
    fsize = (fsize/rs)*rs;
    if (!group) {
        ERRLOG("Cluster %s not found",cluster);
        return;
    }
    if (queryDistributedFileDirectory().exists(lname)) {
        ERRLOG("File %s already exists",lname);
        return;
    }
    IPropertyTree* attr = createPTree("Attr");
    attr->setProp("@owner","sdsfix");
    attr->setPropInt64("@size",fsize);
    attr->setPropInt("@recordSize",rs);
    attr->setPropInt("@recordCount",fsize/rs);
    CDfsLogicalFileName lfn;
    lfn.set(lname);
    DFD_OS os=DFD_OSdefault;
    StringBuffer ver;
    if (getDaliServixVersion(group->queryNode(0).endpoint(),ver)!=0) { // if cross-os needs dafilesrv
        if (strstr(ver.str(),"Windows")!=NULL)
            os = DFD_OSwindows;
        else 
            os = DFD_OSunix;
    }
    StringBuffer lfns;
    Owned<IFileDescriptor> fdesc = createFileDescriptor(lfn.get(lfns).str(),group,attr,os); 
    if (!fdesc) {
        ERRLOG("Could not create file %s(1)",lname);
        return;
    }
    Owned<IDistributedFile> dfile = queryDistributedFileDirectory().createNew(fdesc);
    if (!dfile) {
        ERRLOG("Could not create file %s(2)",lname);
        return;
    }
    unsigned n = group->ordinality();
    Owned<IDistributedFilePartIterator> piter = dfile->getIterator();   
    MemoryAttr ma;
    byte * buf = (byte *)ma.allocate(0x10000);
    offset_t tot=0;
    offset_t psize = ((fsize/rs+n-1)/n)*rs;
    ForEach(*piter) {
        IDistributedFilePart &part = piter->query();
        RemoteFilename rfn;
        part.getFilename(rfn);
        Owned<IFile> file = createDaliServixFile(rfn); //createIFile(rfn);
        Owned<IFileIO> fileio = file->open(IFOcreate);
        if (tot>=fsize)
            break;
        if (fsize-tot<psize)
            psize = fsize-tot;
        offset_t done = 0;
        CRC32 fileCRC;
        while (done<psize) {
            size32_t sz = (psize-done>0x10000)?0x10000:((size32_t)(psize-done));
            randFill(buf,sz);
            fileio->write(done,sz,buf);
            done += sz;
            fileCRC.tally(sz, buf);
        }
        fileio.clear();
        tot += psize;
        IPropertyTree &props = part.lockProperties();
        props.setPropInt64("@size",psize);
        props.setPropInt64("@fileCrc", (unsigned long)fileCRC.get());
        CDateTime createTime, modifiedTime, accessedTime;
        file->getTime(&createTime, &modifiedTime, &accessedTime);
        unsigned hour, min, sec, nanosec;
        modifiedTime.getTime(hour, min, sec, nanosec);
        modifiedTime.setTime(hour, min, sec, 0);
        StringBuffer timeStr;
        modifiedTime.getString(timeStr);
        props.setProp("@modified", timeStr.str());
        part.unlockProperties();
    }
    dfile->setModified();
    dfile->attach(lfn.get());
}

void testMakePhysicalName()
{
    StringBuffer dir;
    makePhysicalPartName(".::test", 0, 0, dir.clear());printf("%s\n",dir.str());
    makePhysicalPartName(" .::test", 0, 0, dir.clear());printf("%s\n",dir.str());
    makePhysicalPartName(" . ::test", 0, 0, dir.clear());printf("%s\n",dir.str());
    makePhysicalPartName("x::test", 0, 0, dir.clear());printf("%s\n",dir.str());
    makePhysicalPartName("xy::test", 0, 0, dir.clear());printf("%s\n",dir.str());
    makePhysicalPartName("xy ::test", 0, 0, dir.clear());printf("%s\n",dir.str());
    makePhysicalPartName(" x ::.::test", 0, 0, dir.clear());printf("%s\n",dir.str());
    makePhysicalPartName(" x :: . ::test", 0, 0, dir.clear());printf("%s\n",dir.str());
    makePhysicalPartName(" x ::yyy::test", 0, 0, dir.clear());printf("%s\n",dir.str());
    makePhysicalPartName(" x ::.::yyy ::test", 0, 0, dir.clear());printf("%s\n",dir.str());
    makePhysicalPartName(" x ::yyy::. ::test", 0, 0, dir.clear());printf("%s\n",dir.str());
    makePhysicalPartName(" x :: .:: yyy:: . ::test", 0, 0, dir.clear());printf("%s\n",dir.str());
}

void printWorkunitTimings(const char *wuid)
{
    StringBuffer path;
    path.append("/WorkUnits/").append(wuid);
    Owned<IRemoteConnection> conn = querySDS().connect(path, myProcessSession(), 0, 5*60*1000);  
    if (!conn) {
        printf("WU %s not found",wuid);
        return;
    }
    IPropertyTree *wu = conn->queryRoot();
    Owned<IPropertyTreeIterator> iter = wu->getElements("Timings/Timing");
    StringBuffer name;
    printf("Name,graph,sub,gid,time ms,time min\n-\n");
    ForEach(*iter) {
        if (iter->query().getProp("@name",name.clear())) {
            if ((name.length()>11)&&(memcmp("Graph graph",name.str(),11)==0)) {
                unsigned gn;
                const char *s = getNum(name.str(),gn);
                unsigned sn;
                s = getNum(s,sn);
                if (gn&&sn) {
                    const char *gs = strchr(name.str(),'(');
                    unsigned gid = 0;
                    if (gs)
                        getNum(gs+1,gid);
                    unsigned time = iter->query().getPropInt("@duration");
                    printf("\"%s\",%d,%d,%d,%d,%d\n",name.str(),gn,sn,gid,time,(time/60000));
                }
            }
        }
    }

}

void testwufiles(const char *filename)
{
    StringBuffer path;
    path.append("/WorkUnits/");
    Owned<IRemoteConnection> conn = querySDS().connect(path, myProcessSession(), 0, 5*60*1000);  
    if (!conn) {
        printf("WU root not found");
        return;
    }
    IPropertyTree *wu = conn->queryRoot();
    StringBuffer query;
    query.appendf("*[FilesRead/File/@name=\"%s\"]",filename);
    Owned<IPropertyTreeIterator> iter = wu->getElements(query.str());
    ForEach(*iter) {
        StringBuffer wuid;
        iter->query().getName(wuid);
        printf("%s\n",wuid.str());
    }
}

void testDaliDown()
{
    Semaphore term;
    while (!term.wait(1000)) {
        if (!verifyCovenConnection(60*1000)) {
            PROGLOG("Dali stopped");
            break;
        }
        PROGLOG("...");
    }
}

void  testLogicalFileList(const char *mask, bool includenormal, bool includesuper, StringArray &out)
{
    MemoryBuffer mb;
    if (!mask||!*mask)
        mask ="*";
    Owned<IDFAttributesIterator> iter = queryDistributedFileDirectory().getDFAttributesIterator(mask,true,includesuper,NULL);
    if (iter) {
        StringBuffer s;
        ForEach(*iter) {
            IPropertyTree &attr=iter->query();
            const char *name = attr.queryProp("@name");
            if (!name||!*name)
                continue;
            out.append(name);
        }
    }

}


void testLogicalFileSuperOwners(const char *lfn,StringArray &out)
{
    MemoryBuffer mb;
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn,NULL);
    if (df) {
        StringBuffer name;
        Owned<IDistributedSuperFileIterator> iter = df->getOwningSuperFiles();
        ForEach(*iter) {
            iter->query().getLogicalName(name.clear());
            size32_t sz = name.length();
            if (!sz)
                continue;
            out.append(name.str());
        }
    }
    else
        ERRLOG("LogicalFileSuperOwners: Could not locate file %s", lfn);
}

void testDaliOverload(const char *mask)
{
#if 0
    StringArray files;
    testLogicalFileList(mask,true,false,files);
    ForEachItemIn(i,files) {
        StringArray owners;
        testLogicalFileSuperOwners(files.item(i),owners);
    }
#else
    MemoryBuffer mb;
    SocketEndpoint ep(".:7070");
    RemoteFilename rfn;
    rfn.setPath(ep,"/c$/test");
    Owned<IFile> f = createIFile(rfn);
    if (f) {
        StringBuffer s;
        StringBuffer ds;
        Owned<IDirectoryIterator> di = f->directoryFiles("*",true);
        if (di) {
            ForEach(*di) {
                di->getName(s.clear());
                __int64 fsz = di->getFileSize();
                CDateTime dt;
                di->getModifiedTime(dt);
                size32_t sz = s.length();
                dt.getString(ds.clear());
                ds.padTo(19);
                mb.append(sz).append(sz,s.str()).append(fsz).append(19,ds.str());
            }
        }
    }

#endif

}



void fixTilde()
{
    // first took for files in the root
    Owned<IRemoteConnection> conn = querySDS().connect("Files", myProcessSession(), 0, 5*60*1000);  
    if (!conn) {
        printf("Files root not found");
        return;
    }
    IPropertyTree *root = conn->queryRoot();
    IPropertyTree *top = NULL;
    Owned<IPropertyTreeIterator> iter = root->getElements("File");
    StringArray todelete;
    ForEach(*iter) {
        IPropertyTree &f = iter->query();
        IPropertyTree *n = createPTree(&f);
        StringBuffer name;
        if (!n->getProp("@name",name))
            continue;
        todelete.append(name);
        StringBuffer newname;
        newname.append("__").append(name);
        if (!top) {
            top = root->queryPropTree("Scope[@name=\".\"]");
            if (!top) {
                top = createPTree("Scope");
                top->setProp("@name",".");
                top = root->addPropTree("Scope",top);
            }
        }
        StringBuffer mask;
        loop {
            mask.clear().appendf("File[@name=\"%s\"]",newname.str());
            if (!top->queryPropTree(mask.str()))
                break;
            newname.append('_');
        }
        n->setProp("@name",newname.str());
        top->addPropTree("File",n);
        PROGLOG("Renamed File '%s' to '.::%s'",name.str(),newname.str());
    }
    ForEachItemIn(i,todelete) {
        StringBuffer sb;
        sb.appendf("File[@name=\"%s\"]",todelete.item(i));
        root->removeProp(sb.str());
    }
    Owned<IPropertyTreeIterator> iter2 = root->getElements("Scope");
    ForEach(*iter2) {
        IPropertyTree &s = iter2->query();
        StringBuffer name;
        s.getProp("@name",name);
        if ((name.length()==0)||(name.charAt(0)=='~')) {
            StringBuffer newname;
            newname.append("__").append(name);
            StringBuffer mask;
            loop {
                mask.clear().appendf("Scope[@name=\"%s\"]",newname.str());
                if (!root->queryPropTree(mask.str()))
                    break;
                newname.append('_');
            }
            s.setProp("@name",newname.str());
            PROGLOG("Renamed Scope '%s' to '%s'",name.str(),newname.str());
        }
    }

}

const char * ns(const char *s)
{
    return s?s:"";
}

void dirtocsv(const char *src)
{
    Owned<IFile> iFile = createIFile(src);
    Owned<IFileIO> iFileIO = iFile->open(IFOread);
    if (!iFileIO)
    {
        DBGLOG("Could not open to %s",src);
        return;
    }
    size32_t sz = iFile->size();
    StringBuffer xml;
    iFileIO->read(0, sz, xml.reserve(sz));
    Owned<IPropertyTree> branch = createPTreeFromXMLString(xml.str());
    IPropertyTree *dirs = branch->queryPropTree("Directories");
    Owned<IPropertyTreeIterator> iter = dirs->getElements("Directory");
    ForEach(*iter) {
        IPropertyTree &pt=iter->query();
        printf("%s,%s,%s,%s,%s,%s,%s\n",ns(pt.queryProp("Name")),ns(pt.queryProp("Size")),ns(pt.queryProp("MinIP")),ns(pt.queryProp("MinSize")),ns(pt.queryProp("MaxIP")),ns(pt.queryProp("MaxSize")),ns(pt.queryProp("Skew")));
    }
}

bool testFileLock(const char *lname,unsigned timesecs,bool write)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname);
    if (!file) {
        printf("Cannot find %s",lname);
        return false;
    }
    if (write)
        file->lockProperties();
    printf("sleeping");
    Sleep(timesecs*1000);
    return true;
}

void testLogicalFileCompare(const char *name1,const char *name2, int mode)
{
    StringBuffer errstr;
    int ret = queryDistributedFileDirectory().fileCompare(name1,name2,mode?(DistributedFileCompareMode)mode:DFS_COMPARE_FILES_LOGICAL,errstr);
    switch (ret) {
    case DFS_COMPARE_RESULT_FAILURE:        printf("Compare failed: %s\n",errstr.str()); break;
    case DFS_COMPARE_RESULT_DIFFER_OLDER:   printf("Compare result: Differ Older (%d)\n",ret); break;
    case DFS_COMPARE_RESULT_SAME_OLDER:     printf("Compare result: Same Older (%d)\n",ret); break;
    case DFS_COMPARE_RESULT_SAME:           printf("Compare result: Same (%d)\n",ret); break;
    case DFS_COMPARE_RESULT_SAME_NEWER:     printf("Compare result: Same Newer (%d)\n",ret); break;
    case DFS_COMPARE_RESULT_DIFFER_NEWER:   printf("Compare result: Differ Newer (%d)\n",ret); break;
    }
}

void testLogicalFileVerify(const char *name, bool crc)
{
    StringBuffer errstr;
    if (queryDistributedFileDirectory().filePhysicalVerify(name,crc,errstr)) 
        printf("Verified OK\n");
    else
        printf("%s\n",errstr.str());
}

void testValidate(const char *s)
{
    if (s) {
        CDfsLogicalFileName name;
        printf("'%s' - '%s'\n",s,name.setValidate(s)?name.get():"FAILED");
    }
    else {
        testValidate("");
        testValidate("  ");
        testValidate("~");
        testValidate(" ~");
        testValidate("   ~ ~");
        testValidate("s");
        testValidate("~s");
        testValidate(" ~s");
        testValidate("   ~s ~");
        testValidate("s::x");
        testValidate("~s::x");
        testValidate(" ~s::");
        testValidate("   ~s::x ~");
        testValidate(" x y:: xxx ");
        testValidate(" x y :: xxx ");
        testValidate("  x xx ");
        testValidate("  x : xx ");
        testValidate(" x y:: xxx :: zzz");
        testValidate(" x y :: xxx :: zz z ");
        testValidate("  x xx ::z");
        testValidate("  x : xx ::zzz ");
        testValidate("xx::::xxx");
        testValidate("a::b::c::d::e");
    }
}

void printPartDesc(IPartDescriptor *p)
{
    printf("Part[%d]\n",p->queryPartIndex());
    StringBuffer tmp1;
    for (unsigned i=0;i<p->numCopies();i++) {
        if (p->isMulti()) {
            RemoteMultiFilename rmfn;
            RemoteFilename rfn;
            p->getMultiFilename(i,rmfn);
            ForEachItemIn(j1,rmfn) {
                RemoteFilename &rfn = rmfn.item(j1);
                printf("  file (%d,%d   ) = %s\n",i,j1,rfn.getRemotePath(tmp1.clear()).str());
            }
        }
        else {
            RemoteFilename rfn;
            p->getFilename(i,rfn);
            printf("  file (%d   ) = %s\n",i,rfn.getRemotePath(tmp1.clear()).str());
        }
    }
}


void testFileDescriptor()
{
    Owned<IGroup> grp = queryNamedGroupStore().lookup("thor");
    if (grp) {
        ClusterPartDiskMapSpec mspec;
        RemoteFilename rfn;
        constructPartFilename(grp,1,2,"test::xxx","file._$P$_of_$N$","/c$/roxiedata",0,mspec,rfn);
    }
    Owned<IPropertyTree> pp = createPTree("Part");
    Owned<IFileDescriptor> fdesc = createFileDescriptor();
    fdesc->setDefaultDir("/c$/thordata/test");
    Owned<INode> node = createINode("192.168.0.1");
    pp->setPropInt64("@size",1234);
    fdesc->setPart(0,node,"testfile1.d00._1_of_5",pp);
    node.setown(createINode("192.168.0.2"));
    pp->setPropInt64("@size",2345);
    fdesc->setPart(1,node,"testfile1.d00._2_of_5",pp);
    SocketEndpoint ep("192.168.0.3");
    RemoteFilename rfn;
    rfn.setPath(ep,"/c$/thordata/test/testfile1.d00._3_of_5");
    pp->setPropInt64("@size",3456);
    fdesc->setPart(2,rfn,pp);
    node.setown(createINode("192.168.0.4"));
    fdesc->setPart(3,node,"tf1.txt,tf2.txt,testfile1.d00._4_of_5");
    fdesc->setPart(4,node,"testfile1.d00._5_of_5",pp);
    rfn.setPath(node->endpoint(),"/c$/thorback/test/testfile1.d00._5_of_5");
    //fdesc->setReplicatePart(4,rfn);
    printf("-----------------------------------------------------------------------------------\n");
    printDesc(fdesc);
    printf("-----------------------------------------------------------------------------------\n");
    MemoryBuffer mb;
    fdesc->serialize(mb);
    Owned<IFileDescriptor> fdesc2 = deserializeFileDescriptor(mb);
    printDesc(fdesc2);
    for (unsigned i=0;i<fdesc->numParts();i++) {
        printf("-------------------------------------------------------------------------------\n");
        Owned<IPartDescriptor> pd = fdesc->getPart(i);
        printPartDesc(pd);
        pd->serialize(mb.clear());
        Owned<IPartDescriptor> pd2 = deserializePartFileDescriptor(mb);
        printf("-------------------------------------------------------------------------------\n");
        printPartDesc(pd2);
    }
    printf("-------------------------------------------------------------------------------\n");
    queryDistributedFileDirectory().removeEntry("sdsfix::testfile1");
    Owned<IDistributedFile> dfile = queryDistributedFileDirectory().createNew(fdesc);
    dfile->attach("sdsfix::testfile1");
    dfile.clear();
    dfsFile("sdsfix::testfile1",NULL,false);
    printf("-------------------------------------------------------------------------------\n");
    fileTree("sdsfix::testfile1");
    setNodeCaching(false);
}   

typedef MapStringTo<bool> SubfileSet;

#if 0
void printSubfiles(IUserDescriptor *user=NULL)
{
    CDfsLogicalFileName lfn;
    KeptAtomTable files;
    StringBuffer query;
    SubfileSet sfset;
    Owned<IDFScopeIterator> siter = queryDistributedFileDirectory().getScopeIterator(NULL,true,user);
    ForEach(*siter) {
        lfn.set(siter->query(),"X");
        lfn.makeScopeQuery(query.clear());
        Owned<IRemoteConnection> conn = querySDS().connect(query.str(),myProcessSession(),0, INFINITE);
        if (conn) {
            IPropertyTree *t = conn->queryRoot();
            Owned<IPropertyTreeIterator> iter = t->getElements("SuperFile/SubFile");
            ForEach(*iter) {
                const char *name =  iter->query().queryProp("@name");
                printf("%s\n",name);
                if (!sfset.getValue(name)) 
                    sfset.setValue(name, true);
            }
        }
    }
}
#else
void printSubfiles(IUserDescriptor *user=NULL)
{
    SubfileSet sfset;
    Owned<IRemoteConnection> conn = querySDS().connect("Files",myProcessSession(),0, INFINITE);
    if (conn) {
        IPropertyTree *t = conn->queryRoot();
        Owned<IPropertyTreeIterator> iter = t->getElements("//SubFile");
        ForEach(*iter) {
            const char *name =  iter->query().queryProp("@name");
            printf("%s\n",name);
            if (!sfset.getValue(name)) 
                sfset.setValue(name, true);
        }
    }
}
#endif


IPropertyTree *getFileTree(const char *lname,const INode *foreigndali,IUserDescriptor *user=NULL, unsigned cachetimeoutmins=60, unsigned timeout=FOREIGN_DALI_TIMEOUT)
{
    if (!foreigndali||foreigndali->isHost())
        queryDistributedFileDirectory().getFileTree(lname,foreigndali,user,timeout);
    CDfsLogicalFileName lfn;
    lfn.set(lname);
    StringBuffer cachename("/Roxie/FileCache/foreign::");
    foreigndali->endpoint().getUrlStr(cachename);
    cachename.append("::").append(lfn.get());
    Owned<IRemoteConnection> conn = querySDS().connect(cachename,myProcessSession(),RTM_CREATE_QUERY|RTM_LOCK_WRITE, timeout);
    if (!conn) 
        throw MakeStringException(DFSERR_ForeignDaliTimeout, "getFileTree: Timeout connecting to ", cachename.str());

    MemoryBuffer mb;
    IPropertyTree &root = *conn->queryRoot();
    CDateTime now;
    StringBuffer ds;
    if (root.getProp("@date",ds)&&ds.length()) {
        CDateTime dt;
        dt.setString(ds.str());
        dt.adjustTime(cachetimeoutmins);
        now.setNow();
        if (dt.compare(now)>0) { // its ok
            conn->changeMode(RTM_LOCK_READ,INFINITE);
            root.getPropBin(NULL,mb);
            return createPTree(mb);
        }
    }
    Owned<IPropertyTree> tree = queryDistributedFileDirectory().getFileTree(lname,foreigndali,user,timeout);
    tree->serialize(mb);
    root.setPropBin(NULL,mb.length(),mb.toByteArray());
    now.setNow();
    now.getString(ds.clear());
    root.setProp("@date",ds);
    return tree;
};

void cleanGeneratedDlls(bool logonly)
{
    loop {
        Owned<IRemoteConnection> conn = querySDS().connect("/GeneratedDlls", myProcessSession(), 0, 100000);
        if (!conn) {
            printf("ERROR cannot connect to dali");
            return;
        }
        PROGLOG("loading GeneratedDll");
        StringArray todelete;
        Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("GeneratedDll");
        PROGLOG("loaded GeneratedDll");
        unsigned num = 0;
        ForEach(*iter) {
            IPropertyTree &pt = iter->query();
            Owned<IPropertyTreeIterator> liter = pt.getElements("location");
            bool ok = true;
            bool notfound = false;
            ForEach(*liter) {
                IPropertyTree &loc = liter->query();
                const char *ip = loc.queryProp("@ip");
                if (!ip||!*ip) 
                    continue;
                if (memcmp(ip,"10.150.64",9)==0)
                    ok = false;
                else if (memcmp(ip,"10.150.68",9)==0)
                    ok = false;
                else if (memcmp(ip,"10.150.28",9)==0)
                    ok = false;
                else if (memcmp(ip,"10.150.29",9)==0)
                    ok = false;
                else if (memcmp(ip,"172.16.99",9)==0)
                    ok = false;
                else if (memcmp(ip,"10.150.50",9)==0)
                    ok = false;
                else if (memcmp(ip,"10.150.51",9)==0)
                    ok = false;
                const char *dll = loc.queryProp("@dll");
                if (!dll||!*dll) 
                    continue;
                RemoteFilename rfn;
                SocketEndpoint ep(ip);
                rfn.setPath(ep,dll);
                StringBuffer path;
                rfn.getRemotePath(path);
                try {
                    Owned<IFile> dllfile = createIFile(rfn);
                    if (!dllfile) {
                        ERRLOG("createIFile(%s) failed",path.str());
                    }
                    else if (!dllfile->exists()) {
                        ERRLOG("File %s doesn't exist",path.str());
                        ok = false;
                        notfound = true;
                    }
                }
                catch(IException *e) {
                    EXCLOG(e,path.str());
                    e->Release();
                    ok = false;
                }

            }
            const char *created = pt.queryProp("@created");
            if (!created||!*created)
                continue;
            CDateTime dt;
            dt.setString(created);
            unsigned year;
            unsigned month; 
            unsigned day;
            dt.getDate(year, month, day);
            if (year==2008)
                continue;
            if (!notfound&&(year==2007))
                continue;
            bool found = true;
            if (ok) {
                const char *kind = pt.queryProp("@kind");
                if (!kind||((strcmp(kind,"Workunit CPP")!=0)&&(strcmp(kind,"Workunit DLL")!=0))) 
                    continue;
                const char *name = pt.queryProp("@name");
                if (!name||!*name)
                    continue;
                const char *s = strstr(name,"W200");
                if (!s)
                    continue;
                const char *e = s+4;
                while (*e&&(*e!='.'))
                    e++;
                StringBuffer query("/WorkUnits/");
                query.append(e-s,s);
                Owned<IRemoteConnection> wuconn = querySDS().connect(query.str(), myProcessSession(), 0, 5*60*1000);  ;
                found = (wuconn.get()!=NULL);
                if (!found) {
                    printf("%s does not exist\n",query.str());
                    ok = false;
                }
            }
            if (!ok) {
                todelete.append(pt.queryProp("@uid"));
                if (logonly)
                    printf("UID %s to be deleted\n",pt.queryProp("@uid"));
                if (!logonly&&(num++==1000))
                    break;
            }
        }
        if (logonly||(num==0))
            break;
        iter.clear();
        conn.clear();
        ForEachItemIn(i,todelete) {
            StringBuffer query;
            query.appendf("GeneratedDlls/GeneratedDll[@uid=\"%s\"]",todelete.item(i));
            conn.setown(querySDS().connect(query.str(), myProcessSession(), RTM_LOCK_READ|RTM_DELETE_ON_DISCONNECT, 100000));
            if (conn)
                printf("Deleting %s\n",query.str());
            else
                printf("Cannot find %s\n",query.str());

            conn.clear();
        }
    }
}

void testGroupCompare()
{
    GroupRelation gr;
    Owned<IGroup> grp1 = createIGroup("10.173.10.81");
    gr = grp1->compare(grp1);
    Owned<IGroup> grp3 = createIGroup("10.173.10.81-83");
    gr = grp3->compare(grp3);
    gr = grp1->compare(grp3);
    gr = grp3->compare(grp1);
    Owned<IGroup> grp3b = createIGroup("10.173.10.83,82,81");
    gr = grp1->compare(grp3b);
    gr = grp3b->compare(grp1);
    gr = grp3->compare(grp3b);
    gr = grp3b->compare(grp3);
    Owned<IGroup> grp5 = createIGroup("10.173.10.81-85");
    gr = grp1->compare(grp5);
    gr = grp3b->compare(grp5);
    gr = grp5->compare(grp3b);
    gr = grp3->compare(grp5);
    gr = grp5->compare(grp3);
    Owned<IGroup> grp5b = createIGroup("10.173.10.83-87");
    gr = grp1->compare(grp5b);
    gr = grp3b->compare(grp5b);
    gr = grp5b->compare(grp3b);
    gr = grp3->compare(grp5b);
    gr = grp5b->compare(grp3);
    gr = grp5->compare(grp5b);
    gr = grp5b->compare(grp5);
    Owned<IGroup> grp5c = createIGroup("10.173.10.81-83,81,82");
    gr = grp1->compare(grp5c);
    gr = grp5c->compare(grp1);
    gr = grp3->compare(grp5c);
    gr = grp5c->compare(grp3);
    gr = grp3b->compare(grp5c);
    gr = grp5c->compare(grp3b);
    gr = grp5->compare(grp5c);
    gr = grp5c->compare(grp5);
}



void testGroupFind()
{
    testGroupCompare();
    Owned<IGroup> grp = createIGroup("10.173.10.81-83"); // */queryNamedGroupStore().lookup("10.173.10.80");
    StringBuffer name;
    queryNamedGroupStore().find(grp,name,true);
}

void testClusterNames(const char *name)
{
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(name, NULL);
    if(df) {
        StringArray clusters;
        df->getClusterNames(clusters);
    }
}

void XmlMemSize(const char *filename)
{
    Owned<IFile> iFile = createIFile(filename);
    Owned<IFileIO> iFileIO = iFile->open(IFOread);
    if (!iFileIO)
    {
        DBGLOG("Could not open to %s",filename);
        return;
    }
    size32_t sz = iFile->size();
    StringBuffer xml;
    iFileIO->read(0, sz, xml.reserve(sz));
    Owned<IPropertyTree> branch = createPTreeFromXMLString(xml.str(), ipt_none, xr_ignoreWhiteSpace);
    unsigned tm = setAllocHook(true);
    branch.clear();
    tm -= setAllocHook(false);
    PROGLOG("%d bytes",tm);
}


void xmlFormat(const char *fni,const char *fno)
{
#if 0
    int tmpFlag = _CrtSetDbgFlag( _CRTDBG_REPORT_FLAG );
    tmpFlag |= _CRTDBG_CHECK_ALWAYS_DF;
    _CrtSetDbgFlag( tmpFlag );
#endif
    StringBuffer trc;
    getSystemTraceInfo(trc,PerfMonStandard);
    PROGLOG("%s",trc.str());
    PROGLOG("Loading...");
    unsigned t = msTick();
    Owned<IPropertyTree> pt = createPTreeFromXMLFile(fni);
    PROGLOG("Loaded took %dms",msTick()-t);
    getSystemTraceInfo(trc.clear(),PerfMonStandard);
    PROGLOG("%s",trc.str());
    if (fno&&(*fno!='!')) {
        PROGLOG("Saving to %s",fno);
        t = msTick();
        saveXML(fno,pt);
        PROGLOG("Saving took %dms",msTick()-t);
    }
}

/*
void xmlScan(const char *fni,const char *pat,bool mem)
{
    StringArray results;
    Owned<IPropertyTree> pt = createPTreeFromXMLFile(fni);
    StringBuffer full;
    PTreeScan(pt,pat,full,results,mem);
    ForEachItemIn(i,results)
        printf("%s\n",results.item(i));
}
*/

static int scompare(const char **s1,const char **s2)
{
    if (*s1==*s2)
        return 0;
    if (!*s1)
        return -1;
    if (!*s2)
        return -1;
    return strcmp(*s1,*s2);
}

/*
void xmlCompare(const char *fni1,const char *fni2,const char *pat)
{
    StringArray results1;
    Owned<IPropertyTree> pt = createPTreeFromXMLFile(fni1);
    StringBuffer full;
    PTreeScan(pt,pat,full,results1,false);
    StringArray results2;
    pt.clear();
    pt.setown(createPTreeFromXMLFile(fni2));
    PTreeScan(pt,pat,full,results2,false);
    results1.sort(scompare);
    results2.sort(scompare);
    unsigned i=0;
    unsigned j=0;
    while ((i<results1.ordinality())&&(j<results2.ordinality())) {
        const char *l = results1.item(i);
        const char *r = results2.item(j);
        int cmp = strcmp(l,r);
        if (cmp==0) {
            i++; 
            j++;
        }
        else if (cmp<0) {
            printf(">> %s\n",l);
            i++;
        }
        else {
            printf("<< %s\n",r);
            j++;
        }
    }
    while (i<results1.ordinality()) {
        printf(">> %s\n",results1.item(i));
        i++;
    }
    while (j<results2.ordinality()) {
        printf("<< %s\n",results1.item(j));
        j++;
    }
}
*/

void testThorRunningWUs()
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Status/Servers",myProcessSession(),RTM_LOCK_READ,30000);
    if (conn.get()) 
    {
        Owned<IPropertyTreeIterator> it(conn->queryRoot()->getElements("Server"));
        ForEach(*it) {
            StringBuffer instance;
            if(it->query().hasProp("@queue"))
            {
                const char* queue=it->query().queryProp("@queue");
                if(strstr(queue,".thor")) {
                    Owned<IPropertyTreeIterator> wuids(it->query().getElements("WorkUnit"));
                    ForEach(*wuids) {
                        IPropertyTree &wu = wuids->query();
                        const char* wuid=wu.queryProp(NULL);
                        if (wuid&&*wuid) {
                            const char *prioclass = wu.queryProp("@priorityClass");
                            bool high = false;
                            if (prioclass&&(stricmp(prioclass,"high")==0))
                                high = true;
                            PROGLOG("%s running on queue %s",wuid,queue);
                        }
                    }
                }
            }
        }
    }
}

void convertBinBranch(IPropertyTree &cluster,const char *branch)
{
    StringBuffer query(branch);
    query.append("/data");
    IPropertyTree *t;
    MemoryBuffer buf;
    cluster.getPropBin(query.str(),buf);
    if (buf.length()) {
        StringBuffer xml;
        xml.append(buf.length(),buf.toByteArray());
        t = createPTreeFromXMLString(xml.str());
        cluster.removeProp(query.str());
        cluster.addPropTree(query.str(),t);
    }
}


void getDFUXREF(const char *dst)
{
    Owned<IRemoteConnection> conn = querySDS().connect("DFU/XREF",myProcessSession(),RTM_LOCK_READ, INFINITE);
    Owned<IPropertyTree> root = createPTree(conn->getRoot());
    Owned<IPropertyTreeIterator> iter = root->getElements("Cluster");
    ForEach(*iter) {
        IPropertyTree &cluster = iter->query();
        convertBinBranch(cluster,"Directories");
        convertBinBranch(cluster,"Lost");
        convertBinBranch(cluster,"Found");
        convertBinBranch(cluster,"Orphans");
        convertBinBranch(cluster,"Messages");
    }

    Owned<IFile> f = createIFile(dst);
    Owned<IFileIO> io = f->open(IFOcreate);
    Owned<IFileIOStream> fstream = createBufferedIOStream(io);
    toXML(root, *fstream);          // formatted (default)
    DBGLOG("DFU/XREF saved in '%s'",dst);
    conn->close();
}

static void T()
{
//  testClusterNames("thor_data400::in::apex");
//  testClusterNames("thor_data400::base::hss_name_address_prod");
#if 0
    Owned<IRemoteConnection> conn = querySDS().connect("/Files", myProcessSession(), 0, 100000);
    if (!conn) {
        ERRLOG("cannot connect to /Files");
        return;
    }
    const char *test = "Scope[@name=\"thor_dev\"]/Scope[@name=\"rttemp\"]/File[@name=\"testkeyedjoin\"][Cluster/@name=\"xthor\"]";
    IPropertyTree *t = conn->queryRoot()->queryPropTree(test);
    if (t)
        printf("name = %s\n",t->queryProp("@name"));
#endif
#if 1
    Owned<IPropertyTree> tree = queryDistributedFileDirectory().getFileTree("thor_dev::temp::testsub1");
    StringBuffer str;
    toXML(tree, str.clear());
    printf("%s\n",str.str());
    tree.setown(queryDistributedFileDirectory().getFileTree("thor_dev::temp::testsub1@thor"));
    toXML(tree, str.clear());
    printf("%s\n",str.str());
    tree.setown(queryDistributedFileDirectory().getFileTree("thor_dev::temp::testsub1@jakethor"));
    toXML(tree, str.clear());
    printf("%s\n",str.str());
    tree.setown(queryDistributedFileDirectory().getFileTree("thor_dev::temp::testsub1@zzz"));
    toXML(tree, str.clear());
    printf("%s\n",str.str());

    StringArray clusters;
//  queryDistributedFileDirectory().lookupFileClusters("thor_dev::temp::testsub1",clusters);
    ForEachItemIn(i,clusters)
        printf("%d: %s\n",i,clusters.item(i));
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup("thor_dev::temp::testsub1");
    if (file) {
        printf("OK\n");
    }
#endif
}

void testMultiDali(const char *filename,const char *dali1,const char *dali2)
{
    __int64 sz1 = -1;
    __int64 sz2 = -1;
    SocketEndpoint ep;
    ep.set(dali1,DALI_SERVER_PORT);
    Owned<IGroup> group = createIGroup(1,&ep); 
    try {
        initClientProcess(group, DCR_Util);
        {
            Owned<IDistributedFile> file1 = queryDistributedFileDirectory().lookup(filename);
            if (file1)
                sz1 = file1->getFileSize(true,false);
        }
    }
    catch (IException *e) {
        pexception("testMultiDali.1",e);
        e->Release();
    }
    try {
        closedownClientProcess();
    }
    catch (IException *e) {
        pexception("testMultiDali.2",e);
        e->Release();
    }
    ep.set(dali2,DALI_SERVER_PORT);
    group.setown(createIGroup(1,&ep)); 
    try {
        initClientProcess(group, DCR_Util);
        {
            Owned<IDistributedFile> file2 = queryDistributedFileDirectory().lookup(filename);
            if (file2)
                sz2 = file2->getFileSize(true,false);
        }
    }
    catch (IException *e) {
        pexception("testMultiDali.3",e);
        e->Release();
    }
    try {
        closedownClientProcess();
    }
    catch (IException *e) {
        pexception("testMultiDali.4",e);
        e->Release();
    }
    printf("sizes %"I64F"d,%"I64F"d\n",sz1,sz2);
}

void testFileBackupCrash(const char *filename)
{
#define DEBUG_DIR "debug"
    try
    {
        unsigned crc = getFileCRC(filename);
        StringBuffer dst(".");
        addPathSepChar(dst);
        dst.append(DEBUG_DIR);
        addPathSepChar(dst);
        recursiveCreateDirectoryForFile(dst.str());
        OwnedIFile dFile = createIFile(dst.str());
        Owned<IDirectoryIterator> dIter = dFile->directoryFiles();
        unsigned debugFiles = 0;
        ForEach (*dIter) 
            debugFiles++;
        if (debugFiles >= 10) 
            return;
        StringBuffer fname(filename);
        getFileNameOnly(fname);
        dst.append(fname.str()).append('.').append(crc);
        OwnedIFile backupIFile = createIFile(dst.str());
        if (!backupIFile->exists()) // a copy could already have been made
        {
            PROGLOG("Backing up: %s", filename);
            OwnedIFile iFile = createIFile(filename);
            copyFile(backupIFile, iFile);
            PROGLOG("Backup made: %s", dst.str());
        }
    }
    catch (IException *e)
    {
        StringBuffer tmp;
        EXCLOG(e, tmp.append("Failed to take backup of: ").append(filename).str());
        e->Release();
    }
    byte *p = (byte*)99;
    *p =1;
}

void testDaliClientConnect(const char *dalis,unsigned delay)
{
    unsigned _t;
    Owned<IGroup> serverGroup = createIGroup(dalis, DALI_SERVER_PORT);
    _t = msTick();
    initClientProcess(serverGroup, DCR_Other);
    fprintf(stderr,"initClientProcess = %dms\n",msTick()-_t); 
    Sleep(delay*1000);
    _t = msTick();
    ::closedownClientProcess(); // dali client closedown
    fprintf(stderr,"closedownClientProcess = %dms\n",msTick()-_t);
}



void fileTime(const char *logicalName)
{
    StringBuffer fullname(logicalName);
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(fullname.str());
    if (file) {
        CDateTime dt;
        StringBuffer dtstr;
        file->getModificationTime(dt);
        dt.getString(dtstr);
        PROGLOG("date %s for file %s", dtstr.str(), fullname.str());
    }
}

void lookupGroup(const char *name,const char *remotedali)
{
    Owned<IGroup> group;
    if (remotedali&&*remotedali) {
        Owned<INode> node = createINode(remotedali,DALI_SERVER_PORT);
        group.setown(queryNamedGroupStore().getRemoteGroup(node, name));
    }
    else
        group.setown(queryNamedGroupStore().lookup(name));
    if (!group) {
        ERRLOG("cannot find group %s",name);
        return;
    }
    StringBuffer eps;
    for (unsigned i=0;i<group->ordinality();i++) {
        group->queryNode(i).endpoint().getUrlStr(eps.clear());
        printf("%d: %s\n",i,eps.str());
    }
}


void testMutex(const char *name,unsigned timeout,unsigned stime)
{
    Owned<IDaliMutex>  mutex = createDaliMutex(name);
    mutex->enter(timeout);
    PROGLOG("entered");
    Sleep(stime);
    PROGLOG("leaving");
    mutex->leave();
}

void testDfsRename(const char *from,const char *to)
{
    if (queryDistributedFileDirectory().renamePhysical(from,to))
        printf("file %s renamed to %s\n",from,to);
    else
        printf("file %s could not be renamed to %s\n",from,to);
}


class cLMLnotify: public CInterface, implements ILargeMemLimitNotify
{
public:
    IMPLEMENT_IINTERFACE;
    bool take(memsize_t tot)
    {
        PROGLOG("take %d",tot);
        return true;
    }
    void give(memsize_t tot)
    {
        PROGLOG("take %d",tot);
    }
};

void testLMlimit()
{
    Owned<cLMLnotify> notify = new cLMLnotify;
    setLargeMemLimitNotify(10000,notify);
    CLargeMemoryAllocator lmem(10000000,4094,false);
    PROGLOG("Before alloc");
    void *a = lmem.alloc(12000);
    PROGLOG("Sleeping");
    Sleep(50000);
    PROGLOG("Freeing");
    lmem.reset();
    PROGLOG("Exit");


}

void configureFileDescriptor(const char *logicalName, IFileDescriptor &fileDesc, bool fixSizes)
{
    unsigned __int64 offset = 0;
    Owned<IPartDescriptorIterator> iter = fileDesc.getIterator();
    bool noSize = false;
    ForEach (*iter)
    {
        IPartDescriptor &part = iter->query();
        IPropertyTree &props = part.queryProperties();
        props.setPropInt64("@offset", offset);
        if (props.hasProp("@size"))
        {
            if (noSize)
                WARNLOG("Some parts of logical file \"%s\" have sizes others do not!", logicalName);
            else
                offset += props.getPropInt64("@size");
        }
        else
        {
            if (fixSizes)
            {
                StringBuffer fileName;
                //getPartFilename(part, 0, fileName, false);
                //OwnedIFile iFile = createIFile(fileName.str());
                offset_t sz = 1234567; //iFile->size();
                props.setPropInt64("@size", sz);
                offset += sz;
            }
            else
                noSize = true;
        }
    }
}

IFileDescriptor *getConfiguredFileDescriptor(IDistributedFile &file, bool fixSizes)
{
    IFileDescriptor *fileDesc = file.getFileDescriptor();
    Owned<IDistributedFilePartIterator> iter = file.getIterator();
    unsigned partn = 0;
    // ensure @size's present as some activities rely upon e.g. @offset's
    ForEach (*iter)
    {
        IDistributedFilePart &part = iter->query();
        IPartDescriptor *partDesc = fileDesc->queryPart(partn);
        offset_t sz = part.getFileSize(true, false);
        partDesc->queryProperties().setPropInt64("@size", sz);
        partn++;
    }
    configureFileDescriptor(file.queryLogicalName(), *fileDesc, fixSizes);
    return fileDesc;
}


void testSerializeFile(const char *name)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(name);
    if (file) {
        unsigned slave = 0;
        Owned<IFileDescriptor> fileDesc = getConfiguredFileDescriptor(*file,false);
        Owned<IPartDescriptor> partDesc = fileDesc->getPart(slave);
        MemoryBuffer dst;
        partDesc->serialize(dst);
        PROGLOG("Serialized size(1) = %d",dst.length());
        partDesc.clear();
        partDesc.setown(deserializePartFileDescriptor(dst));
        dst.clear();
        UnsignedArray parts;
        parts.append(3);
        parts.append(7);
        parts.append(11);
        parts.append(19);
        fileDesc->serializeParts(dst,parts);
        PROGLOG("Serialized size(4) = %d",dst.length());
        IArrayOf<IPartDescriptor> parray;
        deserializePartFileDescriptors(dst,parray);
        assert(parray.ordinality()==4);
        dst.clear();
        fileDesc->serialize(dst);
        fileDesc.clear();
        fileDesc.setown(deserializeFileDescriptor(dst));
    }
}

void testNamedMutex()
{
    NamedMutex nm("atest");
    PROGLOG("Locking");
    nm.lock();
    PROGLOG("Locked,sleeping 60s");
    Sleep(60000);
    PROGLOG("Unlocking");
    nm.unlock();
    PROGLOG("Unlocked, sleeping 10s");
    Sleep(10000);
    PROGLOG("Locking 10s timeout");
    if (nm.lockWait(10000)) {
        PROGLOG("Locked,sleeping 10s");
        Sleep(10000);
        PROGLOG("Unlocking");
        nm.unlock();
    }
    else
        PROGLOG("Timed out");
}

void testSetAccessed(const char *name)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(name);
    if (!file) {
        ERRLOG("%s not found",name);
        return;
    }
    file->setAccessed();
}


void listCrossClusterReads(const char *froms,const char *tos)
{
    CDateTime from;
    from.setDateString(froms);
    CDateTime to;
    to.setDateString(tos);
    unsigned start = msTick();
    StringAttrArray res;
    queryAuditLogs(from,to,"FileAccess,Thor,READ",res);
    ForEachItemIn(i,res) {
        StringArray param;
        CslToStringArray(res.item(i).text.get(), param);
/*
            0:date
            1:FileAccess
            2:Thor
            3:READ
            4:cluster
            5:user
            6:file
            7:WUID
            8:graph
*/
        if (param.ordinality()<8)
            continue;
        StringBuffer where;
        Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(param.item(6));
        offset_t sz;
        if (file) {
            sz = file->getFileSize(true,false);     
            StringArray clusters;
            unsigned n = file->getClusterNames(clusters);
            if (n==0)
                where.append("NoCluster");
            else {
                ForEachItemIn(j,clusters) 
                    if (stricmp(clusters.item(j),param.item(4))==0) {
                        where.append(clusters.item(j));
                }
                if (where.length()==0)
                    where.append(clusters.item(0));
            }
        }
        else
            where.append("Unknown");
        if (sz!=(offset_t)-1)
            printf("%s,%s.%s,%s,%s,%"I64F"d\n",param.item(0),param.item(7),param.item(6),where.str(),param.item(4),sz);
        else
            printf("%s,%s.%s,%s,%s\n",param.item(0),param.item(7),param.item(6),where.str(),param.item(4));
        fflush(stdout);
    }
    printf("Took %dms\n",msTick()-start);
}

void testbinsz(const char *path)
{
    Owned<IFile> iFile = createIFile(path);
    Owned<IFileIO> iFileIO = iFile->open(IFOread);
    if (!iFileIO)
    {
        DBGLOG("Could not open to %s",path);
        return;
    }
    size32_t sz = iFile->size();
    StringBuffer xml;
    iFileIO->read(0, sz, xml.reserve(sz));
    Owned<IPropertyTree> branch = createPTreeFromXMLString(xml.str());
    Owned<IPropertyTree> t1 = createPTree("test1");
    StringBuffer str;
    toXML(branch,str);
    t1->setPropBin("data",str.length(),str.str());
    toXML(t1,str.clear());
    Owned<IFile> f1 = createIFile("t1.xml");
    Owned<IFileIO> io1 = f1->open(IFOcreate);
    Owned<IFileIOStream> fstream1 = createBufferedIOStream(io1);
    toXML(t1, *fstream1);           // formatted (default)
    Owned<IPropertyTree> t2 = createPTree("test2");
    MemoryBuffer mb;
    branch->serialize(mb);
    t2->setPropBin("data",mb.length(),mb.toByteArray());
    toXML(t2,str.clear());
    Owned<IFile> f2 = createIFile("t2.xml");
    Owned<IFileIO> io2 = f2->open(IFOcreate);
    Owned<IFileIOStream> fstream2 = createBufferedIOStream(io2);
    toXML(t2, *fstream2);           // formatted (default)

    MemoryBuffer m;
    t1->serialize(m);
    printf("size1 = %d\n",m.length());
    t2->serialize(m.clear());
    printf("size2 = %d\n",m.length());

}

void testGetTreeRaw(const char *dali,const char *xpath)
{
    Owned<INode> remotedali = createINode(dali,DALI_SERVER_PORT);
    Owned<IPropertyTreeIterator>iter=querySDS().getElementsRaw(xpath,remotedali,1000*60);
    ForEach(*iter) {
        StringBuffer str;
        toXML(&iter->query(),str);
        printf("%s\n",str.str());
    }
}


void testFTSlaveLocation(const char *remotedali,const char *ips)
{
#if 0
    StringBuffer progpath;
    StringBuffer workdir;
    Owned<INode> dali = createINode(remotedali);
    IpAddress ip(ips);
    if (getRemoteRunInfo(remotedali,"FTSlaveProcess", "ftslave", NULL, ip, progpath, workdir, remotedali, 1000*60*5)) {
        printf("progpath = '%s'\n",progpath.str());
        printf("workdir = '%s'\n",workdir.str());
    }
#endif
}

void testGetFileTree(const char *filename)
{
#if 0
    Owned<IFile> file = createIFile(filename);
    Owned<IFileIO> fileio = file->open(IFOread);
    CFileReader freader(fileio);
    loop {
        size32_t len;
        const char *ln = freader.nextLine(len);
        if (!ln)
            break;
        Owned<IPropertyTree> df = queryDistributedFileDirectory().getFileTree(ln, daliNode, NULL, DALI_FILE_LOOKUP_TIMEOUT); // don't want to pass the ~
    }
#endif
}

void testSetPath()
{
    CDfsLogicalFileName lfn;
    lfn.setFromMask("/c$/roxiedata/roxie_dev_linux/thor_data50/key/stuminiquery_name_password._$p$_of_51","/c$/roxiedata");
}


void testLostFile(const char *_lfn)
{
    CDfsLogicalFileName lfn;
    lfn.set(_lfn);
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lfn);
    if (!file) {
        ERRLOG("could not lookup possible lost file: %s",lfn.get());
        return;
    }
    StringBuffer tmpname;
    StringBuffer tmp;
    CDateTime dt;
    Owned<IPropertyTree> ft = createPTree("File");
    if (file->getModificationTime(dt)) {
        CDateTime now;
        now.setNow();
        CDateTime co(dt);
        co.adjustTime(RECENTCUTOFF*60*24);
        if (co.compare(now)>=0) {
            WARNLOG("Recent file ignored: %s",lfn.get());
            return;
        }
        dt.getString(tmp.clear());
        ft->setProp("Modified",tmp.str());
    }
    unsigned np = file->numParts();
    unsigned cn = 0;                    
    ft->setProp("Name",lfn.get());
    tmp.clear().append(file->queryPartMask()).toLowerCase();
    ft->setProp("Partmask",tmp.str());
    ft->setPropInt("Numparts",np);
    file->getClusterName(cn,tmp.clear());
    ft->setProp("Cluster",tmp.str());
    bool *primlost = new bool[np];
    bool *replost = new bool[np];
    for (unsigned i0=0;i0<np;i0++) {
        primlost[i0] = true;
        replost[i0] = true;
    }
    bool ok = true;
    Owned<IDistributedFilePartIterator> piter = file->getIterator();
    ForEach(*piter) {
        IDistributedFilePart &part = piter->query();
        unsigned pn = part.getPartIndex();
        unsigned nc = part.numCopies();
        for (unsigned copy = 0; copy < nc; copy++) {
            RemoteFilename rfn;
            part.getFilename(rfn,copy);
            Owned<IFile> partfile = createIFile(rfn);
            StringBuffer eps;
            bool lost = true;
            try {
                if (partfile->exists()) {
                    if (copy>0)
                        replost[pn] = false;
                    else
                        primlost[pn] = false;
                    lost = false;
                }
            }
            catch (IException *e)
            {
                StringBuffer tmp("Checking file ");
                rfn.getRemotePath(tmp);
                EXCLOG(e, tmp.str());
                ok = false;
            }
            if (!ok)
                break;
            if (lost) {
                Owned<IPropertyTree> pt = createPTree("Part");
                StringBuffer tmp;
                rfn.queryEndpoint().getIpText(tmp);
                pt->setProp("Node",tmp.str());
                pt->setPropInt("Num",pn+1);
                if (copy>0)
                    pt->setPropInt("Replicate",copy);
                ft->addPropTree("Part",pt.getClear());
            }
        }
    }
    unsigned pc = 0;
    unsigned rc = 0;
    unsigned c = 0;
    for (unsigned i1=0;i1<np;i1++) {
        if (primlost[i1]&&replost[i1]) {
            pc++;
            rc++;
            c++;
        }
        else if (primlost[i1]) 
            pc++;
        else if (replost[i1])
            rc++;
    }
    delete [] primlost;
    delete [] replost;
    if (c) {
        PROGLOG("Adding %s to lost files",lfn.get());
        ft->addPropInt("Partslost",c);
        ft->addPropInt("Primarylost",pc);
        ft->addPropInt("Replicatedlost",rc);
        StringBuffer outstr;
        toXML(ft,outstr);
        printf("%s\n",outstr.str());
    }
}


void getAllFiles(const char *ip)
{
    RemoteFilename rfn;
    SocketEndpoint ep(ip);
    rfn.setPath(ep,"/c$");
    Owned<IFile> dir = createIFile(rfn);
    Owned<IDirectoryIterator> iter = dir->directoryFiles("*",true,false);
    StringBuffer name;
    unsigned n=0;
    IArrayOf<IFile> files;
    StringArray names;
    Int64Array sizes;
    ForEach(*iter) {
        files.append(iter->get());
        iter->getName(name.clear());
        names.append(name.str());
        sizes.append(iter->getFileSize());
    }
    iter.clear();
    while (files.ordinality()) {
        Owned<IFile> file = &files.popGet();
        name.clear().append(names.item(names.ordinality()-1));
        names.pop();
        CDateTime createTime; 
        CDateTime modifiedTime;
        CDateTime accessedTime;
        __int64 size = sizes.item(sizes.ordinality()-1);
        sizes.pop();
        if (file->getTime(&createTime, &modifiedTime, &accessedTime)) {
            name.append(',');
            createTime.getString(name);
            name.append(',');
            modifiedTime.getString(name);
            name.append(',');
            accessedTime.getString(name);
            name.append(',');
            name.append(size);
            printf("%s\n",name.str());
        }
    }
}


void testDaliSecurity()
{
    Owned<IUserDescriptor> user = createUserDescriptor();
    user->set("molly","triumphant");
    unsigned auditflags = DALI_LDAP_AUDIT_REPORT|DALI_LDAP_READ_WANTED;
    int perm = 255;
    const char *scopename = "polyphonic";
    if (scopename&&*scopename) {
        perm = querySessionManager().getPermissionsLDAP("workunit",scopename,user,auditflags);
        if (perm<0) {
            if (perm==-1) 
                perm = 255;
            else 
                perm = 0;
        }
        if (!HASREADPERMISSION(perm)) 
            printf("failed!\n");

    }
    printf("ok!\n");
}

/*
void tidyUpSoDir()
{
    Owned<IFile> idir;
    SocketEndpoint ep("10.173.64.201",getDaliServixPort());
    RemoteFilename rfn;
    rfn.setPath(ep,"/c$/thor/sodir");
    idir.setown(createIFile(rfn));
    Owned<IDirectoryIterator> iter = idir->directoryFiles("*.so",false,false);
    ForEach(*iter) {
        iter->getName(name.clear());
        const char *s = iter->query().queryFilename();
        printf("%s\n",n,name.str());
    }
}   
*/

bool compressRepeatedEscape(const char *value,StringBuffer &compressedout)
{
    const unsigned char *base = (const unsigned char *)value;
    loop {
        StringBuffer compressed;
        const unsigned char *s = base;
        while (*s) {
            if (((*s&0xc0)==0xc0)&&((s[1]&0xc0)==0x80)) {
                if (!compressed.length()) 
                    compressed.append(s-base,(const char *)base);
                compressed.append((char)(((*s&0x3f)<<6)+(s[1]&0x3f)));
                s++;
            }
            else if (compressed.length()) 
                compressed.append(*s);
            s++;
        }
        if (compressed.length()==0)
            break;
        compressedout.clear().swapWith(compressed);
        base = (const unsigned char *)compressedout.str();
    }
    return compressedout.length()!=0;
}

void XMLcheckfix(IPropertyTree &tree,StringBuffer &path,const char *name,const char *value, bool fix, bool check)
{
    if (!value)
        return;
    StringBuffer compressedout;
    if (compressRepeatedEscape(value,compressedout)) {
        PROGLOG("%s%s from %d to %d '%s'",path.str(),name,strlen(value),compressedout.length(),compressedout.str());
        if (fix||check) {
            try {
                Owned<IRemoteConnection> conn = querySDS().connect(path.str(), myProcessSession(), 0, 5*60*1000);  
                if (conn) {
                    IPropertyTree *root = conn->queryRoot();
                    const char *oldval = root->queryProp(name);
                    if (oldval) {
                        StringBuffer oldcompressed;
                        if (compressRepeatedEscape(oldval,oldcompressed)) {
                            if (strcmp(compressedout.str(),oldcompressed.str())==0) {
                                if (fix)
                                    root->setProp(name,compressedout.str());
                                PROGLOG("%s %s%s",fix?"Updated":"Would update",path.str(),name);
                            }
                            else
                                PROGLOG("Values differ for %s%s",path.str(),name);
                        }
                        else
                            PROGLOG("No change needed %s%s",path.str(),name);
                    }
                    else 
                        PROGLOG("Could not find %s%s",path.str(),name);
                }
                else
                    ERRLOG("Could not connect to %s",path.str());
            }
            catch (IException *e) {
                EXCLOG(e,"connecting");
            }
        }
    }
}

void XMLscanfix(IPropertyTree &tree,StringBuffer &path, bool fix, bool check)
{
    size32_t pl = path.length();
    // first scan attributes
    Owned<IAttributeIterator> ai = tree.getAttributes();
    ForEach(*ai) {
        XMLcheckfix(tree,path,ai->queryName(),ai->queryValue(),fix,check);
    }
    ai.clear();
    // then prop
    if (!tree.isBinary(NULL)) {
        const char *s = tree.queryProp(NULL);
        XMLcheckfix(tree,path,"",s,fix,check);
    }
    // then children
    Owned<IPropertyTreeIterator> iter = tree.getElements("*");
    unsigned idx=0;
    StringBuffer lastname;
    ForEach(*iter) {
        const char *name = iter->query().queryName();
        if (strcmp(name,lastname.str())==0)
            idx++;
        else {
            idx=1;
            lastname.clear().append(name);
        }
        path.appendf("%s[%d]/",name,idx);
        XMLscanfix(iter->query(),path,fix,check);
        path.setLength(pl);
    }
}

void XMLscanfix(const char *filename,bool fix,bool check)
{
    Owned<IPropertyTree> tree = createPTreeFromXMLFile(filename);
    StringBuffer path("/");
    XMLscanfix(*tree,path,fix,check);
}

void testLock(const char *lock,bool read)
{
    Owned<IRemoteConnection> conn = querySDS().connect(lock,myProcessSession(),read?RTM_LOCK_READ:RTM_LOCK_WRITE, 1000*60);
    if (!conn) 
        throw MakeStringException(DFSERR_ForeignDaliTimeout, "testLock: Timeout connecting to ", lock);
    Sleep(1000*60*10);
}

void testRFD()
{
    MemoryBuffer mb;
    RemoteFilename rfn;
    const char * machine = "edata11xxxxxxxb.br.seisint.com";
    const char * dir = "/";
    const char *mask = "*";
    bool sub = false;
    SocketEndpoint ep ; //(machine);
    rfn.setPath(ep,dir);
    Owned<IFile> f = createIFile(rfn);
    if (f) {
        StringBuffer s;
        StringBuffer ds;
        Owned<IDirectoryIterator> di = f->directoryFiles(mask,sub);
        if (di) {
            ForEach(*di) {
                di->getName(s.clear());
                __int64 fsz = di->getFileSize();
                CDateTime dt;
                di->getModifiedTime(dt);
                size32_t sz = s.length();
                dt.getString(ds.clear());
                ds.padTo(19);
                mb.append(sz).append(sz,s.str()).append(fsz).append(19,ds.str());
            }
        }
    }
    printf("res = %d",mb.length());
}

static unsigned clustersToGroups(IPropertyTree *envroot,const StringArray &cmplst,StringArray &cnames,StringArray &groups,bool *done)
{
    if (!envroot)
        return 0;
    for (int roxie=0;roxie<2;roxie++) {
        Owned<IPropertyTreeIterator> clusters= envroot->getElements(roxie?"RoxieCluster":"ThorCluster");
        unsigned ret = 0;
        ForEach(*clusters) {
            IPropertyTree &cluster = clusters->query();
            const char *name = cluster.queryProp("@name");
            if (name&&*name) {
                ForEachItemIn(i,cmplst) {
                    const char *s = cmplst.item(i);
                    assertex(s);
                    if ((strcmp(s,"*")==0)||WildMatch(name,s,true)) {
                        const char *group = cluster.queryProp("@nodeGroup");
                        if (!group||!*group)
                            group = name;
                        bool found = false;
                        ForEachItemIn(j,groups) 
                            if (strcmp(groups.item(j),group)==0)
                                found = true;
                        if (!found) {
                            cnames.append(name);
                            groups.append(group);
                            if (done)
                                done[i] =true;
                            break;
                        }
                    }
                }
            }
        }
    }
    return groups.ordinality();
}

void MPping(const char *eps)
{
    SocketEndpoint ep(eps);
    Owned<INode> node = createINode(ep);
    Owned<IGroup> grp = createIGroup(1,&ep);
    Owned<ICommunicator> comm = createCommunicator(grp,true);
    if (!comm->verifyConnection(0,60*1000)) 
        ERRLOG("MPping %s failed",eps);
    else
        PROGLOG("MPping %s succeeded",eps);
}

void clusterList()
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ, 1000*60);
    if (!conn) {
        ERRLOG("Could not connect to /Environment/Software");
        return;
    }
    StringArray list;
    list.append("*");
    StringArray groups;
    StringArray cnames;
    bool *done = (bool *)calloc(list.ordinality(),sizeof(bool));
    clustersToGroups(conn->queryRoot(),list,cnames,groups,done);
    free(done);
    ForEachItemIn(i,cnames) {
        printf("%s,%s\n",cnames.item(i),groups.item(i));
    }
}


void listRedirection()
{
    IDFSredirection &red = queryDistributedFileDirectory().queryRedirection();
    StringBuffer pat;
    StringBuffer repl;
    for (unsigned i=0;red.getEntry(i,pat.clear(),repl.clear());i++) {
        printf("%d: %s => %s\n",i,pat.str(),repl.str());
    }
}


void addRedirection(const char *pat,const char *repl,unsigned pos)
{
    IDFSredirection &red = queryDistributedFileDirectory().queryRedirection();
    red.update(pat,repl,pos);
}

void lookupRedirection(const char *lfn)
{
    IDFSredirection &red = queryDistributedFileDirectory().queryRedirection();
    Owned<IDfsLogicalFileNameIterator> match = red.getMatch(lfn);
    printf("%s -> \n",lfn);
    ForEach(*match) {
        const char *fn = match->query().get();
        printf("   %s\n",fn);
    }
}



int main(int argc, char* argv[])
{
    
    enableMemLeakChecking(true);

    InitModuleObjects();
    EnableSEHtoExceptionMapping();
    setDaliServixSocketCaching(true);
    StringBuffer dalieps(argv[1]);
    if (argc<2) {
        usage(argv[0]);
        return -1;
    }


    const char *at = strchr(argv[1],'@');
    unsigned myport = 0;
    if (at) {
        dalieps.setLength(at-argv[1]);
        myport = atoi(at+1);
    }
    bool done = false;
    try {

        //testSetPath();
        StringBuffer logName("sdsfix");
        if (myport)
            logName.append('_').append(myport);
        StringBuffer aliasLogName(logName);
        aliasLogName.append(".log");
        ILogMsgHandler *fileMsgHandler = getRollingFileLogMsgHandler(logName.str(), ".log", MSGFIELD_STANDARD, false, true, NULL, aliasLogName.str());
        queryLogMsgManager()->addMonitorOwn(fileMsgHandler, getCategoryLogMsgFilter(MSGAUD_all, MSGCLS_all, TopDetail));
        queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_prefix);

        if ((argc==2) && (stricmp(argv[1],"coalesce")==0))
        {
            coalesceStore();
            done = true;
        }
        if ((argc==3) && (stricmp(argv[1],"gettags")==0))
        {
                getTags(argv[2]);
                done = true;
        }
        if ((argc>=3) && (stricmp(argv[2],"testconnect")==0))
        {
            testDaliClientConnect(argv[1],(argc>3)?atoi(argv[3]):1000);
            done = true;
        }
        if ((argc>=2) && (stricmp(argv[1],"extchk")==0))
        {
            checkExternals(argc-2, &argv[2]);
            return 0;
        }
        else if ((argc>2)  && (stricmp(argv[1],"xmlsize")==0))
        {
            XmlSize((argc>=3)?argv[2]:NULL,(argc>=4)?atof(argv[3]):1.0);
            done = true;
        }
        else if ((argc>2)  && (stricmp(argv[1],"xmlpatch")==0))
        {
            XmlPatch(argv[2],argc>=4?&argv[3]:NULL);
            done = true;
        }
        else if ((argc==4)  && (stricmp(argv[1],"xmlformat")==0))
        {
            xmlFormat(argv[2],argv[3]);
            done = true;
        }
        else if ((argc==3)  && (stricmp(argv[1],"validatenodes")==0))
        {
            SocketEndpointArray epa;
            epa.fromText(argv[2],getDaliServixPort());
            SocketEndpointArray failures;
            UnsignedArray failedcodes;
            StringArray failedmessages;
            validateNodes(epa,true,true,true,NULL,0,failures,failedcodes,failedmessages);
            ForEachItemIn(i,failures) {
                StringBuffer tmp;
                printf("FAILED(%d) %s : %s\n",failedcodes.item(i),failures.item(i).getUrlStr(tmp).str(),failedmessages.item(i));
            }
            done = true;
        }

/*
        else if ((argc==4)  && (stricmp(argv[1],"xmlscan")==0))
        {
            xmlScan(argv[2],argv[3],false);
            done = true;
        }
        else if ((argc==4)  && (stricmp(argv[1],"xmlmemscan")==0))
        {
            xmlScan(argv[2],argv[3],true);
            done = true;
        }
        else if ((argc==5)  && (stricmp(argv[1],"xmlcompare")==0))
        {
            xmlCompare(argv[2],argv[3],argv[4]);
            done = true;
        }
        else if ((argc>2)  && (stricmp(argv[1],"xmlmemsize")==0))
        {
            XmlMemSize((argc>=3)?argv[2]:NULL);
            done = true;
        }
*/
        else if ((argc==5)  && (stricmp(argv[1],"multidali")==0))
        {
            testMultiDali(argv[2],argv[3],argv[4]);
            done = true;
        }
        else if ((argc>1)  && (stricmp(argv[1],"testrfd")==0))
        {
            testRFD();
            done = true;
        }
    }
    catch (IException *e) {
        pexception("Exception",e);
        e->Release();
        done = true;
    }


    unsigned ret = 0;
    if (!done) {
        
        SocketEndpoint ep;
        SocketEndpointArray epa;
        ep.set(dalieps.str(),DALI_SERVER_PORT);
        epa.append(ep);
        Owned<IGroup> group = createIGroup(epa); 


        try {
            initClientProcess(group, DCR_Util, myport);

        //  testDaliSecurity();


            const char *arg = argv[2];
            bool nextif=false; // kludge for compiler limit
            if ((argc==4) && (stricmp(arg,"delete")==0))
            {
                deleteBranch(argv[3],true);
            }
            else if ((argc==4) && (stricmp(arg,"fdelete")==0))
            {
                deleteFileBranch(argv[3],true);
            }
            else if ((argc==4) && (stricmp(arg,"xdelete")==0))
            {
                deleteBranch(argv[3],false);
            }
            else if ((argc==5) && (stricmp(arg,"set")==0))
            {
                setProp(argv[3],argv[4]);
            }
            else if ((argc==5) && (stricmp(arg,"add")==0))
            {
                setProp(argv[3],argv[4]);
            }
            else if ((argc==4) && (stricmp(arg,"get")==0))
            {
                getProp(argv[3]);
            }
            else if ((argc==4) && (stricmp(arg,"delp")==0))
            {
                delProp(argv[3]);
            }
            else if ((argc==5) && (stricmp(arg,"bget")==0))
            {
                getPropBin(argv[3],argv[4]);
            }
            else if ((argc==4) && (stricmp(arg,"xget")==0))
            {
                xgetProp(argv[3]);
            }
            else if ((argc==4) && (stricmp(arg,"wget")==0))
            {
                wgetProp(argv[3]);
            }
            else if ((argc==5) && (stricmp(arg,"export")==0))
            {
                _export_(argv[3],argv[4]);
            }
            else if ((argc==4) && (stricmp(arg,"branchdir")==0))
            {
                branchdir(argv[3]);
            }
            else if ((argc==5) && (stricmp(arg,"safeexport")==0))
            {
                _export_(argv[3],argv[4],true);
            }
            else if ((argc==5) && (stricmp(arg,"import")==0))
            {
                import(argv[3],argv[4],false);
            }
            else if ((argc==5) && (stricmp(arg,"bimport")==0))
            {
                bimport(argv[3],argv[4],false);
            }
            else if ((argc==5) && (stricmp(arg,"importadd")==0))
            {
                import(argv[3],argv[4],true);
            }
            else if ((argc>=3)  && (stricmp(arg,"dfsattr")==0))
            {
                dfsAttr((argc>=4)?argv[3]:NULL,(argc>=6)?argv[4]:NULL,(argc>=6)?argv[5]:NULL);
            }
            else if ((argc==5)  && (stricmp(arg,"dfsrename")==0))
            {
                testDfsRename(argv[3],argv[4]);
            }
            else if ((argc>=3)  && (stricmp(arg,"dfscsv")==0))
            {
                dfsCsv((argc>=4)?argv[3]:NULL,(argc>=6)?argv[4]:NULL,(argc>=6)?argv[5]:NULL);
            }
            else if (((argc==4)||(argc==5))  && (stricmp(arg,"dfsfile")==0))
            {
                dfsFile(argv[3],(argc==5)?argv[4]:NULL,false);
            }
            else if ((argc==4) && (stricmp(arg,"dfscluster")==0))
            {
                dfsCluster(argv[3]);
            }
            else if (((argc==4)||(argc==5))  && (stricmp(arg,"lookupfile")==0))
            {
                dfsFile(argv[3],(argc==5)?argv[4]:NULL,true);
            }
            else if (((argc==4)||(argc==5))  && (stricmp(arg,"lookupgroup")==0))
            {
                lookupGroup(argv[3],(argc==5)?argv[4]:NULL);
            }
            else if ((argc==4)  && (stricmp(arg,"lookuptest")==0))
            {
                lookuptest(argv[3]);
            }
            else if ((argc==5)  && (stricmp(arg,"roxiefile")==0))
            {
                testRoxieFile(argv[3],argv[4]);
            }
            else if ((argc==4)  && (stricmp(arg,"dfssize")==0))
            {
                dfsSize(argv[3]);
            }
            else if ((argc==4)  && (stricmp(arg,"clusterfiles")==0))
            {
                dfsClusterFiles(argv[3]);
            }
            else if ((argc==4)  && (stricmp(arg,"unlinksubfile")==0))
            {
                unlinkSubFile(argv[3]);
            }
            else if ((argc==4)  && (stricmp(arg,"dfsfilelist")==0))
            {
                dfsFileList(argv[3]);
            }
            else if ((argc==4)  && (stricmp(arg,"dfscompressed")==0))
            {
                dfsCompressed(argv[3]);
            }
            else if ((argc==4)  && (stricmp(arg,"superowners")==0))
            {
                dfsSuperParents(argv[3]);
            }
            else if (((argc==4)||(argc==5))  && (stricmp(arg,"dfsdesc")==0))
            {
                dfsDesc(argv[3],(argc==5)?argv[4]:NULL);
            }
            else if ((argc==5)  && (stricmp(arg,"dfsmultidesc")==0))
            {
                dfsMultiDesc(argv[3],atoi(argv[4]));
            }
            else if (((argc==4)||(argc==5))  && (stricmp(arg,"dfscheck")==0))
            {
                dfsCheck(argv[3],(argc==5)?argv[4]:NULL);
            }
            else if ((argc==4)  && (stricmp(arg,"replicatemap")==0))
            {
                replicateMap(argv[3]);
            }
            else if ((argc==4) && (stricmp(arg,"filetree")==0))
            {
                fileTree(argv[3]);
            }
            else if ((argc>=3)  && (stricmp(arg,"sdssize")==0))
            {
                __int64 is=0;
                __int64 es=0;
                sizebranch(0,(argc==3)?NULL:argv[3],is,es);
            }
            else if ((argc==3)  && (stricmp(arg,"cleardfu")==0))
            {
                clearDFURecovery();
            }
            else if ((argc==3)  && (stricmp(arg,"runningdfu")==0))
            {
                displayDFURecovery();
            }
            else if ((argc>=3)  && (stricmp(arg,"listworkunits")==0))
            {
                printWorkunits((argc>3)?argv[3]:NULL,(argc>4)?argv[4]:NULL,(argc>5)?argv[5]:NULL);
            }
            else if ((argc==4)  && (stricmp(arg,"workunittimings")==0))
            {
                printWorkunitTimings(argv[3]);
            }
            else if ((argc==4)  && (stricmp(arg,"qprobe")==0))
            {
                probeQ(argv[3]);
            }
            else if ((argc==5)  && (stricmp(arg,"nudgethor")==0))
            {
                //nudgeThor(argv[3],argv[4]);
            }
            else if ((argc>3)  && (stricmp(arg,"persists")==0))
            {
                persistsList(argv[3]);
            }
            else if ((argc>=4)  && (stricmp(arg,"crclist")==0))
            {
                crcList(argv[3],argv[4]);
            }
            else if ((argc>2)  && (stricmp(arg,"initclustergroups")==0))
            {
                initClusterGroups();
            }
            else if ((argc>2)  && (stricmp(arg,"fixin")==0))
            {
                fixIN();
            }
    /*
            else if ((argc>2)  && (stricmp(arg,"checkcrc")==0))
            {
                checkCrc();
            }
    */
            else if ((argc>=4)  && (stricmp(arg,"checkfilecrc")==0))
            {
                FILE* out = (argc==4)?NULL:fopen(argv[4], "wb+");
                verifyFile(out,argv[3],0);
                if (out)
                    fclose(out);
            }
            else if ((argc==4)  && (stricmp(arg,"checkscopes")==0))
            {
                checkScopes((strcmp(argv[3],".")!=0)?argv[3]:NULL,false);
            }
            else if ((argc==3)  && (stricmp(arg,"allscopes")==0))
            {
                checkScopes(NULL,true);
            }
            else if ((argc==3)  && (stricmp(arg,"cleanscopes")==0))
            {
                cleanScopes();
            }
            else if ((argc==3)  && (stricmp(arg,"dfsallparts")==0))
            {
                dfsPartList();
            }
            else if ((argc>=5)  && (stricmp(arg,"checkfilescrc")==0))
            {
                verifyFiles(argv[3],argv[4],(argc>5)?atoi(argv[5]):0);
            }
            else if ((argc==3)  && (stricmp(arg,"fixdates")==0))
            {
                fixDates();
            }
            else if ((argc==3)  && (stricmp(arg,"fillsds")==0))
            {
                fillSDS();
            }
            else if ((argc==4)  && (stricmp(arg,"dfsexists")==0))
            {
                ret = dfsExists(argv[3]);
                printf("\"%s\" %s\n",argv[3],(ret==0)?"exists":"does not exist");
            }
            else if ((argc==4)  && (stricmp(arg,"timelookup")==0))
            {
                timeLookup(argv[3]);
            }
            else if ((argc==4)  && (stricmp(arg,"dfsmodified")==0))
            {
                dfsModified(argv[3]);
            }
            else 
                nextif = true;

            if (!nextif) {}     // kludge for compiler limit
            else if ((argc==4)  && (stricmp(arg,"extracttimings")==0))
            {
                if (stricmp(argv[3],"*")==0) {
                    Owned<IRemoteConnection> conn = querySDS().connect("/", myProcessSession(), 0, 5*60*1000);  
                    Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("WorkUnits/*");
                    ForEach(*iter) {
                        IPropertyTree &pt=iter->query();
                        try {
                            PROGLOG("Processing WUID: %s",pt.queryName());
                            DumpWorkunitTimings(&pt);
                        }
                        catch (IException *e) {
                                EXCLOG(e,"extracttimings");
                                e->Release();
                        }
                    }
                }
                else {
                    Owned<IPropertyTree> pt = createPTreeFromXMLFile(argv[3]);
                    DumpWorkunitTimings(pt);
                }
            }
            else if ((argc>=5)  && (stricmp(arg,"auditlog")==0))
            {
                CDateTime from;
                from.setDateString(argv[3]);
                CDateTime to;
                to.setDateString(argv[4]);
                unsigned start = msTick();
                StringAttrArray res;
                queryAuditLogs(from,to,(argc>5)?argv[5]:NULL,res);
                ForEachItemIn(i,res) 
                    printf("%s\n",res.item(i).text.get());
                printf("Took %dms\n",msTick()-start);
            }
            else if ((argc>=5)  && (stricmp(arg,"listcrossclusterreads")==0))
            {
                listCrossClusterReads(argv[3],argv[4]);
            }
            else if ((argc==4)  && (stricmp(arg,"sdsexists")==0))
            {
                StringBuffer out;
                ret = sdsExists(argv[3],out);
                if (ret==0)
                    printf("\"%s\" exists\n",out.str());
                else
                    printf("\"%s\" does not exist\n",argv[3]);
            }
            else if ((argc>=4)  && (stricmp(arg,"checksuperfile")==0))
            {
                checkSuperFile(argv[3],(argc==5)&&(stricmp(argv[4],"fix")==0));
            }
            else if ((argc==4)  && (stricmp(arg,"checksubfile")==0))
            {
                checkSubFile(argv[3],false);
            }
            else if ((argc==4)  && (stricmp(arg,"fixcompsize")==0))
            {
                Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(argv[3]);
                if (!file) 
                    ERRLOG("%s not found",argv[3]);
                else
                    fixCompSize(argv[3],file);
            }
            else if ((argc==3)  && (stricmp(arg,"fixcompsizes")==0))
            {
                fixCompSizes();
            }
            else if ((argc==3)  && (stricmp(arg,"fixsupersizes")==0))
            {
                fixSuperSizes();
            }
            else if ((argc==3)  && (stricmp(arg,"testsuperwidths")==0))
            {
                testSuperWidths();
            }
            else if ((argc==4)  && (stricmp(arg,"fixsupersize")==0))
            {
                fixSuperSizeTest(argv[3]);
            }
            else if ((argc==3)  && (stricmp(arg,"listwuorphans")==0))
            {
                listWuOrphans(false);
            }
            else if ((argc==3)  && (stricmp(arg,"delwuorphans")==0))
            {
                listWuOrphans(true);
            }
            else if ((argc==6)  && (stricmp(arg,"dfsperms")==0))
            {
                ret = dfsPerms(argv[3],argv[4],argv[5]);
            }
            else if ((argc==5)  && (stricmp(arg,"dfslockread")==0))
            {
                ret = testFileLock(argv[3],atoi(argv[4]),false);
            }
            else if ((argc==5)  && (stricmp(arg,"dfslockwrite")==0))
            {
                ret = testFileLock(argv[3],atoi(argv[4]),true);
            }
            else if ((argc==3)  && (stricmp(arg,"pruneattecl")==0))
            {
                pruneAttEcl();
            }
            else if ((argc==3)  && (stricmp(arg,"wusizes")==0))
            {
                PrintWUsizes();
            }
            else if ((argc==3)  && (stricmp(arg,"testdalidown")==0))
            {
                testDaliDown();
            }
            else if ((argc==3)  && (stricmp(arg,"queuedump")==0))
            {
                dumpQueues();
            }
            else if ((argc==3)  && (stricmp(arg,"badwus")==0))
            {
                printBadWorkUnits();
            }
            else if ((argc==3)  && (stricmp(arg,"checksuperlinks")==0))
            {
                checkSuperFileLinkage();
            }
            else if ((argc==3)  && (stricmp(arg,"cleanrootfiles")==0))
            {
                cleanRootFiles();
            }
            else if ((argc==4)  && (stricmp(arg,"dirtocsv")==0))
            {
                dirtocsv(argv[3]);
            }
            else if ((argc==4)  && (stricmp(arg,"testwufiles")==0)) {
                testwufiles(argv[3]);
            }
            else if ((argc==8)  && (stricmp(arg,"testpagedget")==0))
            {
                testPagedGet(argv[3],argv[4],argv[5],atoi(argv[6]),atoi(argv[7]));
            }
            else if ((argc==7)  && (stricmp(arg,"maketestfile")==0))
            {
                makeTestFile(argv[3],argv[4],atoi64_l(argv[5],strlen(argv[5])),atoi(argv[6]));
            }
            else if ((argc==3)  && (stricmp(arg,"fixtilde")==0))
            {
                fixTilde();
            }
            else if ((argc==3)  && (stricmp(arg,"fileattrlist")==0))
            {
                fileAttrList();
            }
            else if ((argc==3)  && (stricmp(arg,"testfd")==0))
            {
                testFileDescriptor();
            }
            else if ((argc==3)  && (stricmp(arg,"subfiles")==0))
            {
                printSubfiles();
            }
            else if ((argc>=4)  && (stricmp(arg,"checkclustercrcs")==0)) 
            {
                bool verbose = (argc>4)&&(stricmp(argv[4],"verbose")==0);
                int csvarg = 4;
                if (verbose)
                    csvarg = 5;
                else
                    verbose = (argc>5)&&(stricmp(argv[5],"verbose")==0);
                checkClusterCRCs(argv[3],true,verbose,(argc>csvarg)?argv[csvarg]:NULL);
            }
            else if ((argc==4)  && (stricmp(arg,"checkfilecrcs")==0)) 
            {
                checkClusterCRCs(NULL,true,true,NULL,argv[3]);
            }
            else if ((argc>=5)  && (stricmp(arg,"filecompare")==0))
            {
                testLogicalFileCompare(argv[3],argv[4],(argc>5)?atoi(argv[5]):0);
            }
            else if ((argc==3)  && (stricmp(arg,"cleangenerateddlls")==0))
            {
                cleanGeneratedDlls(false);
            }
            else if ((argc==3)  && (stricmp(arg,"checkgenerateddlls")==0))
            {
                cleanGeneratedDlls(true);
            }
            else if ((argc==3)  && (stricmp(arg,"findanonymousclusters")==0))
            {
                findAnonymousClusters(false);
            }
            else if ((argc==3)  && (stricmp(arg,"fixanonymousclusters")==0))
            {
                findAnonymousClusters(true);
            }
/*
            else if ((argc==5)  && (stricmp(arg,"thorusage")==0))
            {
                clusterThorUsage(argv[3],atoi(argv[4]));
            }
            else if ((argc==7)  && (stricmp(arg,"thorlogusage")==0))
            {
                clusterThorUsageLog(argv[3],argv[4],argv[5],argv[6]);
            }
*/
            else if ((argc==4)  && (stricmp(arg,"verifyfile")==0))
            {
                testLogicalFileVerify(argv[3],true);
            }
            else if ((argc==4)  && (stricmp(arg,"testdalioverload")==0))
            {
                testDaliOverload(argv[3]);
            }
            
            else if ((argc==4)  && (stricmp(arg,"testfilebackupcrash")==0))
            {
                testFileBackupCrash(argv[3]);
            }
            else if ((argc==4)  && (stricmp(arg,"filetime")==0))
            {
                fileTime(argv[3]);
            }
            else if ((argc>=4)  && (stricmp(arg,"testmutex")==0))
            {
                testMutex(argv[3],(argc>4)?atoi(argv[4]):(unsigned)-1,(argc>5)?atoi(argv[5]):1000);
            }
            else if ((argc==3)  && (stricmp(arg,"testmemlimit")==0))
            {
                testLMlimit();
            }
            else if ((argc==4)  && (stricmp(arg,"testserializefile")==0))
            {
                testSerializeFile(argv[3]);
            }
            else if ((argc==4)  && (stricmp(arg,"getdfuxref")==0))
            {
                getDFUXREF(argv[3]);
            }
            else if ((argc==4)  && (stricmp(arg,"testbinsz")==0))
            {
                testbinsz(argv[3]);
            }
            else if ((argc==5)  && (stricmp(arg,"sdsgetremote")==0))
            {
                testGetTreeRaw(argv[3],argv[4]);
            }
            else if ((argc==5)  && (stricmp(arg,"getftslaveloc")==0))
            {
                testFTSlaveLocation(argv[3],argv[4]);
            }
/*
            else if ((argc==4)  && (stricmp(arg,"testxreffile")==0))
            {
                testXrefFile(argv[3]);
            }
            else if ((argc==5)  && (stricmp(arg,"testclustermutex")==0))
            {
                testClusterMutex(argv[3],argv[4],myport);
            }
*/
            else if ((argc==3)  && (stricmp(arg,"thorrunningwus")==0))
            {
                testThorRunningWUs();
            }
            else if ((argc==3)  && (stricmp(arg,"testnamedmutex")==0))
            {
                testNamedMutex();
            }
            else if ((argc==3)  && (stricmp(arg,"listwufiles")==0))
            {
                listWorkUnitAssociatedFiles();
            }
            else if ((argc==4)  && (stricmp(arg,"dfssetaccessed")==0))
            {
                testSetAccessed(argv[3]);
            }
            else if ((argc==4)  && (stricmp(arg,"testlostfile")==0))
            {
                testLostFile(argv[3]);
            }
            else if ((argc==4)  && (stricmp(arg,"getallfiles")==0))
            {
                getAllFiles(argv[3]);
            }
            else if ((argc==3)  && (stricmp(arg,"listfileclusters")==0))
            {
                listFileClusters();
            }
            else if ((argc==3)  && (stricmp(arg,"listsupersub")==0))
            {
                testSuperSubList();
            }
            else if ((argc==4)  && (stricmp(arg,"compratio")==0))
            {
                fileCompressionRatio(argv[3]);
            }
            else if ((argc==4)  && (stricmp(arg,"replicate")==0))
            {
                testReplicate(argv[3]);
            }
            else if ((argc==3)  && (stricmp(arg,"testcompfiles")==0))
            {
                testCompressedFileSizes();
            }
            else if ((argc==3)  && (stricmp(arg,"dfsgroups")==0))
            {
                dfsGroups();
            }
            else if ((argc==3)  && (stricmp(arg,"opwarning")==0))
            {
                FLLOG(MCoperatorWarning, "nhTEST");
            }
            else if ((argc==4)  && ((stricmp(arg,"validatenodes")==0)||(stricmp(arg,"validatedafs")==0)))
            {
                bool valdisk = stricmp(arg,"validatenodes")==0;
                SocketEndpointArray epa;
                Owned<IGroup> grp = queryNamedGroupStore().lookup(argv[3]);
                if (grp) {
                    grp->getSocketEndpoints(epa);
                    ForEachItemIn(i1,epa) {
                        epa.item(i1).port = getDaliServixPort();
                    }
                    
                    SocketEndpointArray failures;
                    UnsignedArray failedcodes;
                    StringArray failedmessages;
                    unsigned start = msTick();
                    validateNodes(epa,valdisk,valdisk,true,NULL,0,failures,failedcodes,failedmessages);
                    ForEachItemIn(i,failures) {
                        StringBuffer tmp;
                        printf("FAILED(%d) %s : %s\n",failedcodes.item(i),failures.item(i).getUrlStr(tmp).str(),failedmessages.item(i));
                    }
                    printf("Time taken = %dms\n",msTick()-start);
                }
                else
                    printf("ERROR: Group %s not found\n",argv[3]);

            }
            else if ((argc>=4)  && (stricmp(arg,"xmlscanfix")==0))
            {
                XMLscanfix(argv[3],(argc>4)?(stricmp(argv[4],"fix")==0):false,(argc>4)?(stricmp(argv[4],"check")==0):false);
            }
            else if ((argc==4)  && (stricmp(arg,"testlockread")==0))
            {
                testLock(argv[3],true);
            }
            else if ((argc==4)  && (stricmp(arg,"testlockwrite")==0))
            {
                testLock(argv[3],false);
            }
            else if ((argc==3)  && (stricmp(arg,"clusterlist")==0))
            {
                clusterList();
            }
            else if ((argc>=4)  && (stricmp(arg,"changefilescluster")==0))
            {
                changeClusterName(argv[3],(argc>4)?argv[4]:NULL);
            }
            else if ((argc==4)  && (stricmp(arg,"mpping")==0))
            {
                MPping(argv[3]);
            }
            else if ((argc==3)  && (stricmp(arg,"compressparts")==0))
            {
                compressParts();
            }
            else if ((argc==3)  && (stricmp(arg,"pruneprotected")==0))
            {
                pruneProtectedWorkunits();
            }
            else if ((argc==3)  && (stricmp(arg,"listredirection")==0))
            {
                listRedirection();
            }
            else if ((argc>4)  && (stricmp(arg,"addredirection")==0))
            {
                addRedirection(argv[3],argv[4],(argc>5)?atoi(argv[5]):-1);
                listRedirection();
            }
            else if ((argc==4)  && (stricmp(arg,"lookupredirection")==0))
            {
                lookupRedirection(argv[3]);
            }
            
            else {
                usage(argv[0]);
                printf("%d: %s\n",argc,arg);
            }
        }
        catch (IException *e) {
            pexception("SDSFIX",e);
            e->Release();
        }

        closedownClientProcess();
    }
    setDaliServixSocketCaching(false);
    setNodeCaching(false);
    releaseAtoms();
    return ret;
}

