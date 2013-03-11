/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#include "platform.h"
#include "portlist.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jptree.hpp"
#include "jarray.hpp"
#include "jencrypt.hpp"
#include "jregexp.hpp"
#include "jptree.hpp"
#include "jlzw.hpp"
#include "jexcept.hpp"
#include "jset.hpp"
#include "jprop.hpp"

#include "mpbase.hpp"
#include "mpcomm.hpp"

#include "daclient.hpp"
#include "dadiags.hpp"
#include "danqs.hpp"
#include "dadfs.hpp"
#include "dasds.hpp"
#include "dautils.hpp"
#include "daaudit.hpp"
#include "daft.hpp"

#include "rmtfile.hpp"

#include "workunit.hpp"

#ifdef _WIN32
#include <conio.h>
#else
#define _getch getchar
#define _putch putchar
#endif

#define DEFAULT_DALICONNECT_TIMEOUT 5 // seconds
static unsigned daliConnectTimeoutMs = 5000;

static bool noninteractive=false;

void usage(const char *exe)
{
  printf("Usage:\n");
  printf("  %s [<daliserver-ip>] <command> { <option> }\n", exe);              
  printf("\n");
  printf("Data store commands:\n");
  printf("  export <branchxpath> <destfile>\n");
  printf("  import <branchxpath> <srcfile>\n");
  printf("  importadd <branchxpath> <srcfile>\n");
  printf("  delete <branchxpath>\n");
  printf("  set <xpath> <value>        -- set single value\n");
  printf("  get <xpath>                -- get single value\n");
  printf("  bget <xpath> <dest-file>   -- binary property\n");
  printf("  xget <xpath>               -- (multi-value tail can have commas)\n");
  printf("  wget <xpath>               -- (gets all matching xpath)\n");
  printf("  add <xpath> <value>        -- adds new value\n");
  printf("  delv <xpath>               -- deletes value\n");
  printf("  count <xpath>              -- counts xpath matches\n");
  printf("\n");
  printf("Logical File meta information commands:\n");
  printf("  dfsfile <logicalname>          -- get meta information for file\n");
  printf("  dfspart <logicalname> <part>   -- get meta information for part num\n");
  printf("  dfscsv <logicalnamemask>       -- get csv info. for files matching mask\n");
  printf("  dfsgroup <logicalgroupname> [filename] -- get IPs for logical group (aka cluster). Written to optional filename if provided\n");
  printf("  dfsmap <logicalname>           -- get part files (primary and replicates)\n");
  printf("  dfsexists <logicalname>        -- sets return value to 0 if file exists\n");
  printf("  dfsparents <logicalname>       -- list superfiles containing file\n");
  printf("  dfsunlink <logicalname>        -- unlinks file from all super parents\n");
  printf("  dfsverify <logicalname>        -- verifies parts exist, returns 0 if ok\n");
  printf("  setprotect <logicalname> <id>  -- overwrite protects logical file\n");
  printf("  unprotect <logicalname> <id>   -- unprotect (if id=* then clear all)\n");
  printf("  listprotect <logicalnamemask>  <id-mask> -- list protected files\n");
  printf("  checksuperfile <superfilename> [fix=true|false] -- check superfile links consistent and optionally fix\n");
  printf("  checksubfile <subfilename>     -- check subfile links to parent consistent\n");
  printf("  listexpires <logicalnamemask>  -- lists logical files with expiry value\n");
  printf("  listrelationships <primary> <secondary>\n");
  printf("  dfsperm <logicalname>           -- returns LDAP permission for file\n");
  printf("  dfscompratio <logicalname>      -- returns compression ratio of file\n");
  printf("  dfsscopes <mask>                -- lists logical scopes (mask = * for all)\n");
  printf("  cleanscopes                     -- remove empty scopes\n");
  printf("\n");
  printf("Workunit commands:\n");
  printf("  listworkunits <workunit-mask> [<prop>=<val> <lower> <upper>]\n");
  printf("  listmatches <connection xpath> [<match xpath>=<val> [<property xpath>]]\n");
  printf("  workunittimings <WUID>\n");
  printf("\n");
  printf("Other dali server and misc commands:\n");
  printf("  serverlist <mask>               -- list server IPs (mask optional)\n");
  printf("  clusterlist <mask>              -- list clusters   (mask optional)\n");
  printf("  auditlog <fromdate> <todate> <match>\n");
  printf("  coalesce                        -- force transaction coalesce\n");
  printf("  mpping <server-ip>              -- time MP connect\n");
  printf("  daliping [ <num> ]              -- time dali server connect\n");
  printf("  getxref <destxmlfile>           -- get all XREF information\n");
  printf("  dalilocks [ <ip-pattern> ] [ files ] -- get all locked files/xpaths\n");
  printf("  unlock <xpath or logicalfile>   --  unlocks either matching xpath(s) or matching logical file(s), can contain wildcards\n");
  printf("  wuidcompress <wildcard> <type>  --  scan workunits that match <wildcard> and compress resources of <type>\n");
  printf("  wuiddecompress <wildcard> <type> --  scan workunits that match <wildcard> and decompress resources of <type>\n");
  printf("  xmlsize <filename> [<percentage>] --  analyse size usage in xml file, display individual items above 'percentage' \n");
  printf("\n");
  printf("Common options\n");
  printf("  server=<dali-server-ip>         -- server ip\n");
  printf("                                  -- can be 1st param if numeric ip (or '.')\n");
  printf("  user=<username>                 -- for file operations\n");
  printf("  password=<password>             -- for file operations\n");
  printf("  logfile=<filename>              -- filename blank for no log\n");
  printf("  rawlog=0|1                      -- if raw omits timestamps etc\n");
  printf("  timeout=<seconds>               -- set dali connect timeout\n");
}

#define SDS_LOCK_TIMEOUT  60000

static void outln(const char *ln)
{
    PROGLOG("%s",ln);
}

#define OUTLOG PROGLOG


static const char *remLeading(const char *s)
{
    if (*s=='/')
        s++;
    return s;
}

static bool isWild(const char *path)
{
    if (strchr(path,'?')||strchr(path,'*'))
        return true;
    return false;
}


static const char *splitpath(const char *path,StringBuffer &head,StringBuffer &tmp)
{
    if (path[0]!='/')
        path = tmp.append('/').append(path).str();
    return splitXPath(path, head);
}

// NB: there's strtoll under Linux
static unsigned __int64 hextoll(const char *str, bool &error)
{
    unsigned len = strlen(str);
    if (!len) return 0;

    unsigned __int64 factor = 1;
    unsigned __int64 rolling = 0;
    char *ptr = (char *)str+len-1;
    loop {
        char c = *ptr;
        unsigned v;
        if (isdigit(c))
            v = c-'0';
        else if (c>='A' && c<='F')
            v = 10+(c-'A');
        else if (c>='a' && c<='f')
            v = 10+(c-'a');
        else {
            error = true;
            return 0;
        }
        rolling += v * factor;
        factor <<= 4;
        if (ptr == str)
            break;
        --ptr;
    }
    error = false;
    return rolling;
}


static IRemoteConnection *connectXPathOrFile(const char *path,bool safe,StringBuffer &xpath)
{
    CDfsLogicalFileName lfn;
    StringBuffer lfnpath;
    if ((strstr(path,"::")!=NULL)&&!strchr(path,'/')) {
        lfn.set(path);
        lfn.makeFullnameQuery(lfnpath,DXB_File);
        path = lfnpath.str();
    }
    else if (strchr(path+((*path=='/')?1:0),'/')==NULL)
        safe = true;    // all root trees safe
    Owned<IRemoteConnection> conn = querySDS().connect(remLeading(path),myProcessSession(),safe?0:RTM_LOCK_READ, daliConnectTimeoutMs);
    if (!conn&&lfnpath.length()) {
        lfn.makeFullnameQuery(lfnpath.clear(),DXB_SuperFile);
        path = lfnpath.str();
        conn.setown(querySDS().connect(remLeading(path),myProcessSession(),safe?0:RTM_LOCK_READ, daliConnectTimeoutMs));
    }
    if (conn.get())
        xpath.append(path);
    return conn.getClear();
}


//=============================================================================

static void _export_(const char *path,const char *dst,bool safe=false)
{
    StringBuffer xpath;
    Owned<IRemoteConnection> conn = connectXPathOrFile(path,safe,xpath);
    if (!conn) {
        ERRLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    Owned<IFile> f = createIFile(dst);
    Owned<IFileIO> io = f->open(IFOcreate);
    Owned<IFileIOStream> fstream = createBufferedIOStream(io);
    toXML(root, *fstream);          // formatted (default)
    OUTLOG("Branch %s saved in '%s'",xpath.str(),dst);
    conn->close();
}

//==========================================================================================================

static void import(const char *path,const char *src,bool add)
{
    Owned<IFile> iFile = createIFile(src);
    Owned<IFileIO> iFileIO = iFile->open(IFOread);
    if (!iFileIO)
    {
        ERRLOG("Could not open to %s",src);
        return;
    }
    size32_t sz = (size32_t)iFile->size();
    StringBuffer xml;
    iFileIO->read(0, sz, xml.reserve(sz));
    Owned<IPropertyTree> branch = createPTreeFromXMLString(xml.str());
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    if (!add) {
        Owned<IRemoteConnection> bconn = querySDS().connect(remLeading(path),myProcessSession(),RTM_LOCK_READ|RTM_SUB, daliConnectTimeoutMs);
        if (bconn) {
            Owned<IPropertyTree> broot = bconn->getRoot();
            StringBuffer bakname;
            Owned<IFileIO> io = createUniqueFile(NULL, tail, "bak", bakname);
            OUTLOG("Saving backup of %s to %s",path,bakname.str());
            Owned<IFileIOStream> fstream = createBufferedIOStream(io);
            toXML(broot, *fstream);         // formatted (default)
        }
    }
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),0, daliConnectTimeoutMs);
    if (!conn) {
        ERRLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    if (!add) {
        Owned<IPropertyTree> child = root->getPropTree(tail);
        root->removeTree(child);
    }
    Owned<IPropertyTree> oldEnvironment;
    if (streq(path,"Environment"))
        oldEnvironment.setown(createPTreeFromIPT(conn->queryRoot()));
    root->addPropTree(tail,LINK(branch));
    conn->commit();
    OUTLOG("Branch %s loaded from '%s'",path,src);
    conn->close();
    if (*path=='/')
        path++;
    if (strcmp(path,"Environment")==0) {
        OUTLOG("Refreshing cluster groups from Environment");
        StringBuffer response;
        initClusterGroups(false, response, oldEnvironment);
        if (response.length())
            PROGLOG("updating Environment via import path=%s : %s", path, response.str());
    }
}

//=============================================================================


static void _delete_(const char *path,bool backup)
{
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),RTM_LOCK_WRITE, daliConnectTimeoutMs);
    if (!conn) {
        ERRLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    Owned<IPropertyTree> child = root->getPropTree(tail);
    if (!child) {
        ERRLOG("Couldn't find %s/%s",head.str(),tail);
        return;
    }
    if (backup) {
        StringBuffer bakname;
        Owned<IFileIO> io = createUniqueFile(NULL,"daliadmin", "bak", bakname);
        OUTLOG("Saving backup of %s/%s to %s",head.str(),tail,bakname.str());
        Owned<IFileIOStream> fstream = createBufferedIOStream(io);
        toXML(child, *fstream);         // formatted (default)
    }
    root->removeTree(child);
    child.clear();
    root.clear();
    conn->commit();
    conn->close();
}

//=============================================================================

static void set(const char *path,const char *val)
{
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),RTM_LOCK_WRITE, daliConnectTimeoutMs);
    if (!conn) {
        ERRLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    StringBuffer oldv;
    StringBuffer newv;
    root->getProp(tail,oldv);
    root->setProp(tail,val);
    conn->commit();
    root->getProp(tail,newv);
    OUTLOG("Changed %s from '%s' to '%s'",path,oldv.str(),newv.str());
    conn->close();
}

//=============================================================================

static void get(const char *path)
{
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),RTM_LOCK_READ, daliConnectTimeoutMs);
    if (!conn) {
        ERRLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    StringBuffer val;
    root->getProp(tail,val);
    OUTLOG("Value of %s is: '%s'",path,val.str());
    conn->close();
}

//=============================================================================


static void bget(const char *path,const char *outfn)
{
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),RTM_LOCK_READ, daliConnectTimeoutMs);
    if (!conn) {
        ERRLOG("Could not connect to %s",path);
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

//=============================================================================

static void xget(const char *path)
{
    if (!path||!*path)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect("/",myProcessSession(),RTM_LOCK_READ, daliConnectTimeoutMs);
    if (!conn) {
        ERRLOG("Could not connect to /");
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

            outln(res.str());
        } while (it->next());
    }
    conn->close();
}

//=============================================================================

static void wget(const char *path)
{
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),RTM_LOCK_READ, daliConnectTimeoutMs);
    if (!conn) {
        ERRLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements(tail);
    unsigned n = 0;
    ForEach(*iter) {
        n++;
        const char *s = iter->query().queryName();
        OUTLOG("%d,%s",n,s);
    }
    conn->close();
}

//=============================================================================

static void add(const char *path,const char *val)
{
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),RTM_LOCK_WRITE, daliConnectTimeoutMs);
    if (!conn) {
        ERRLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    StringBuffer oldv;
    StringBuffer newv;
    root->addProp(tail,val);
    conn->commit();
    OUTLOG("Added %s value '%s'",path,val);
    conn->close();
}

//=============================================================================

static void delv(const char *path)
{
    StringBuffer head;
    StringBuffer tmp;
    const char *tail=splitpath(path,head,tmp);
    if (!tail)
        return;
    Owned<IRemoteConnection> conn = querySDS().connect(head.str(),myProcessSession(),RTM_LOCK_WRITE, daliConnectTimeoutMs);
    if (!conn) {
        ERRLOG("Could not connect to %s",path);
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    StringBuffer val;
    root->getProp(tail,val);
    root->removeProp(tail);
    OUTLOG("Value of %s was: '%s'",path,val.str());
    conn->close();
}

//=============================================================================

static void count(const char *path)
{
    unsigned result = querySDS().queryCount(path);
    OUTLOG("Count of %s is: %d", path, result);
}

//=============================================================================

static void dfsfile(const char *lname,IUserDescriptor *userDesc, UnsignedArray *partslist=NULL)
{
    StringBuffer str;
    CDfsLogicalFileName lfn;
    lfn.set(lname);
    if (!lfn.isExternal()) {
        Owned<IPropertyTree> tree = queryDistributedFileDirectory().getFileTree(lname,userDesc,NULL,daliConnectTimeoutMs,true); //,userDesc);
        if (partslist)
            filterParts(tree,*partslist);
        if (!tree) {
            ERRLOG("%s not found",lname);
            return;
        }
        toXML(tree, str);
        outln(str.str());
    }
    else {
        Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname,userDesc);
        if (file) {
            Owned<IFileDescriptor> fdesc = file->getFileDescriptor();
            Owned<IPropertyTree> t = createPTree("File");
            fdesc->serializeTree(*t);
            filterParts(t,*partslist);
            toXML(t, str.clear());
            outln(str.str());
        }
    }
}

//=============================================================================

static void dfspart(const char *lname,IUserDescriptor *userDesc, unsigned partnum)
{
    UnsignedArray partslist;
    partslist.append(partnum);
    dfsfile(lname,userDesc,&partslist);
}

//=============================================================================

void dfscsv(const char *dali,IUserDescriptor *udesc)
{

    const char *fields[] = {
        "name","group","directory","partmask","modified","job","owner","workunit","numparts","size","recordCount","recordSize","compressedSize",NULL
    };

    Owned<INode> foreigndali;
    if (dali&&*dali&&(*dali!='*')) {
        SocketEndpoint ep(dali,DALI_SERVER_PORT);
        foreigndali.setown(createINode(ep));
    }
    unsigned start = msTick();
    IDFAttributesIterator *iter = queryDistributedFileDirectory().getDFAttributesIterator("*",udesc,true,false,foreigndali);
    StringBuffer ln;
    unsigned i;
    for (i=0;fields[i];i++) {
        if (i>0)
            ln.append(',');
        ln.append('"').append(fields[i]).append('"');
    }
    outln(ln.str());
    if (iter) {
        StringBuffer aname;
        StringBuffer vals;
        ForEach(*iter) {
            IPropertyTree &attr=iter->query();
            ln.clear();
            for (i=0;fields[i];i++) {
                aname.clear().append('@').append(fields[i]);
                const char *val = attr.queryProp(aname.str());
                if (i>0)
                    ln.append(',');
                if (val)
                    while (*val) {
                        if (*val!=',')
                            ln.append(*val);
                        val++;
                    }
            }
            outln(ln.str());
        }
    }
}


//=============================================================================

static void dfsgroup(const char *name, const char *outputFilename)
{
    Owned<IFileIOStream> io;
    if (outputFilename)
    {
        OwnedIFile iFile = createIFile(outputFilename);
        OwnedIFileIO iFileIO = iFile->open(IFOcreate);
        io.setown(createIOStream(iFileIO));
    }
    Owned<IGroup> group = queryNamedGroupStore().lookup(name);
    if (!group)
    {
        ERRLOG("cannot find group %s",name);
        return;
    }
    StringBuffer eps;
    for (unsigned i=0;i<group->ordinality();i++)
    {
        group->queryNode(i).endpoint().getUrlStr(eps.clear());
        if (io)
        {
            eps.newline();
            io->write(eps.length(), eps.str());
        }
        else
            OUTLOG("%s",eps.str());
    }
}

//=============================================================================

static void dfsmap(const char *lname, IUserDescriptor *user)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname,user);
    if (!file) {
        ERRLOG("File %s not found",lname);
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
        outln(ln.str());
        pn++;
    }
}

//=============================================================================

static int dfsexists(const char *lname,IUserDescriptor *user)
{
    return queryDistributedFileDirectory().exists(lname,user)?0:1;
}
//=============================================================================

static void dfsparents(const char *lname, IUserDescriptor *user)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname,user);
    if (file) {
        Owned<IDistributedSuperFileIterator> iter = file->getOwningSuperFiles();
        ForEach(*iter) 
            OUTLOG("%s,%s",iter->query().queryLogicalName(),lname);
    }
}

//=============================================================================

static void dfsunlink(const char *lname, IUserDescriptor *user)
{
    loop {
        Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname,user);
        if (file) {
            Owned<IDistributedSuperFileIterator> iter = file->getOwningSuperFiles();
            if (!iter->first())
                break;
            IDistributedSuperFile *sf = &iter->query();
            if (sf->removeSubFile(lname,false))
                OUTLOG("removed %s from %s",lname,sf->queryLogicalName());
            else
                ERRLOG("FAILED to remove %s from %s",lname,sf->queryLogicalName());
        }
    }
}

//=============================================================================

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

    bool matchesFindParam(const void * et, const void *fp, unsigned fphash) const
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


static int dfsverify(const char *name,CDateTime *cutoff, IUserDescriptor *user)
{
    static CIpTable dafilesrvips;
    Owned<IDistributedFile> file=queryDistributedFileDirectory().lookup(name,user);
    if (!file) {
        ERRLOG("VERIFY: cannot find %s",name);
        return 1;
    }
    CDateTime filetime;
    if (file->getModificationTime(filetime)) {
        if (cutoff&&(filetime.compare(*cutoff)<=0))
            return 0;
    }

    IPropertyTree &fileprops = file->queryAttributes();
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
                    ERRLOG("VERIFY: file %s, cannot run DAFILESRV on %s",name,ips.str());
                    return 4;
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
        return 2;
    }
    if (list.ordinality()==0)
        return 0;
    OUTLOG("VERIFY: start file %s",name);
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
                // OUTLOG("VERIFY: part %s on %s",partfile->queryFilename(),rfn.queryEndpoint().getUrlStr(eps).str());
                if (partfile) {
                    CriticalUnblock unblock(crit);
                    item.crc = partfile->getCRC();
                    partfile->getTime(NULL,&item.dt,NULL);
                    if ((item.crc==0)&&!partfile->exists()) {
                        ERRLOG("VERIFY: does not exist part %s on %s",partfile->queryFilename(),rfn.queryEndpoint().getUrlStr(eps).str());
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
        if (item.crc!=item.requiredcrc) {
            StringBuffer rfs;
            ERRLOG("VERIFY: FAILED %s (%x,%x) file %s",name,item.crc,item.requiredcrc,item.filename.getRemotePath(rfs).str());
            afor.ok = false;
        }
    }
    if (afor.ok) {
        OUTLOG("VERIFY: OK file %s",name);
        return 0;
    }
    return 3;
}

//=============================================================================

static void setprotect(const char *filename, const char *callerid, IUserDescriptor *user)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(filename,user);
    file->setProtect(callerid,true);
}

//=============================================================================

static void unprotect(const char *filename, const char *callerid, IUserDescriptor *user)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(filename,user);
    file->setProtect((strcmp(callerid,"*")==0)?NULL:callerid,false);
}
//=============================================================================

static void listprotect(const char *filename, const char *callerid)
{
    Owned<IDFProtectedIterator> piter = queryDistributedFileDirectory().lookupProtectedFiles((strcmp(callerid,"*")==0)?NULL:callerid); 
    ForEach(*piter) {
        if (WildMatch(piter->queryFilename(),filename))
            OUTLOG("%s,%s,%s,%u",piter->isSuper()?"SuperFile":"File",piter->queryFilename(),piter->queryOwner(),piter->getCount());
    }
}

//=============================================================================


static bool allyes = false;

static bool getResponse()
{
    if (allyes)
        return true;
    int ch;
    do
    {
        ch = toupper(ch = _getch());
    } while (ch != 'Y' && ch != 'N' && ch != '*');
    _putch(ch);
    _putch('\n');
    if (ch=='*') {
        allyes = true;
        return true;
    }
    return ch=='Y' ? true : false;
}

static bool doFix()
{
    if (allyes)
        return true;
    printf("Fix? (Y/N/*):");
    return getResponse();
}

static void checksuperfile(const char *lfn,bool fix=false)
{
    if (strcmp(lfn,"*")==0) {
        class csuperfilescan: public CSDSFileScanner
        {

            virtual bool checkScopeOk(const char *scopename)
            {
                OUTLOG("Processing scope %s",scopename);
                return true;
            }

            void processSuperFile(IPropertyTree &superfile,StringBuffer &name)
            {
                try {
                    checksuperfile(name.str(),fix);
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
    Owned<IRemoteConnection> conn = querySDS().connect(query.str(),myProcessSession(),fix?RTM_LOCK_WRITE:0, daliConnectTimeoutMs);
    if (!conn) {
        ERRLOG("Could not connect to %s",lfn);
        ERRLOG("Superfile %s FAILED",lname.get());
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
                ERRLOG("Superfile %s FAILED",lname.get());
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
                ERRLOG("Superfile %s FAILED",lname.get());
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
            Owned<IRemoteConnection> subconn = querySDS().connect(subquery.str(),myProcessSession(),fix?RTM_LOCK_WRITE:0, daliConnectTimeoutMs);
            if (!subconn) {
                sublname.makeFullnameQuery(subquery.clear(), DXB_SuperFile, true);
                subconn.setown(querySDS().connect(subquery.str(),myProcessSession(),0, daliConnectTimeoutMs));
            }
            if (!subconn) {
                ERRLOG("SuperFile %s is missing sub-file file %s",lname.get(),subname.str());
                if (!fix||!doFix()) {
                    ERRLOG("Superfile %s FAILED",lname.get());
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
                    Owned<IRemoteConnection> sdconn = querySDS().connect(sdquery.str(),myProcessSession(),0, daliConnectTimeoutMs);
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
                    OUTLOG("Candidate %s",pname.str());
                }
                if (fix&&doFix()) {
                    Owned<IPropertyTree> t = createPTree("SuperOwner");
                    t->setProp("@name",lname.get());
                    subroot->addPropTree("SuperOwner",t.getClear());

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
                ERRLOG("SuperFile %s: corrupt, subfile file part %d spurious",lname.get(),pn);
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
                    ERRLOG("FIX Empty Superfile %s contains non-empty Attr",lname.get());
                root->removeTree(sub);
            }
            else if (sub->getPropInt64("@recordCount")||sub->getPropInt64("@size"))
                ERRLOG("FAIL Empty Superfile %s contains non-empty Attr sz=%"I64F"d rc=%"I64F"d",lname.get(),sub->getPropInt64("@recordCount"),sub->getPropInt64("@size"));

        }
    }
    if (fixed)
        OUTLOG("Superfile %s FIXED - from %d to %d subfiles",lname.get(),n,subnum);
    else
        OUTLOG("Superfile %s OK - contains %d subfiles",lname.get(),n);
}

//=============================================================================

static void checksubfile(const char *lfn)
{
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
                OUTLOG("Processing scope %s",scopename);
                return true;
            }

            void processFile(IPropertyTree &root,StringBuffer &name)
            {
                try {
                    checksubfile(name.str());
                }
                catch (IException *e) {
                    EXCLOG(e,"processSuperFiles");
                    e->Release();
                }
            }

            void processSuperFile(IPropertyTree &root,StringBuffer &name)
            {
                try {
                    checksubfile(name.str());
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
    CDfsLogicalFileName lname;
    lname.set(lfn);
    StringBuffer query;
    lname.makeFullnameQuery(query, DXB_File, true);
    Owned<IRemoteConnection> conn = querySDS().connect(query.str(),myProcessSession(),0, daliConnectTimeoutMs);
    if (!conn) {
        lname.makeFullnameQuery(query.clear(), DXB_SuperFile, true);
        conn.setown(querySDS().connect(query.str(),myProcessSession(),0, daliConnectTimeoutMs));
    }
    if (!conn) {
        ERRLOG("Could not connect to %s",lfn);
        ERRLOG("Subfile %s FAILED",lname.get());
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
        Owned<IRemoteConnection> sdconn = querySDS().connect(sdquery.str(),myProcessSession(),0, daliConnectTimeoutMs);
        if (!conn) {
            ERRLOG("SubFile %s has missing owner superfile %s",lname.get(),sdlname.get());
            ok = false;
        }
        else {
            StringBuffer path;
            IPropertyTree *sub = sdconn->queryRoot()->queryPropTree(path.clear().appendf("SubFile[@name=\"%s\"]",lname.get()).str());
            if (!sub) {
                ERRLOG("Superfile %s is not linked to %s",sdlname.get(),lname.get());
                ok = false;
            }
        }
    }
    if (ok)
        OUTLOG("SubFile %s OK",lname.get());
}

//=============================================================================

static void listexpires(const char * lfnmask, IUserDescriptor *user)
{
    IDFAttributesIterator *iter = queryDistributedFileDirectory().getDFAttributesIterator(lfnmask,user,true,false);
    ForEach(*iter) {
        IPropertyTree &attr=iter->query();
        const char * expires = attr.queryProp("@expires");
        if (expires&&*expires) {
            const char * name = attr.queryProp("@name");
            if (name&&*name) {
                OUTLOG("%s expires on %s",name,expires);
            }
        }
    }
}

//=============================================================================

static void listrelationships(const char *primary,const char *secondary)
{
    Owned<IFileRelationshipIterator> iter = queryDistributedFileDirectory().lookupFileRelationships(primary,secondary,NULL,NULL,S_LINK_RELATIONSHIP_KIND,NULL,NULL,NULL);
    ForEach(*iter) {
        OUTLOG("%s,%s,%s,%s,%s,%s,%s,%s",
            iter->query().queryKind(),
            iter->query().queryPrimaryFilename(),
            iter->query().querySecondaryFilename(),
            iter->query().queryPrimaryFields(),
            iter->query().querySecondaryFields(),
            iter->query().queryCardinality(),
            iter->query().isPayload()?"payload":"",
            iter->query().queryDescription());

    }
}

//=============================================================================

int dfsperm(const char *obj,IUserDescriptor *user)
{
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
    OUTLOG("perm %s = %d",obj,perm);
    return perm;
}

//=============================================================================


static offset_t getCompressedSize(IDistributedFile *file)
{  // this should be parallel!  TBD
    if (!file)
        return (offset_t)-1;
    offset_t ret = (offset_t)file->queryAttributes().getPropInt64("@compressedSize",-1);
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

static void dfscompratio (const char *lname, IUserDescriptor *user)
{
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lname,user);
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
                    out.appendf(", Ratio %.2f:1 (%%%d)",(float)size/csize,(unsigned)(csize*100/size));
            }
        }
        else
            out.appendf("not compressed, size %"I64F"d",size);
    }
    else
        out.appendf("File %s not found",lname);
    outln(out.str());
}

//=============================================================================


static bool onlyNamePtree(IPropertyTree *t)
{
    if (!t)
        return true;
    if (t->numUniq())
        return false;
    Owned<IAttributeIterator> ai = t->getAttributes();
    if (ai->first()) {
        if (strcmp(ai->queryName(),"@name")!=0)
            return false;
        if (ai->next())
            return false;
    }
    const char *s = t->queryProp(NULL);
    if (s&&*s)
        return false;
    return true;
}

static bool countScopeChildren(IPropertyTree *t,unsigned &files, unsigned &sfiles, unsigned &scopes, unsigned &other)
{
    scopes = 0; 
    files = 0;
    sfiles = 0;
    other = 0;
    if (!t)
        return false;
    Owned<IPropertyTreeIterator> it = t->getElements("*");
    ForEach(*it) {
        IPropertyTree *st = &it->query();
        const char *s = st?st->queryName():NULL;
        if (!s) 
            other++;
        else if (stricmp(s,queryDfsXmlBranchName(DXB_File))==0) 
            files++;
        else if (stricmp(s,queryDfsXmlBranchName(DXB_SuperFile))==0) 
            sfiles++;
        else if (stricmp(s,queryDfsXmlBranchName(DXB_Scope))==0) 
            scopes++;
        else
            other++;
    }
    return (other!=0)||(files!=0)||(sfiles!=0)||(scopes!=0)||(!onlyNamePtree(t));
}

static void dfsscopes(const char *name, IUserDescriptor *user)
{
    bool wild = isWild(name);
    Owned<IDFScopeIterator> iter = queryDistributedFileDirectory().getScopeIterator(user,wild?NULL:name,true,true);
    StringBuffer ln;
    ForEach(*iter) {
        CDfsLogicalFileName dlfn;
        StringBuffer scope;
        if (!wild&&name&&*name&&(strcmp(name,".")!=0))
            scope.append(name).append("::");
        scope.append(iter->query());
        if (wild&&!WildMatch(scope.str(),name))
            continue;
        dlfn.set(scope.str(),"x");
        StringBuffer s;
        dlfn.makeScopeQuery(s,true);
        ln.clear().append("SCOPE '").append(iter->query()).append('\'');
        Owned<IRemoteConnection> conn = querySDS().connect(s.str(),myProcessSession(),RTM_LOCK_READ, daliConnectTimeoutMs);
        if (!conn)
            ERRLOG("%s - Could not connect using %s",ln.str(),s.str());
        else {
            unsigned files;
            unsigned sfiles;
            unsigned scopes;
            unsigned other;
            if (countScopeChildren(conn->queryRoot(),files,sfiles,scopes,other)) {
                ln.appendf(" Files=%d SuperFiles=%d Scopes=%d",files,sfiles,scopes);
                if (other)
                    ln.appendf(" others=%d",other);
                OUTLOG("%s",ln.str());
            }
            else
                OUTLOG("%s EMPTY",ln.str());
        }
    }
}

//=============================================================================

static bool recursiveCheckEmptyScope(IPropertyTree &ct)
{
    Owned<IPropertyTreeIterator> iter = ct.getElements("*");
    ForEach(*iter) {
        IPropertyTree &item = iter->query();
        const char *n = item.queryName();
        if (!n||(strcmp(n,queryDfsXmlBranchName(DXB_Scope))!=0))
            return false;
        if (!recursiveCheckEmptyScope(item))
            return false;
    }
    return true;
}


static void cleanscopes(IUserDescriptor *user)
{
    Owned<IDFScopeIterator> iter = queryDistributedFileDirectory().getScopeIterator(user, NULL,true,true);
    CDfsLogicalFileName dlfn;
    StringBuffer s;
    StringArray toremove;
    ForEach(*iter) {
        CDfsLogicalFileName dlfn;
        StringBuffer scope;
        scope.append(iter->query());
        dlfn.set(scope.str(),"x");
        dlfn.makeScopeQuery(s.clear(),true);
        Owned<IRemoteConnection> conn = querySDS().connect(s.str(),myProcessSession(),RTM_LOCK_READ, daliConnectTimeoutMs);
        if (!conn)  
            DBGLOG("Could not connect to '%s' using %s",iter->query(),s.str());
        else {
            if (recursiveCheckEmptyScope(*conn->queryRoot())) {
                toremove.append(iter->query());
                PROGLOG("EMPTY %s, %s",iter->query(),s.str());
            }
        }
    }
    iter.clear();
    ForEachItemIn(i,toremove) {
        PROGLOG("REMOVE %s",toremove.item(i));
        try {
            queryDistributedFileDirectory().removeEmptyScope(toremove.item(i));
        }
        catch (IException *e) {
            EXCLOG(e,"checkScopes");
            e->Release();
        }
    }
}

//=============================================================================

static void listworkunits(const char *test,const char *min, const char *max)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/", myProcessSession(), 0, daliConnectTimeoutMs);
    Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("WorkUnits/*");
    ForEach(*iter) {
        IPropertyTree &e=iter->query();
        if (test&&*test) {
            const char *tval = strchr(test,'=');
            if (!tval) {
                ERRLOG("missing '=' in %s",test);
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
        outln(e.queryName());
    }
}

//=============================================================================

static void listmatches(const char *path, const char *match, const char *pval)
{
    Owned<IRemoteConnection> conn = querySDS().connect(path, myProcessSession(), 0, daliConnectTimeoutMs);
    if (!conn)
    {
        PROGLOG("Failed to connect to %s", path);
        return;
    }
    StringBuffer output("Listing matches for path=");
    output.append(path);
    if (match)
    {
        output.append(", match=").append(match);
        if (pval)
            output.append(", property value = ").append(pval);
    }
    Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements(match?match:"*", iptiter_remote);
    ForEach(*iter)
    {
        IPropertyTree &e=iter->query();
        output.clear().append(e.queryName());
        const char *val = e.queryProp(pval?pval:NULL);
        if (val)
            output.append(" = ").append(val);
        outln(output.str());
    }
}

//=============================================================================


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

static void workunittimings(const char *wuid)
{
    StringBuffer path;
    path.append("/WorkUnits/").append(wuid);
    Owned<IRemoteConnection> conn = querySDS().connect(path, myProcessSession(), 0, daliConnectTimeoutMs);
    if (!conn) {
        ERRLOG("WU %s not found",wuid);
        return;
    }
    IPropertyTree *wu = conn->queryRoot();
    Owned<IPropertyTreeIterator> iter = wu->getElements("Timings/Timing");
    StringBuffer name;
    outln("Name,graph,sub,gid,time ms,time min");
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
                    OUTLOG("\"%s\",%d,%d,%d,%d,%d",name.str(),gn,sn,gid,time,(time/60000));
                }
            }
        }
    }

}

//=============================================================================



static void serverlist(const char *mask)
{
    Owned<IRemoteConnection> conn = querySDS().connect( "/Environment/Software", myProcessSession(),  RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
        throw MakeStringException(0,"Failed to connect to Environment/Software");
    IPropertyTree* root = conn->queryRoot();
    Owned<IPropertyTreeIterator> services= root->getElements("*");
    ForEach(*services) {
        IPropertyTree& t = services->query();
        const char *name = t.queryName();
        if (name) {
            if (!mask||!*mask||WildMatch(name,mask)) {
                Owned<IPropertyTreeIterator> insts = t.getElements("Instance");
                ForEach(*insts) {
                    StringBuffer ips;
                    insts->query().getProp("@netAddress",ips);
                    StringBuffer dir;
                    insts->query().getProp("@directory",dir);
                    OUTLOG("%s,%s,%s",name,ips.str(),dir.str());
                }
            }
        }
    }
}

//=============================================================================

static void clusterlist(const char *mask)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
        throw MakeStringException(0,"Failed to connect to Environment/Software");
    IPropertyTree* root = conn->queryRoot();
    Owned<IPropertyTreeIterator> clusters;
    clusters.setown(root->getElements("ThorCluster"));
    ForEach(*clusters) {
    }
    clusters.setown(root->getElements("RoxieCluster"));
    ForEach(*clusters) {
    }
    clusters.setown(root->getElements("EclAgentProcess"));
    ForEach(*clusters) {
    }
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

static void clusterlist()
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ, daliConnectTimeoutMs);
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
    ForEachItemIn(i,cnames) 
        OUTLOG("%s,%s",cnames.item(i),groups.item(i));
}


//=============================================================================

static void auditlog(const char *froms, const char *tos, const char *matchs)
{
    CDateTime from;
    try {
        from.setDateString(froms);
    }
    catch (IException *) {
        ERRLOG("%s: invalid date (format YYYY-MM-DD)",froms);
        throw;
    }
    CDateTime to;
    try {
        to.setDateString(tos);
    }
    catch (IException *) {
        ERRLOG("%s: invalid date (format YYYY-MM-DD)",tos);
        throw;
    }
    StringAttrArray res;
    queryAuditLogs(from,to,matchs,res);
    ForEachItemIn(i,res)
        outln(res.item(i).text.get());
}

//=============================================================================

static void coalesce()
{
    const char *daliDataPath = NULL;
    const char *remoteBackupLocation = NULL;
    Owned<IStoreHelper> iStoreHelper = createStoreHelper(NULL, daliDataPath, remoteBackupLocation, SH_External|SH_RecoverFromIncErrors);
    unsigned baseEdition = iStoreHelper->queryCurrentEdition();

    StringBuffer storeFilename(daliDataPath);
    iStoreHelper->getCurrentStoreFilename(storeFilename);
    OUTLOG("Loading store: %s", storeFilename.str());
    Owned<IPropertyTree> root = createPTreeFromXMLFile(storeFilename.str());
    OUTLOG("Loaded: %s", storeFilename.str());

    if (baseEdition != iStoreHelper->queryCurrentEdition())
        OUTLOG("Store was changed by another process prior to coalesce. Exiting.");
    else
    {
        if (!iStoreHelper->loadDeltas(root))
            OUTLOG("Nothing to coalesce");
        else
            iStoreHelper->saveStore(root, &baseEdition);
    }
}

//=============================================================================


static void mpping(const char *eps)
{
    SocketEndpoint ep(eps);
    Owned<INode> node = createINode(ep);
    Owned<IGroup> grp = createIGroup(1,&ep);
    Owned<ICommunicator> comm = createCommunicator(grp,true);
    unsigned start = msTick();
    if (!comm->verifyConnection(0,60*1000))
        ERRLOG("MPping %s failed",eps);
    else
        OUTLOG("MPping %s succeeded in %d",eps,msTick()-start);
}

//=============================================================================

static void daliping(const char *dalis,unsigned connecttime,unsigned n)
{
    OUTLOG("Dali(%s) connect time: %d ms",dalis,connecttime);
    if (!n)
        return;
    StringBuffer qname("TESTINGQ_");
    SocketEndpoint ep;
    ep.setLocalHost(0);
    ep.getUrlStr(qname);
    Owned<INamedQueueConnection> qconn;
    qconn.setown(createNamedQueueConnection(0));
    Owned<IQueueChannel> channel;
    channel.setown(qconn->open(qname.str()));
    MemoryBuffer mb;
    while (channel->probe()) {
        mb.clear();
        channel->get(mb);
    }
    unsigned max = 0;
    unsigned tot = 0;
    for (unsigned i=0;i<=n;i++) {
        mb.clear().append("Hello").append(i);
        ep.serialize(mb);
        unsigned start = msTick();
        channel->put(mb);
        channel->get(mb);
        if (i) {        // ignore first
            unsigned t = msTick()-start;
            if (t>max)
                max = t;
            tot += t;
            OUTLOG("Dali(%s) ping %d ms",dalis,t);
            if (i+1<n)
                Sleep(1000);
        }
    }
    OUTLOG("Dali(%s) ping  avg = %d max = %d ms",dalis,tot/n,max);
}


//=============================================================================

static void convertBinBranch(IPropertyTree &cluster,const char *branch)
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

static void getxref(const char *dst)
{
    Owned<IRemoteConnection> conn = querySDS().connect("DFU/XREF",myProcessSession(),RTM_LOCK_READ, daliConnectTimeoutMs);
    Owned<IPropertyTree> root = createPTreeFromIPT(conn->getRoot());
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
    OUTLOG("DFU/XREF saved in '%s'",dst);
    conn->close();
}

struct CTreeItem : public CInterface
{
    String *tail;
    CTreeItem *parent;
    unsigned index;
    unsigned startOffset;
    unsigned endOffset;
    unsigned adjust;
    bool supressidx;
    CTreeItem(CTreeItem *_parent, String *_tail, unsigned _index, unsigned _startOffset)
    {
        parent = LINK(_parent);
        startOffset = _startOffset;
        endOffset = 0;
        adjust = 0;
        index = _index;
        supressidx = true;
        tail = _tail;
    }
    ~CTreeItem()
    {
        if (parent)
            parent->Release();
        ::Release(tail);
    }
    void getXPath(StringBuffer &xpath)
    {
        if (parent)
            parent->getXPath(xpath);
        xpath.append('/').append(tail->toCharArray());
        if ((index!=0)||tail->IsShared())
            xpath.append('[').append(index+1).append(']');
    }
    unsigned size() { return endOffset?(endOffset-startOffset):0; }
    unsigned adjustedSize(bool &adjusted) { adjusted = (adjust!=0); return size()-adjust; }
};

class CXMLSizesParser : public CInterface
{
    Owned<IPullPTreeReader> xmlReader;
    PTreeReaderOptions xmlOptions;
    double pc;

    class CParse : public CInterface, implements IPTreeNotifyEvent
    {
        CIArrayOf<CTreeItem> stack;
        String * levtail;
        CIArrayOf<CTreeItem> arr;
        unsigned limit;
        __int64 totalSize;

        static int _sortF(CInterface **_left, CInterface **_right)
        {
            CTreeItem **left = (CTreeItem **)_left;
            CTreeItem **right = (CTreeItem **)_right;
            return ((*right)->size() - (*left)->size());
        }
    public:

        IMPLEMENT_IINTERFACE;

        CParse(unsigned __int64 _totalSize, double limitpc) : totalSize(_totalSize)
        {
            levtail = NULL;
            limit = (unsigned)((double)totalSize*limitpc/100.0);
        }
        void reset()
        {
            stack.kill();
        }

// IPTreeNotifyEvent
        virtual void beginNode(const char *tag, offset_t startOffset)
        {
            String *tail = levtail;
            if (levtail&&(0 == strcmp(tag, levtail->toCharArray())))
                tail->Link();
            else
                tail = new String(tag);
            levtail = NULL;     // opening new child
            CTreeItem *parent = stack.empty()?NULL:&stack.tos();
            CTreeItem *item = new CTreeItem(parent, tail, tail->getLinkCount(), startOffset);
            stack.append(*item);
        }
        virtual void newAttribute(const char *tag, const char *value)
        {
        }
        virtual void beginNodeContent(const char *tag)
        {
        }
        virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
        {
            CTreeItem *tos = &stack.tos();
            assertex(tos);
            tos->endOffset = endOffset;
            bool adjusted;
            unsigned sz = tos->adjustedSize(adjusted);
            if (sz>=limit)
            {
                CTreeItem *parent = tos->parent;
                while (parent) {
                    parent->adjust += sz;
                    parent = parent->parent;
                }
                tos->Link();
                arr.append(*tos);
                levtail = tos->tail;
            }
            else
                levtail = NULL;
            stack.pop();
        }

        void printFullResults()
        {
            arr.sort(_sortF);
            ForEachItemIn(m, arr)
            {
                CTreeItem &match = arr.item(m);
                StringBuffer xpath;
                match.getXPath(xpath);
                printf("xpath=%s, size=%d\n", xpath.str(), match.size());
            }
        }
        void printResultTree()
        {
            if (!totalSize)
                return;
            StringBuffer res;
            ForEachItemIn(i, arr) {
                CTreeItem &item = arr.item(i);
                bool adjusted;
                unsigned sz = item.adjustedSize(adjusted);
                if (sz>=limit) {
                    res.clear();
                    item.getXPath(res);
                    if (adjusted)
                        res.append(" (rest)");
                    res.padTo(40);
                    res.appendf(" %10d(%5.2f%%)",sz,((float)sz*100.0)/(float)totalSize);
                    printf("%s\n",res.str());
                }
            }
        }
    } *parser;

public:
    IMPLEMENT_IINTERFACE;

    CXMLSizesParser(const char *fName, PTreeReaderOptions _xmlOptions=ptr_none, double _pc=1.0) : xmlOptions(_xmlOptions), pc(_pc) { go(fName); }
    ~CXMLSizesParser() { ::Release(parser); }

    void go(const char *fName)
    {
        OwnedIFile ifile = createIFile(fName);
        OwnedIFileIO ifileio = ifile->open(IFOread);
        if (!ifileio)
            throw MakeStringException(0, "Failed to open: %s", ifile->queryFilename());
        parser = new CParse(ifileio->size(), pc);
        Owned<IIOStream> stream = createIOStream(ifileio);
        xmlReader.setown(createPullXMLStreamReader(*stream, *parser, xmlOptions));
    }

    void printResultTree()
    {
        parser->printResultTree();
    }

    virtual bool next()
    {
        return xmlReader->next();
    }

    virtual void reset()
    {
        parser->reset();
        xmlReader->reset();
    }
};

static void xmlSize(const char *filename, double pc)
{

    try
    {
        OwnedIFile iFile = createIFile(filename);
        if (!iFile->exists())
            OUTLOG("File '%s' not found", filename);
        else
        {
            Owned<CXMLSizesParser> parser = new CXMLSizesParser((filename&&*filename)?filename:"dalisds.xml", ptr_none, pc);
            while (parser->next())
                ;
            parser->printResultTree();
        }
    }
    catch (IException *e)
    {
        pexception("xmlSize", e);
        e->Release();
    }
}

//=============================================================================

static bool begins(const char *&ln,const char *pat)
{
    size32_t sz = strlen(pat);
    if (memicmp(ln,pat,sz)==0) {
        ln += sz;
        return true;
    }
    return false;
}

static void dodalilocks(const char *pattern,const char *obj,Int64Array *conn,bool filesonly)
{
    StringBuffer buf;
    getDaliDiagnosticValue("locks",buf);
    for (int pass=(filesonly?1:0);pass<2;pass++) {
        bool headerdone = false;
        StringBuffer line;
        StringBuffer curfile;
        StringBuffer curxpath;
        StringBuffer ips;
        StringBuffer times;
        StringBuffer sessid;
        StringBuffer connid;
        const char *s = buf.str();
        loop {
            line.clear();
            while (*s&&(*s!='\n')) 
                line.append(*(s++));
            if (line.length()) {
                const char *ln = line.str();
                if (begins(ln,"Locks on path: ")) {
                    curfile.clear();
                    curxpath.clear();
                    const char *x = ln;
                    while (*x)
                        curxpath.append(*(x++));
                    if (begins(ln,"/Files")) {
                        while (*ln&&(begins(ln,"/Scope[@name=\"")||begins(ln,"/File[@name=\""))) {
                            if (curfile.length())
                                curfile.append("::");
                            while (*ln&&(*ln!='"'))
                                curfile.append(*(ln++));
                            if (*ln=='"')
                                ln++;
                            if (*ln==']')
                                ln++;
                        }
                    }
                }
                else if (isdigit(*ln)) {
                    if (obj) 
                        if (!curxpath.length()||!WildMatch(curxpath.str(),obj)) 
                            if (!curfile.length()||!WildMatch(curfile.str(),obj)) 
                                continue;
                    if ((curfile.length()!=0)==(pass==1)) {
                        ips.clear();
                        while (*ln&&(*ln!=':'))
                            ips.append(*(ln++));
                        if (!pattern||WildMatch(ips.str(),pattern)) {
                            ips.append(*ln++);
                            while (isdigit(*ln))
                                ips.append(*ln++);
                            while (*ln!='|')    
                                ln++;
                            ln++; // sessid start
                            sessid.clear();
                            while (*ln!='|') {
                                sessid.append(*ln);
                                ln++;
                            }
                            sessid.clip();
                            ln++; // connectid start
                            connid.clear();
                            while (*ln!='|') {
                                connid.append(*ln);
                                ln++;
                            }
                            connid.clip();
                            ln++; // mode start
                            unsigned mode = 0;
                            while (isdigit(*ln))
                                mode = mode*10+(*(ln++)-'0');
                            while (*ln!='|')
                                ln++;
                            ln++; // duration start
                            times.clear();
                            while (*ln&&(*ln!='('))
                                times.append(*(ln++));
                            ln++;
                            unsigned duration = 0;
                            while (isdigit(*ln))
                                duration = duration*10+(*(ln++)-'0');
                            if (conn) {
                                bool err;
                                __int64 c = (__int64)hextoll(connid.str(),err);
                                if (!err) {
                                    bool found = false;
                                    ForEachItemIn(i,*conn) 
                                        if (c==conn->item(i))
                                            found = true;
                                    if (!found)
                                        conn->append(c);
                                }
                            }
                            else {
                                if (!headerdone) {
                                    OUTLOG( "\nServer IP         , session   ,mode, time              ,duration ,%s",pass?"File":"XPath");
                                    OUTLOG(   "==================,===========,===,====================,=========,=====");
                                    headerdone = true;
                                }
                                OUTLOG("%s, %s, %d, %s, %d, %s",ips.str(),sessid.str(),mode,times.str(),duration,pass?curfile.str():curxpath.str());
                            }
                        }
                    }
                }
            }
            if (!*s)
                break;
            s++;
        }
    }
}

static void dalilocks(const char *pattern,bool fileonly)
{
    dodalilocks(pattern,NULL,NULL,fileonly);
}

//=============================================================================

static void unlock(const char *pattern)
{
    Int64Array conn;
    dodalilocks(NULL,pattern,&conn,false);
    ForEachItemIn(i,conn) {
        MemoryBuffer mb;
        __int64 connectionId = conn.item(i);
        bool success;
        bool disconnect = false;        // TBD?
        mb.append("unlock").append(connectionId).append(disconnect);
        getDaliDiagnosticValue(mb);
        mb.read(success);
        StringBuffer connectionInfo;
        if (!success)
            PROGLOG("Lock %"I64F"x not found",connectionId);
        else {
            mb.read(connectionInfo);
            PROGLOG("Lock %"I64F"x successfully removed: %s", connectionId, connectionInfo.str());
        }
    }
}

static void wuidCompress(const char *match, const char *type, bool compress)
{
    if (0 != stricmp("graph", type))
    {
        WARNLOG("Currently, only type=='graph' supported.");
        return;
    }
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IConstWorkUnitIterator> iter = factory->getWorkUnitsByXPath(match);
    ForEach(*iter)
    {
        IConstWorkUnit &wuid = iter->query();

        StringArray graphNames;
        Owned<IConstWUGraphIterator> graphIter = &wuid.getGraphs(GraphTypeAny);
        ForEach(*graphIter)
        {
            SCMStringBuffer graphName;
            IConstWUGraph &graph = graphIter->query();
            Owned<IPropertyTree> xgmml = graph.getXGMMLTreeRaw();
            if (compress != xgmml->hasProp("graphBin"))
            {
                graph.getName(graphName);
                graphNames.append(graphName.s.str());
            }
        }

        if (graphNames.ordinality())
        {
            SCMStringBuffer wuidName;
            wuid.getWuid(wuidName);
            StringAttr msg;
            msg.set(compress?"Compressing":"Uncompressing");
            PROGLOG("%s graphs for workunit: %s", msg.get(), wuidName.s.str());
            Owned<IWorkUnit> wWuid = &wuid.lock();
            ForEachItemIn(n, graphNames)
            {
                Owned<IWUGraph> wGraph = wWuid->updateGraph(graphNames.item(n));
                PROGLOG("%s graph: %s", msg.get(), graphNames.item(n));
                // get/set - will convert to/from new format (binary compress blob)
                Owned<IPropertyTree> xgmml = wGraph->getXGMMLTree(false);
                wGraph->setXGMMLTree(xgmml.getClear(), compress);
            }
        }
    }
}


//=============================================================================


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
                            OUTLOG("%s running on queue %s",wuid,queue);
                        }
                    }
                }
            }
        }
    }
}









#define CHECKPARAMS(mn,mx) { if ((np<mn)||(np>mx)) throw MakeStringException(-1,"%s: incorrect number of parameters",cmd); }




int main(int argc, char* argv[])
{
    int ret = 0;
    InitModuleObjects();
    EnableSEHtoExceptionMapping();
    setDaliServixSocketCaching(true);
    if (argc<2) {
        usage(argv[0]);
        return -1;
    }

    Owned<IProperties> props = createProperties("daliadmin.ini");
    StringArray params;
    SocketEndpoint ep;
    StringBuffer tmps;
    for (int i=1;i<argc;i++) {
        const char *param = argv[i];
        if ((memcmp(param,"server=",7)==0)||
            (memcmp(param,"logfile=",8)==0)||
            (memcmp(param,"rawlog=",8)==0)||
            (memcmp(param,"user=",5)==0)||
            (memcmp(param,"password=",9)==0) ||
            (memcmp(param,"fix=",4)==0) ||
            (memcmp(param,"timeout=",4)==0))
            props->loadProp(param);
        else if ((i==1)&&(isdigit(*param)||(*param=='.'))&&ep.set(((*param=='.')&&param[1])?(param+1):param,DALI_SERVER_PORT))
            props->setProp("server",ep.getUrlStr(tmps.clear()).str());
        else {
            if ((0==stricmp(param,"help")) || (0 ==stricmp(param,"-help")) || (0 ==stricmp(param,"--help"))) {
                usage(argv[0]);
                return -1;
            }
            params.append(param);
        }
    }
    if (!params.ordinality()) {
        usage(argv[0]);
        return -1;
    }

    try {
        StringBuffer logname;
        StringBuffer aliasname;
        bool rawlog = props->getPropBool("rawlog");
        Owned<ILogMsgHandler> fileMsgHandler;
        if (props->getProp("logfile",logname)) {
            if (logname.length()) {
                fileMsgHandler.setown(getFileLogMsgHandler(logname.str(), NULL, rawlog?MSGFIELD_prefix:MSGFIELD_STANDARD, false, false, true));
                queryLogMsgManager()->addMonitorOwn(fileMsgHandler.getClear(), getCategoryLogMsgFilter(MSGAUD_all, MSGCLS_all, TopDetail));
            }
        }
        // set stdout 
        attachStandardHandleLogMsgMonitor(stdout,0,MSGAUD_all,MSGCLS_all&~(MSGCLS_disaster|MSGCLS_error|MSGCLS_warning));
        Owned<ILogMsgFilter> filter = getCategoryLogMsgFilter(MSGAUD_user, MSGCLS_error|MSGCLS_warning);
        queryLogMsgManager()->changeMonitorFilter(queryStderrLogMsgHandler(), filter);
        queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_prefix);
    }
    catch (IException *e) {
        pexception("daliadmin",e);
        e->Release();
        ret = 255;
    }
    unsigned daliconnectelapsed;
    StringBuffer daliserv;
    if (!ret) {
        const char *cmd = params.item(0);
        unsigned np = params.ordinality()-1;

        if (!props->getProp("server",daliserv.clear()))
        {
            // external commands

            if (stricmp(cmd,"xmlsize")==0)
            {
                CHECKPARAMS(1,2);
                xmlSize(params.item(1), np>1?atof(params.item(2)):1.0);
            }
            else
                ERRLOG("Unknown command %s",cmd);
            return 0;
        }
        else
        {
            try {
                SocketEndpoint ep(daliserv.str(),DALI_SERVER_PORT);
                SocketEndpointArray epa;
                epa.append(ep);
                Owned<IGroup> group = createIGroup(epa);
                unsigned start = msTick();
                initClientProcess(group, DCR_Util);
                daliconnectelapsed = msTick()-start;
            }
            catch (IException *e) {
                EXCLOG(e,"daliadmin initClientProcess");
                e->Release();
                ret = 254;
            }
            if (!ret) {
                try {
                    Owned<IUserDescriptor> userDesc;
                    if (props->getProp("user",tmps.clear())) {
                        userDesc.setown(createUserDescriptor());
                        StringBuffer ps;
                        props->getProp("password",ps);
                        userDesc->set(tmps.str(),ps.str());
                        queryDistributedFileDirectory().setDefaultUser(userDesc);
                    }
                    daliConnectTimeoutMs = 1000 * props->getPropInt("timeout", DEFAULT_DALICONNECT_TIMEOUT);
                    if (stricmp(cmd,"export")==0) {
                        CHECKPARAMS(2,2);
                        _export_(params.item(1),params.item(2));
                    }
                    else if (stricmp(cmd,"import")==0) {
                        CHECKPARAMS(2,2);
                        import(params.item(1),params.item(2),false);
                    }
                    else if (stricmp(cmd,"importadd")==0) {
                        CHECKPARAMS(2,2);
                        import(params.item(1),params.item(2),true);
                    }
                    else if (stricmp(cmd,"delete")==0) {
                        CHECKPARAMS(1,1);
                        _delete_(params.item(1),true);
                    }
                    else if (stricmp(cmd,"set")==0) {
                        CHECKPARAMS(2,2);
                        set(params.item(1),params.item(2));
                    }
                    else if (stricmp(cmd,"get")==0) {
                        CHECKPARAMS(1,1);
                        get(params.item(1));
                    }
                    else if (stricmp(cmd,"bget")==0) {
                        CHECKPARAMS(2,2);
                        bget(params.item(1),params.item(2));
                    }
                    else if (stricmp(cmd,"wget")==0) {
                        CHECKPARAMS(1,1);
                        wget(params.item(1));
                    }
                    else if (stricmp(cmd,"xget")==0) {
                        CHECKPARAMS(1,1);
                        wget(params.item(1));
                    }
                    else if (stricmp(cmd,"add")==0) {
                        CHECKPARAMS(2,2);
                        add(params.item(1),params.item(2));
                    }
                    else if (stricmp(cmd,"delv")==0) {
                        CHECKPARAMS(1,1);
                        delv(params.item(1));
                    }
                    else if (stricmp(cmd,"count")==0) {
                        CHECKPARAMS(1,1);
                        count(params.item(1));
                    }
                    else if (stricmp(cmd,"dfsfile")==0) {
                        CHECKPARAMS(1,1);
                        dfsfile(params.item(1),userDesc);
                    }
                    else if (stricmp(cmd,"dfspart")==0) {
                        CHECKPARAMS(2,2);
                        dfspart(params.item(1),userDesc,atoi(params.item(2)));
                    }
                    else if (stricmp(cmd,"dfscsv")==0) {
                        CHECKPARAMS(1,1);
                        dfscsv(params.item(1),userDesc);
                    }
                    else if (stricmp(cmd,"dfsgroup")==0) {
                        CHECKPARAMS(1,2);
                        dfsgroup(params.item(1),(np>1)?params.item(2):NULL);
                    }
                    else if (stricmp(cmd,"dfsmap")==0) {
                        CHECKPARAMS(1,1);
                        dfsmap(params.item(1), userDesc);
                    }
                    else if (stricmp(cmd,"dfsexist")==0) {
                        CHECKPARAMS(1,1);
                        ret = dfsexists(params.item(1),userDesc);
                    }
                    else if (stricmp(cmd,"dfsparents")==0) {
                        CHECKPARAMS(1,1);
                        dfsparents(params.item(1),userDesc);
                    }
                    else if (stricmp(cmd,"dfsunlink")==0) {
                        CHECKPARAMS(1,1);
                        dfsunlink(params.item(1),userDesc);
                    }
                    else if (stricmp(cmd,"dfsverify")==0) {
                        CHECKPARAMS(1,1);
                        ret = dfsverify(params.item(1),NULL,userDesc);
                    }
                    else if (stricmp(cmd,"setprotect")==0) {
                        CHECKPARAMS(2,2);
                        setprotect(params.item(1),params.item(2),userDesc);
                    }
                    else if (stricmp(cmd,"unprotect")==0) {
                        CHECKPARAMS(2,2);
                        unprotect(params.item(1),params.item(2),userDesc);
                    }
                    else if (stricmp(cmd,"listprotect")==0) {
                        CHECKPARAMS(0,2);
                        listprotect((np>1)?params.item(1):"*",(np>2)?params.item(2):"*");

                    }
                    else if (stricmp(cmd,"checksuperfile")==0) {
                        CHECKPARAMS(1,1);
                        bool fix = props->getPropBool("fix");
                        checksuperfile(params.item(1),fix);
                    }
                    else if (stricmp(cmd,"checksubfile")==0) {
                        CHECKPARAMS(1,1);
                        checksubfile(params.item(1));
                    }
                    else if (stricmp(cmd,"listexpires")==0) {
                        CHECKPARAMS(0,1);
                        listexpires((np>1)?params.item(1):"*",userDesc);
                    }
                    else if (stricmp(cmd,"listrelationships")==0) {
                        CHECKPARAMS(2,2);
                        listrelationships(params.item(1),params.item(2));
                    }
                    else if (stricmp(cmd,"dfsperm")==0) {
                        if (!userDesc.get())
                            throw MakeStringException(-1,"dfsperm requires username to be set (user=)");
                        CHECKPARAMS(1,1);
                        ret = dfsperm(params.item(1),userDesc);
                    }
                    else if (stricmp(cmd,"dfscompratio")==0) {
                        CHECKPARAMS(1,1);
                        dfscompratio(params.item(1),userDesc);
                    }
                    else if (stricmp(cmd,"dfsscopes")==0) {
                        CHECKPARAMS(0,1);
                        dfsscopes((np>1)?params.item(1):"*",userDesc);
                    }
                    else if (stricmp(cmd,"cleanscopes")==0) {
                        CHECKPARAMS(0,0);
                        cleanscopes(userDesc);
                    }
                    else if (stricmp(cmd,"listworkunits")==0) {
                        CHECKPARAMS(0,3);
                        listworkunits((np>0)?params.item(1):NULL,(np>1)?params.item(2):NULL,(np>2)?params.item(3):NULL);
                    }
                    else if (stricmp(cmd,"listmatches")==0) {
                        CHECKPARAMS(0,3);
                        listmatches((np>0)?params.item(1):NULL,(np>1)?params.item(2):NULL,(np>2)?params.item(3):NULL);
                    }
                    else if (stricmp(cmd,"workunittimings")==0) {
                        CHECKPARAMS(1,1);
                        workunittimings(params.item(1));
                    }
                    else if (stricmp(cmd,"serverlist")==0) {
                        CHECKPARAMS(1,1);
                        serverlist(params.item(1));
                    }
                    else if (stricmp(cmd,"clusterlist")==0) {
                        CHECKPARAMS(1,1);
                        clusterlist(params.item(1));
                    }
                    else if (stricmp(cmd,"auditlog")==0) {
                        CHECKPARAMS(2,3);
                        auditlog(params.item(1),params.item(2),(np>2)?params.item(3):NULL);
                    }
                    else if (stricmp(cmd,"coalesce")==0) {
                        CHECKPARAMS(0,0);
                        coalesce();
                    }
                    else if (stricmp(cmd,"mpping")==0) {
                        CHECKPARAMS(1,1);
                        mpping(params.item(1));
                    }
                    else if (stricmp(cmd,"daliping")==0) {
                        CHECKPARAMS(0,1);
                        daliping(daliserv.str(),daliconnectelapsed,(np>0)?atoi(params.item(1)):1);
                    }
                    else if (stricmp(cmd,"getxref")==0) {
                        CHECKPARAMS(1,1);
                        getxref(params.item(1));
                    }
                    else if (stricmp(cmd,"dalilocks")==0) {
                        CHECKPARAMS(0,2);
                        bool filesonly = false;
                        if (np&&(stricmp(params.item(np),"files")==0)) {
                            filesonly = true;
                            np--;
                        }
                        dalilocks(np>0?params.item(1):NULL,filesonly);
                    }
                    else if (stricmp(cmd,"unlock")==0) {
                        CHECKPARAMS(1,1);
                        unlock(params.item(1));
                    }
                    else if (stricmp(cmd,"wuidCompress")==0) {
                        CHECKPARAMS(2,2);
                        wuidCompress(params.item(1), params.item(2), true);
                    }
                    else if (stricmp(cmd,"wuidDecompress")==0) {
                        CHECKPARAMS(2,2);
                        wuidCompress(params.item(1), params.item(2), false);
                    }
                    else
                        ERRLOG("Unknown command %s",cmd);
                }
                catch (IException *e) {
                    EXCLOG(e,"daliadmin");
                    e->Release();
                }
                closedownClientProcess();
            }
        }
    }
    setDaliServixSocketCaching(false);
    setNodeCaching(false);
    releaseAtoms();
    fflush(stdout);
    fflush(stderr);
    return ret;
}

