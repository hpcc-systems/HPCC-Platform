/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#include "jio.hpp"
#include "jlog.hpp"
#include "jregexp.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"

#include "sockfile.hpp"
#include "rmtfile.hpp"
#include "remoteerr.hpp"

//----------------------------------------------------------------------------

//#define TEST_DAFILESRV_FOR_UNIX_PATHS     // probably not needed

static class CSecuritySettings
{
    unsigned short daliServixPort;
public:
    CSecuritySettings()
    {
        querySecuritySettings(nullptr, &daliServixPort, nullptr, nullptr, nullptr);
    }

    unsigned short queryDaliServixPort() { return daliServixPort; }
} securitySettings;


unsigned short getDaliServixPort()
{
    return securitySettings.queryDaliServixPort();
}


void setCanAccessDirectly(RemoteFilename & file,bool set)
{
    if (set)
        file.setPort(0);
    else if (file.getPort()==0)                 // foreign daliservix may be passed in
        file.setPort(getDaliServixPort());

}

bool canAccessDirectly(const RemoteFilename & file) // not that well named but historical
{
    return (file.getPort()==0);
}

void setLocalMountRedirect(const IpAddress &ip,const char *dir,const char *mountdir)
{
    setDafsLocalMountRedirect(ip,dir,mountdir);
}



class CDaliServixFilter : public CInterface
{
protected:
    StringAttr dir, sourceRangeText;
    SocketEndpointArray sourceRangeIps;
    bool sourceRangeHasPorts, trace;

    bool checkForPorts(SocketEndpointArray &ips)
    {
        ForEachItemIn(i, ips)
        {
           if (ips.item(i).port)
               return true;
        }
        return false;
    }
public:
    CDaliServixFilter(const char *_dir, const char *sourceRange, bool _trace) : dir(_dir), trace(_trace)
    {
        if (sourceRange)
        {
            sourceRangeText.set(sourceRange);
            sourceRangeIps.fromText(sourceRange, 0);
            sourceRangeHasPorts = checkForPorts(sourceRangeIps);
        }
        else
            sourceRangeHasPorts = false;
    }
    bool queryTrace() const { return trace; }
    const char *queryDirectory() const { return dir; }
    bool testPath(const char *path) const
    {
        if (!dir) // if no dir in filter, match any
            return true;
        else
            return startsWith(path, dir.get());
    }
    bool applyFilter(const SocketEndpoint &ep) const
    {
        if (sourceRangeText.length())
        {
            SocketEndpoint _ep = ep;
            if (!sourceRangeHasPorts) // if source range doesn't have ports, only check ip
                _ep.port = 0;
            return NotFound != sourceRangeIps.find(_ep);
        }
        // NB: If no source range, use target range to decide if filter should apply
        return testEp(ep);
    }
    virtual bool testEp(const SocketEndpoint &ep) const = 0;
    virtual StringBuffer &getInfo(StringBuffer &info)
    {
        if (dir.length())
            info.append(", dir=").append(dir.get());
        if (sourceRangeText.get())
            info.append(", sourcerange=").append(sourceRangeText.get());
        info.append(", trace=(").append(trace ? "true" : "false").append(")");
        return info;
    }
};

class CDaliServixSubnetFilter : public CDaliServixFilter
{
    IpSubNet ipSubNet;
public:
    CDaliServixSubnetFilter(const char *subnet, const char *mask, const char *dir, const char *sourceRange, bool trace) :
        CDaliServixFilter(dir, sourceRange, trace)
    {
        if (!ipSubNet.set(subnet, mask))
            throw MakeStringException(0, "Invalid sub net definition: %s, %s", subnet, mask);
    }
    virtual bool testEp(const SocketEndpoint &ep) const
    {
        return ipSubNet.test(ep);
    }
    virtual StringBuffer &getInfo(StringBuffer &info)
    {
        info.append("subnet=");
        ipSubNet.getNetText(info);
        info.append(", mask=");
        ipSubNet.getMaskText(info);
        CDaliServixFilter::getInfo(info);
        return info;
    }
};

class CDaliServixRangeFilter : public CDaliServixFilter
{
    StringAttr rangeText;
    SocketEndpointArray rangeIps;
    bool rangeIpsHavePorts;
public:
    CDaliServixRangeFilter(const char *_range, const char *dir, const char *sourceRange, bool trace)
        : CDaliServixFilter(dir, sourceRange, trace)
    {
        rangeText.set(_range);
        rangeIps.fromText(_range, 0);
        rangeIpsHavePorts = checkForPorts(rangeIps);
    }
    virtual bool testEp(const SocketEndpoint &ep) const
    {
        SocketEndpoint _ep = ep;
        if (!rangeIpsHavePorts) // if range doesn't have ports, only check ip
            _ep.port = 0;
        return NotFound != rangeIps.find(_ep);
    }
    virtual StringBuffer &getInfo(StringBuffer &info)
    {
        info.append("range=").append(rangeText.get());
        CDaliServixFilter::getInfo(info);
        return info;
    }
};

CDaliServixFilter *createDaliServixFilter(IPropertyTree &filterProps)
{
    CDaliServixFilter *filter = NULL;
    const char *dir = filterProps.queryProp("@directory");
    const char *sourceRange = filterProps.queryProp("@sourcerange");
    bool trace = filterProps.getPropBool("@trace");
    if (filterProps.hasProp("@subnet"))
        filter = new CDaliServixSubnetFilter(filterProps.queryProp("@subnet"), filterProps.queryProp("@mask"), dir, sourceRange, trace);
    else if (filterProps.hasProp("@range"))
        filter = new CDaliServixRangeFilter(filterProps.queryProp("@range"), dir, sourceRange, trace);
    else
        throw MakeStringException(0, "Unknown DaliServix filter definition");
    return filter;
}

class CDaliServixIntercept: public CInterface, implements IDaFileSrvHook
{
    CIArrayOf<CDaliServixFilter> filters;
    StringAttr forceRemotePattern;

    void addFilter(CDaliServixFilter *filter)
    {
        filters.append(*filter);
        StringBuffer msg("DaFileSrvHook: adding translateToLocal [");
        filter->getInfo(msg);
        msg.append("]");
        PROGLOG("%s", msg.str());
    }
public:
    IMPLEMENT_IINTERFACE;
    virtual void forceRemote(const char *pattern)
    {
        forceRemotePattern.set(pattern);
    }
    virtual IFile * createIFile(const RemoteFilename & filename)
    {
        SocketEndpoint ep = filename.queryEndpoint();
        bool noport = (ep.port==0);
        setDafsEndpointPort(ep);
        if (!filename.isLocal()||(ep.port!=DAFILESRV_PORT && ep.port!=SECURE_DAFILESRV_PORT)) // assume standard port is running on local machine
        {
#ifdef __linux__
#ifndef USE_SAMBA
            if (noport && filters.ordinality())
            {
                ForEachItemIn(sn, filters)
                {
                    CDaliServixFilter &filter = filters.item(sn);
                    if (filter.testEp(ep))
                    {
                        StringBuffer lPath;
                        filename.getLocalPath(lPath);
                        if (filter.testPath(lPath.str()))
                        {
                            if (filter.queryTrace())
                            {
                                StringBuffer fromPath;
                                filename.getRemotePath(fromPath);
                                PROGLOG("Redirecting path: '%s' to '%s", fromPath.str(), lPath.str());
                            }
                            return ::createIFile(lPath.str());
                        }
                    }
                }
            }
            return createDaliServixFile(filename);
#endif
#endif
            if (!noport)            // expect all filenames that specify port to be dafilesrc or daliservix
                return createDaliServixFile(filename);  
            if (filename.isUnixPath()
#ifdef TEST_DAFILESRV_FOR_UNIX_PATHS        
                &&testDaliServixPresent(ep)
#endif
                )
                return createDaliServixFile(filename);  
        }
        else if (forceRemotePattern)
        {
            StringBuffer localPath;
            filename.getLocalPath(localPath);
            // must be local to be here, check if matches forceRemotePattern
            if (WildMatch(localPath, forceRemotePattern, false))
                return createDaliServixFile(filename);
        }
        return NULL;
    }
    virtual void addSubnetFilter(const char *subnet, const char *mask, const char *dir, const char *sourceRange, bool trace)
    {
        Owned<CDaliServixFilter> filter = new CDaliServixSubnetFilter(subnet, mask, dir, sourceRange, trace);
        addFilter(filter.getClear());
    }
    virtual void addRangeFilter(const char *range, const char *dir, const char *sourceRange, bool trace)
    {
        Owned<CDaliServixFilter> filter = new CDaliServixRangeFilter(range, dir, sourceRange, trace);
        addFilter(filter.getClear());
    }
    virtual IPropertyTree *addFilters(IPropertyTree *config, const SocketEndpoint *myEp)
    {
        if (!config)
            return NULL;
        Owned<IPropertyTree> result;
        Owned<IPropertyTreeIterator> iter = config->getElements("Filter");
        ForEach(*iter)
        {
            Owned<CDaliServixFilter> filter = createDaliServixFilter(iter->query());
            // Only add filters where myIP matches filter criteria
            if (!myEp || filter->applyFilter(*myEp))
            {
                addFilter(filter.getClear());
                if (!result)
                    result.setown(createPTree());
                result->addPropTree("Filter", LINK(&iter->query()));
            }
        }
        return result.getClear();
    }
    virtual IPropertyTree *addMyFilters(IPropertyTree *config, SocketEndpoint *myEp)
    {
        if (myEp)
            return addFilters(config, myEp);
        else
        {
            SocketEndpoint ep;
            GetHostIp(ep);
            return addFilters(config, &ep);
        }
    }
    virtual void clearFilters()
    {
        filters.kill();
    }
} *DaliServixIntercept = NULL;

unsigned short getActiveDaliServixPort(const IpAddress &ip)
{
    if (ip.isNull())
        return 0;
    SocketEndpoint ep(0, ip);
    setDafsEndpointPort(ep);
    try {
        Owned<ISocket> socket = connectDafs(ep, 10000);
        return ep.port;
    }
    catch (IException *e)
    {
        e->Release();
    }
    return 0;
}

bool testDaliServixPresent(const IpAddress &ip)
{
    return getActiveDaliServixPort(ip) != 0;
}

unsigned getDaliServixVersion(const SocketEndpoint &_ep,StringBuffer &ver)
{
    SocketEndpoint ep(_ep);
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return 0;
    try
    {
        Owned<ISocket> socket = connectDafs(ep, 10000);
        return getRemoteVersion(socket,ver);
    }
    catch (IException *e)
    {
        EXCLOG(e,"getDaliServixVersion");
        e->Release();
    }
    return 0;
}

struct CDafsOsCacheEntry
{
    SocketEndpoint ep;
    DAFS_OS os;
    time_t at;
};

class CDafsOsCache: public SuperHashTableOf<CDafsOsCacheEntry,SocketEndpoint>
{
    void onAdd(void *) {}

    void onRemove(void *et)
    {
        CDafsOsCacheEntry *e = (CDafsOsCacheEntry *)et;
        delete e;
    }
    unsigned getHashFromElement(const void *e) const
    {
        const CDafsOsCacheEntry &elem=*(const CDafsOsCacheEntry *)e;        
        return elem.ep.hash(0);
    }

    unsigned getHashFromFindParam(const void *fp) const
    {
        return ((const SocketEndpoint *)fp)->hash(0);
    }

    const void * getFindParam(const void *p) const
    {
        const CDafsOsCacheEntry &elem=*(const CDafsOsCacheEntry *)p;        
        return (void *)&elem.ep;
    }

    bool matchesFindParam(const void * et, const void *fp, unsigned) const
    {
        return ((CDafsOsCacheEntry *)et)->ep.equals(*(SocketEndpoint *)fp);
    }

    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(CDafsOsCacheEntry,SocketEndpoint);

public:
    static CriticalSection crit;

    CDafsOsCache() 
    {
    }
    ~CDafsOsCache()
    {
        SuperHashTableOf<CDafsOsCacheEntry,SocketEndpoint>::_releaseAll();
    }
    DAFS_OS lookup(const SocketEndpoint &ep,ISocket *sock)
    {
        CriticalBlock block(crit);
        CDafsOsCacheEntry *r = SuperHashTableOf<CDafsOsCacheEntry,SocketEndpoint>::find(&ep);
        bool needupdate=false;
        unsigned t = (unsigned)time(NULL);
        if (!r) {
            r = new CDafsOsCacheEntry;
            r->ep = ep;
            needupdate = true;
            SuperHashTableOf<CDafsOsCacheEntry,SocketEndpoint>::add(*r);
        }
        else
            needupdate = (t-r->at>60*5);        // update every 5 mins
        if (needupdate) {
            r->os = DAFS_OSunknown;
            StringBuffer ver;
            unsigned ret;
            if (sock)
                ret = getRemoteVersion(sock,ver);
            else
                ret = getDaliServixVersion(ep,ver);
            if (ret!=0) { // if cross-os needs dafilesrv
                if (strstr(ver.str(),"Linux")!=NULL)
                    r->os = DAFS_OSlinux;
                else if (strstr(ver.str(),"Windows")!=NULL)
                    r->os = DAFS_OSwindows;
                else if (strstr(ver.str(),"Solaris")!=NULL)
                    r->os = DAFS_OSsolaris;
            }
            r->at = t;
        }
        return r->os;
    }
};


CriticalSection CDafsOsCache::crit;


DAFS_OS getDaliServixOs(const SocketEndpoint &ep,ISocket *socket)
{
#ifdef _DEBUG
    if (ep.isLocal())
#ifdef _WIN32
        return DAFS_OSwindows;
#else
        return DAFS_OSlinux;
#endif
#endif
    static CDafsOsCache cache;
    return cache.lookup(ep,socket);
}



DAFS_OS getDaliServixOs(const SocketEndpoint &ep)
{
    return getDaliServixOs(ep,NULL);
}


unsigned getDaliServixVersion(const IpAddress &ip,StringBuffer &ver)
{
    SocketEndpoint ep(0,ip);
    return getDaliServixVersion(ep,ver);
}

extern REMOTE_API int setDafileSvrTraceFlags(const SocketEndpoint &_ep,byte flags)
{
    SocketEndpoint ep(_ep);
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return -3;
    try {
        Owned<ISocket> socket = connectDafs(ep, 5000);
        return setDafsTrace(socket, flags);
    }
    catch (IException *e)
    {
        EXCLOG(e,"setDafileSvrTraceFlags");
        e->Release();
    }
    return -2;
}

extern REMOTE_API int setDafileSvrThrottleLimit(const SocketEndpoint &_ep, ThrottleClass throttleClass, unsigned throttleLimit, unsigned throttleDelayMs, unsigned throttleCPULimit, unsigned queueLimit, StringBuffer *errMsg)
{
    SocketEndpoint ep(_ep);
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return -3;
    try {
        Owned<ISocket> socket = connectDafs(ep, 5000);
        return setDafsThrottleLimit(socket, throttleClass, throttleLimit, throttleDelayMs, throttleCPULimit, queueLimit, errMsg);
    }
    catch (IException *e)
    {
        EXCLOG(e,"setDafileSvrThrottleLimit");
        e->Release();
    }
    return -2;
}

extern REMOTE_API int getDafileSvrInfo(const SocketEndpoint &_ep, unsigned level, StringBuffer &retstr)
{
    SocketEndpoint ep(_ep);
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return false;
    try {
        Owned<ISocket> socket = connectDafs(ep, 5000);
        return getDafsInfo(socket, level, retstr);
    }
    catch (IException *e)
    {
        EXCLOG(e,"getDafileSvrInfo");
        e->Release();
    }
    return -2;
}


void remoteExtractBlobElements(const char * prefix, const RemoteFilename &file, ExtractedBlobArray & extracted)
{
    SocketEndpoint ep(file.queryEndpoint());
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return;
    StringBuffer filename;
    remoteExtractBlobElements(ep, prefix, file.getLocalPath(filename).str(), extracted);
}


IFile *createDaliServixFile(const RemoteFilename & file)
{
    SocketEndpoint ep(file.queryEndpoint());
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return NULL;
    StringBuffer path;
    file.getLocalPath(path);
    return createRemoteFile(ep, path.str());
}

void setDaliServixSocketCaching(bool set)
{
    clientSetDaliServixSocketCaching(set);
}

void disconnectRemoteFile(IFile *file)
{
    clientDisconnectRemoteFile(file);
}

void disconnectRemoteIoOnExit(IFileIO *fileio,bool set)
{
    clientDisconnectRemoteIoOnExit(fileio,set);
}


bool resetRemoteFilename(IFile *file, const char *newname)
{
    return clientResetFilename(file,newname); 
}


void enableAuthentication(bool set)
{
    enableDafsAuthentication(set);
}

bool asyncCopyFileSection(const char *uuid,                 // from genUUID - must be same for subsequent calls
                            IFile *from,                        // expected to be remote
                            RemoteFilename &to,
                            offset_t toofs,                     // (offset_t)-1 created file and copies to start
                            offset_t fromofs,
                            offset_t size,                      // (offset_t)-1 for all file
                            ICopyFileProgress *progress,
                            unsigned timeout                    // 0 to start, non-zero to wait
                        )
{
    return  clientAsyncCopyFileSection(uuid,from,to,toofs,fromofs,size,progress,timeout);
}


void setRemoteFileTimeouts(unsigned maxconnecttime,unsigned maxreadtime)
{
    clientSetRemoteFileTimeouts(maxconnecttime,maxreadtime);
}

unsigned validateNodes(const SocketEndpointArray &epso,const char *dataDir, const char *mirrorDir, bool chkver, SocketEndpointArray &failures, UnsignedArray &failedcodes, StringArray &failedmessages, const char *filename)
{
    // used for detecting duff nodes
    IPointerArrayOf<ISocket> sockets;
    // dedup nodes
    SocketEndpointArray eps;
    ForEachItemIn(i1,epso)
        eps.appendUniq(epso.element(i1));
    unsigned to=30*1000;
    unsigned n=eps.ordinality();    // use approx log scale (timeout is long but only for failure situation)
    while (n>1) {
        n/=2;
        to+=30*1000;
    }
    multiConnect(eps,sockets,to);
    ForEachItemIn(i,eps) {
        if (sockets.item(i)==NULL) {
            failures.append(eps.item(i));
            failedcodes.append(DAFS_VALIDATE_CONNECT_FAIL);
            failedmessages.append("Connect failure");
        }
    }

    CriticalSection sect;
    class casyncfor: public CAsyncFor
    {
        const SocketEndpointArray &eps;
        const IPointerArrayOf<ISocket> &sockets;
        SocketEndpointArray &failures;
        StringArray &failedmessages;
        UnsignedArray &failedcodes;
        CriticalSection &sect;
        StringAttr dataDir, mirrorDir;
        bool chkv;
        const char *filename;
public:
        casyncfor(const SocketEndpointArray &_eps,const IPointerArrayOf<ISocket> &_sockets,const char *_dataDir,const char *_mirrorDir,bool _chkv, const char *_filename,SocketEndpointArray &_failures, StringArray &_failedmessages,UnsignedArray &_failedcodes,CriticalSection &_sect)
            : eps(_eps), sockets(_sockets), failures(_failures),
              failedmessages(_failedmessages), failedcodes(_failedcodes), sect(_sect),
              dataDir(_dataDir), mirrorDir(_mirrorDir)
        { 
            chkv = _chkv;
            filename = _filename;
        }
        void Do(unsigned i)
        {
            ISocket *sock = sockets.item(i);
            if (!sock)
                return;
            SocketEndpoint ep = eps.item(i);
            bool iswin;
            unsigned code = 0;
            StringBuffer errstr;
            StringBuffer ver;
            try {
                getRemoteVersion(sock,ver);
                iswin = (strstr(ver.str(),"Windows")!=NULL);
            }
            catch (IException *e)
            {
                code = DAFS_VALIDATE_CONNECT_FAIL;
                e->errorMessage(errstr);
                e->Release();
            }
            if (!code&&chkv) {
                const char *rv = ver.str();
                const char *v = remoteServerVersionString();
                while (*v&&(*v!='-')&&(*v==*rv)) {
                    v++;
                    rv++;
                }
                if (*rv!=*v) {
                    if (*rv) {
                        while (*rv&&(*rv!='-'))
                            rv++;
                        while (*v&&(*v!='-'))
                            v++;
                        StringBuffer wanted(v-remoteServerVersionString(),remoteServerVersionString());
                        ver.setLength(rv-ver.str());
                        if (strcmp(ver.str(),wanted.str())<0) { // allow >
                            code = DAFS_VALIDATE_BAD_VERSION;
                            errstr.appendf("Mismatch dafilesrv version ");
                            errstr.append(rv-ver.str(),ver.str());
                            errstr.append(", wanted ");
                            errstr.append(v-remoteServerVersionString(),remoteServerVersionString());
                        }
                    }
                    else {
                        code = DAFS_VALIDATE_CONNECT_FAIL;
                        errstr.appendf("could not contact dafilesrv");
                    }
                }
            }
            if (!code&&(dataDir.get()||mirrorDir.get())) {
                clientAddSocketToCache(ep,sock);
                const char *drivePath = NULL;
                const char *drivePaths[2];
                unsigned drives=0;
                if (mirrorDir.get()) drivePaths[drives++] = mirrorDir.get();
                if (dataDir.get()) drivePaths[drives++] = dataDir.get();
                do
                {
                    StringBuffer path(drivePaths[--drives]);
                    addPathSepChar(path);
                    if (filename)
                        path.append(filename);
                    else {
                        path.append("dafs_");
                        genUUID(path);
                        path.append(".tmp");
                    }
                    RemoteFilename rfn;
                    rfn.setPath(ep,path);
                    Owned<IFile> file = createIFile(rfn);
                    size32_t sz;
                    StringBuffer ds;
                    try {
                        Owned<IFileIO> fileio = file->open(IFOcreate);
                        CDateTime dt;
                        dt.setNow();
                        dt.getString(ds);
                        sz = ds.length()+1;
                        assertex(sz<64);
                        fileio->write(0,sz,ds.str());
                    }
                    catch (IException *e) {
                        if (e->errorCode()==DISK_FULL_EXCEPTION_CODE)
                            code |=  (drivePath==dataDir.get()?DAFS_VALIDATE_DISK_FULL_DATA:DAFS_VALIDATE_DISK_FULL_MIRROR);
                        else
                            code |=  (drivePath==dataDir.get()?DAFS_VALIDATE_WRITE_FAIL_DATA:DAFS_VALIDATE_WRITE_FAIL_MIRROR);
                        if (errstr.length())
                            errstr.append(',');
                        e->errorMessage(errstr);
                        e->Release();
                        continue; // no use trying read
                    }
                    try {
                        Owned<IFileIO> fileio = file->open(IFOread);
                        char buf[64];
                        size32_t rd = fileio->read(0,sizeof(buf)-1,buf);
                        if ((rd!=sz)||(memcmp(buf,ds.str(),sz)!=0)) {
                            StringBuffer s;
                            ep.getIpText(s);
                            throw MakeStringException(-1,"Data discrepancy on disk read of %s of %s",path.str(),s.str());
                        }
                    }
                    catch (IException *e) {
                        code |=  (drivePath==dataDir.get()?DAFS_VALIDATE_READ_FAIL_DATA:DAFS_VALIDATE_READ_FAIL_MIRROR);
                        if (errstr.length())
                            errstr.append(',');
                        e->errorMessage(errstr);
                        e->Release();
                    }
                    if (!filename||!*filename) {
                        // delete file created
                        try {
                            file->remove();
                        }
                        catch (IException *e) {
                            e->Release();           // supress error
                        }
                    }
                }
                while (0 != drives);
            }
            if (code) {
                CriticalBlock block(sect);
                failures.append(ep);
                failedcodes.append(code);
                failedmessages.append(errstr.str());
            }
        }
    } afor(eps,sockets,dataDir,mirrorDir,chkver,filename,failures,failedmessages,failedcodes,sect);
    afor.For(eps.ordinality(), 10, false, true);
    return failures.ordinality();
}

static PointerArrayOf<SharedObject> *hookDlls;

static void installFileHook(const char *hookFileSpec);

extern REMOTE_API void installFileHooks(const char *hookFileSpec)
{
    if (!hookDlls)
        hookDlls = new PointerArrayOf<SharedObject>;
    const char * cursor = hookFileSpec;
    for (;*cursor;)
    {
        StringBuffer file;
        while (*cursor && *cursor != ENVSEPCHAR)
            file.append(*cursor++);
        if(*cursor)
            cursor++;
        if(!file.length())
            continue;
        installFileHook(file);
    }
}

typedef void *(HookInstallFunction)();

static void installFileHook(const char *hookFile)
{
    StringBuffer dirPath, dirTail, absolutePath;
    splitFilename(hookFile, &dirPath, &dirPath, &dirTail, &dirTail);
    makeAbsolutePath(dirPath.str(), absolutePath);
    if (!containsFileWildcard(dirTail))
    {
        addPathSepChar(absolutePath).append(dirTail);
        Owned<IFile> file = createIFile(absolutePath);
        if (file->isDirectory() == foundYes)
        {
            installFileHooks(addPathSepChar(absolutePath).append('*'));
        }
        else if (file->isFile() == foundYes)
        {
            HookInstallFunction *hookInstall;
            SharedObject *so = new SharedObject(); // MORE - this leaks! Kind-of deliberate right now...
            if (so->load(file->queryFilename(), false) &&
                (hookInstall = (HookInstallFunction *) GetSharedProcedure(so->getInstanceHandle(), "installFileHook")) != NULL)
            {
                hookInstall();
                hookDlls->append(so);
            }
            else
            {
                so->unload();
                delete so;
                OERRLOG("File hook library %s could not be loaded", hookFile);
            }
        }
        else
        {
            OERRLOG("File hook library %s not found", hookFile);
        }
    }
    else
    {
        Owned<IDirectoryIterator> dir = createDirectoryIterator(absolutePath, dirTail);
        ForEach(*dir)
        {
            const char *name = dir->query().queryFilename();
            if (name && *name && *name != '.')
                installFileHook(name);
        }
    }
}

// Should be called before closedown, ideally. MODEXIT tries to mop up but may be too late to do so cleanly

extern REMOTE_API void removeFileHooks()
{
    if (hookDlls)
    {
        ForEachItemIn(idx, *hookDlls)
        {
            SharedObject *so = hookDlls->item(idx);
            HookInstallFunction *hookInstall = (HookInstallFunction *) GetSharedProcedure(so->getInstanceHandle(), "removeFileHook");
            if (hookInstall)
                hookInstall();
            delete so;
        }
        delete hookDlls;
        hookDlls = NULL;
    }
}

MODULE_INIT(INIT_PRIORITY_REMOTE_RMTFILE)
{
    if(!DaliServixIntercept)
    {
        DaliServixIntercept = new CDaliServixIntercept;
        addIFileCreateHook(DaliServixIntercept);
    }
    return true;
}

MODULE_EXIT()
{
    if(DaliServixIntercept)
    {
        // delete ConnectionTable;              // too late to delete (jsocket closed down)
        removeIFileCreateHook(DaliServixIntercept);
        ::Release(DaliServixIntercept);
        DaliServixIntercept = NULL;
    }
    removeFileHooks();
}

IDaFileSrvHook *queryDaFileSrvHook()
{
    return DaliServixIntercept;
}

void enableForceRemoteReads()
{
    const char *forceRemotePattern = queryEnvironmentConf().queryProp("forceRemotePattern");
    if (!isEmptyString(forceRemotePattern))
        queryDaFileSrvHook()->forceRemote(forceRemotePattern);
}

bool testForceRemote(const char *path)
{
    const char *forceRemotePattern = queryEnvironmentConf().queryProp("forceRemotePattern");
    return !isEmptyString(forceRemotePattern) && WildMatch(path, forceRemotePattern, false);
}
