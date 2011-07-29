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
#include "portlist.h"

#include "jlib.hpp"
#include "jio.hpp"
#include "jlog.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"

#include "sockfile.hpp"
#include "rmtfile.hpp"
#include "remoteerr.hpp"

//----------------------------------------------------------------------------

//#define TEST_DAFILESRV_FOR_UNIX_PATHS     // probably not needed


unsigned short getDaliServixPort()
{
    return DAFILESRV_PORT;
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




class CDaliServixIntercept: public CInterface, implements IFileCreateHook
{
public:
    IMPLEMENT_IINTERFACE;
    virtual IFile * createIFile(const RemoteFilename & filename)
    {
        SocketEndpoint ep = filename.queryEndpoint();
        bool noport = (ep.port==0);
        setDafsEndpointPort(ep);
        if (!filename.isLocal()||(ep.port!=DAFILESRV_PORT)) {   // assume standard port is running on local machine 
#ifdef __linux__
#ifndef USE_SAMBA   
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
        return NULL;
    }   
} *DaliServixIntercept = NULL;

bool testDaliServixPresent(const SocketEndpoint &_ep)
{
    SocketEndpoint ep(_ep);
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return false;
    try {
        Owned<ISocket> socket = ISocket::connect_timeout(ep,10000);
        return true;
    }
    catch (IException *e)
    {
        e->Release();
    }
    return false;
}

bool testDaliServixPresent(const IpAddress &ip)
{
    SocketEndpoint ep(0,ip);
    return testDaliServixPresent(ep);
}

unsigned getDaliServixVersion(const SocketEndpoint &_ep,StringBuffer &ver)
{
    SocketEndpoint ep(_ep);
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return false;
    try {
        Owned<ISocket> socket = ISocket::connect_timeout(ep,10000);
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
        SuperHashTableOf<CDafsOsCacheEntry,SocketEndpoint>::releaseAll();
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

int remoteExec(const SocketEndpoint &_ep,const char *cmdline, const char *workdir,bool sync,
                                 size32_t insize, void *inbuf, MemoryBuffer *outbuf)
{
    SocketEndpoint ep(_ep);
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return false;
    try {
        Owned<ISocket> socket = ISocket::connect_wait(ep,5000);
        return remoteExec(socket, cmdline, workdir, sync, insize, inbuf, outbuf);
    }
    catch (IException *e)
    {
        EXCLOG(e,"remoteExec");
        e->Release();
    }
    return -2;
}

extern REMOTE_API int setDafileSvrTraceFlags(const SocketEndpoint &_ep,byte flags)
{
    SocketEndpoint ep(_ep);
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return -3;
    try {
        Owned<ISocket> socket = ISocket::connect_wait(ep,5000);
        return setDafsTrace(socket, flags);
    }
    catch (IException *e)
    {
        EXCLOG(e,"setDafileSvrTraceFlags");
        e->Release();
    }
    return -2;
}

extern REMOTE_API int getDafileSvrInfo(const SocketEndpoint &_ep,StringBuffer &retstr)
{
    SocketEndpoint ep(_ep);
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return false;
    try {
        Owned<ISocket> socket = ISocket::connect_wait(ep,5000);
        return getDafsInfo(socket, retstr);
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

void cacheFileConnect(IFile *file,unsigned timeout)
{
    RemoteFilename rfn;
    rfn.setRemotePath(file->queryFilename());
    if (!rfn.isLocal()&&!rfn.isNull()) {
        SocketEndpoint ep = rfn.queryEndpoint();
        if (ep.port)
            clientCacheFileConnect(ep,timeout);
    }
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

class CScriptThread : public Thread
{
    StringAttr script;
    SocketEndpoint ep;
    Semaphore done;
    bool ok;
public:
    IMPLEMENT_IINTERFACE;
    CScriptThread(SocketEndpoint &_ep,const char *_script)
        : ep(_ep), script(_script)
    {
        ok = false;
    }

    int run()
    {
        try {
            int ret = remoteExec(ep,script.get(),"/c$",true,0,NULL,NULL);
            if (ret==0)
                ok = true;
        }
        catch (IException *e) {
            EXCLOG(e,"validateNodes CScriptThread");
            e->Release();
        }
        done.signal();
        return 0;
    }

    bool waitok(unsigned timeout)
    {
        done.wait(timeout);
        return ok;
    }
};

unsigned validateNodes(const SocketEndpointArray &eps,bool chkc,bool chkd,bool chkver,const char *script,unsigned scripttimeout,SocketEndpointArray &failures,UnsignedArray &failedcodes,StringArray &failedmessages, const char *filename)
{
    // used for detecting duff nodes
    PointerIArrayOf<ISocket> sockets;
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
        const PointerIArrayOf<ISocket> &sockets;
        SocketEndpointArray &failures;
        StringArray &failedmessages;
        UnsignedArray &failedcodes;
        CriticalSection &sect;
        bool chkc;
        bool chkd;
        bool chkv;
        const char *filename;
        const char *script;
        unsigned scripttimeout;
public:
        casyncfor(const SocketEndpointArray &_eps,const PointerIArrayOf<ISocket> &_sockets,bool _chkc,bool _chkd,bool _chkv, const char *_script, unsigned _scripttimeout, const char *_filename,SocketEndpointArray &_failures, StringArray &_failedmessages,UnsignedArray &_failedcodes,CriticalSection &_sect) 
            : eps(_eps), sockets(_sockets), 
              failures(_failures), failedmessages(_failedmessages), failedcodes(_failedcodes), sect(_sect)
        { 
            chkc = _chkc;
            chkd = _chkd;
            chkv = _chkv;
            filename = _filename;
            script = _script;
            scripttimeout = (script&&*script)?_scripttimeout:0;
        }
        void Do(unsigned i)
        {
#ifdef NIGEL_TESTING            
            IpAddress badip("10.173.34.70");
#endif
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
            if (!code&&(chkc||chkd)) {
                clientAddSocketToCache(ep,sock);
                StringBuffer path;
                if (iswin) 
                    path.append("c:\\");
                else
                    path.append("/c$/");
                if (filename)
                    path.append(filename);
                else {
                    path.append("dafs_");
                    genUUID(path);
                    path.append(".tmp");
                }
                for (unsigned drive=chkc?0:1;drive<(chkd?2U:1U);drive++) {
                    RemoteFilename rfn;
                    setPathDrive(path,drive);   
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
                            code |=  (drive?DAFS_VALIDATE_DISK_FULL_C:DAFS_VALIDATE_DISK_FULL_D);
                        else
                            code |=  (drive?DAFS_VALIDATE_WRITE_FAIL_C:DAFS_VALIDATE_WRITE_FAIL_D);
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
                        if ((rd!=sz)||(memcmp(buf,ds.str(),sz)!=0)
#ifdef NIGEL_TESTING            
                            ||(drive&&(badip.ipequals(ep)))
#endif
                            ) {
                            StringBuffer s;
                            ep.getIpText(s);
                            throw MakeStringException(-1,"Data discrepancy on disk read of %c$ of %s",'c'+drive,s.str());
                        }
                    }
                    catch (IException *e) {
                        code |=  (drive?DAFS_VALIDATE_READ_FAIL_C:DAFS_VALIDATE_READ_FAIL_D);
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
            }
            if (!code&&scripttimeout) { // use a second thread to implement script timeout
                Owned<CScriptThread> thread = new CScriptThread(ep,script);
                thread->start();
                if (!thread->waitok(scripttimeout)) {
                    code |=  DAFS_SCRIPT_FAIL;
                    if (errstr.length())
                        errstr.append(',');
                    errstr.append("FAILED: ").append(script);
                }
            }
            if (code) {
                CriticalBlock block(sect);
                failures.append(ep);
                failedcodes.append(code);
                failedmessages.append(errstr.str());
            }
        }
    } afor(eps,sockets,chkc,chkd,chkver,script,scripttimeout,filename,failures,failedmessages,failedcodes,sect);
    afor.For(eps.ordinality(), 10, false, true);
    return failures.ordinality();
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
}

