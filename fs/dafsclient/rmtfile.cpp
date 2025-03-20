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

#include <string>
#include <unordered_map>

#include "platform.h"
#include "portlist.h"

#include "jlib.hpp"
#include "jflz.hpp"
#include "jio.hpp"
#include "jlog.hpp"
#include "jregexp.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"
#include "jhtree.hpp"
#include "jsecrets.hpp"

#include "rtldynfield.hpp"
#include "rtlds_imp.hpp"
#include "rtlread_imp.hpp"
#include "rtlrecord.hpp"
#include "eclhelper_dyn.hpp"

#include "rtlcommon.hpp"
#include "rtlformat.hpp"

#include "digisign.hpp"


#include "remoteerr.hpp"
#include "rmtclient.hpp"

#include "rmtclient_impl.hpp"
#include "rmtfile.hpp"

//----------------------------------------------------------------------------

//#define TEST_DAFILESRV_FOR_UNIX_PATHS     // probably not needed

static std::atomic<unsigned> dafilesrvPort{(unsigned)-1};
static CriticalSection dafilesrvCs;
unsigned short getDaliServixPort()
{
    if (dafilesrvPort == (unsigned)-1)
    {
        CriticalBlock block(dafilesrvCs);
        if (dafilesrvPort == (unsigned) -1)
        {
            unsigned short daliServixPort;
            querySecuritySettings(nullptr, &daliServixPort, nullptr, nullptr, nullptr);
            dafilesrvPort = daliServixPort;
        }
    }
    return dafilesrvPort;
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
    CriticalSection secretCrit;
    std::unordered_map<std::string, std::tuple<unsigned, std::string>> endpointMap;

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
            // check 1st if this is a secret based url
            StringBuffer storageSecret;
            getSecretBased(storageSecret, filename);
            if (storageSecret.length())
                return createDaliServixFile(filename, storageSecret);

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
#else
            if (!noport)            // expect all filenames that specify port to be dafilesrc or daliservix
                return createDaliServixFile(filename);  
            if (filename.isUnixPath()
#ifdef TEST_DAFILESRV_FOR_UNIX_PATHS        
                &&testDaliServixPresent(ep)
#endif
                )
                return createDaliServixFile(filename);  
#endif
        }
        else if (forceRemotePattern)
        {
            StringBuffer localPath;
            filename.getLocalPath(localPath);
            // must be local to be here, check if matches forceRemotePattern
            if (WildMatch(localPath, forceRemotePattern, false))
            {
                // check 1st if this is a secret based url
                StringBuffer storageSecret;
                getSecretBased(storageSecret, filename);
                if (storageSecret.length())
                    return createDaliServixFile(filename, storageSecret);
                else
                    return createDaliServixFile(filename);
            }
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
    virtual StringBuffer &getSecretBased(StringBuffer &storageSecret, const RemoteFilename & filename) override
    {
        CLeavableCriticalBlock b(secretCrit);
        if (!endpointMap.empty())
        {
            const SocketEndpoint &ep = filename.queryEndpoint();

            StringBuffer endpointStr;
            ep.getEndpointHostText(endpointStr);

            auto it = endpointMap.find(endpointStr.str());
            if (it != endpointMap.end())
            {
                storageSecret.append(std::get<1>(it->second).c_str());
                b.leave();
                if (0 == storageSecret.length())
                {
                    VStringBuffer secureUrl("https://%s", endpointStr.str());
                    generateDynamicUrlSecretName(storageSecret, secureUrl, nullptr);
                }
            }
        }
        return storageSecret;
    }
    virtual void addSecretEndpoint(const char *endpoint, const char *optSecret) override
    {
        CriticalBlock b(secretCrit);
        auto it = endpointMap.find(endpoint);
        if (it == endpointMap.end())
            endpointMap[endpoint] = { 1, optSecret ? optSecret : "" };
    }
    virtual void removeSecretEndpoint(const char *endpoint) override
    {
        CriticalBlock b(secretCrit);
        auto it = endpointMap.find(endpoint);
        assertex(it != endpointMap.end());
        if (--std::get<0>(it->second) == 0)
            endpointMap.erase(it);
    }
} *DaliServixIntercept = NULL;


void remoteExtractBlobElements(const char * prefix, const RemoteFilename &file, ExtractedBlobArray & extracted)
{
    SocketEndpoint ep(file.queryEndpoint());
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return;
    StringBuffer filename;
    remoteExtractBlobElements(ep, prefix, file.getLocalPath(filename).str(), extracted);
}


//---------------------------------------------------------------------------

class CRemoteDirectoryIterator : implements IDirectoryDifferenceIterator, public CInterface
{
    Owned<IFile>    cur;
    bool            curvalid;
    bool            curisdir;
    StringAttr      curname;
    CDateTime       curdt;
    __int64         cursize;
    StringAttr      dir;
    SocketEndpoint  ep;
    byte            *flags;
    unsigned        numflags;
    unsigned        curidx;
    unsigned        mask;

    MemoryBuffer buf;
public:
    static CriticalSection      crit;

    CRemoteDirectoryIterator(const SocketEndpoint &_ep,const char *_dir)
        : dir(_dir)
    {
        // an extended difference iterator starts with 2 (for bwd compatibility)
        ep = _ep;
        curisdir = false;
        curvalid = false;
        cursize = 0;
        curidx = (unsigned)-1;
        mask = 0;
        numflags = 0;
        flags = NULL;
    }

    bool appendBuf(MemoryBuffer &_buf)
    {
        buf.setSwapEndian(_buf.needSwapEndian());
        byte hdr;
        _buf.read(hdr);
        if (hdr==2) {
            _buf.read(numflags);
            flags = (byte *)malloc(numflags);
            _buf.read(numflags,flags);
        }
        else {
            buf.append(hdr);
            flags = NULL;
            numflags = 0;
        }
        size32_t rest = _buf.length()-_buf.getPos();
        const byte *rb = (const byte *)_buf.readDirect(rest);
        bool ret = true;
        // At the last byte of the rb (rb[rest-1]) is the stream live flag
        //  True if the stream has more data
        //  False at the end of stream
        // The previous byte (rb[rest-2]) is the flag to signal there are more
        // valid entries in this block
        //  True if there are valid directory entry follows this flag
        //  False if there are no more valid entry in this block aka end of block
        // If there is more data in the stream, the end of block flag should be removed
        if (rest&&(rb[rest-1]!=0))
        {
            rest--; // remove stream live flag
            if(rest && (0 == rb[rest-1]))
            	rest--; //Remove end of block flag
            ret = false;  // continuation
        }
        buf.append(rest,rb);
        return ret;
    }

    ~CRemoteDirectoryIterator()
    {
        free(flags);
    }

    IMPLEMENT_IINTERFACE

    bool first()
    {
        curidx = (unsigned)-1;
        buf.reset();
        return next();
    }
    bool next()
    {
        for (;;) {
            curidx++;
            cur.clear();
            curdt.clear();
            curname.clear();
            cursize = 0;
            curisdir = false;
            if (buf.getPos()>=buf.length())
                return false;
            byte isValidEntry;
            buf.read(isValidEntry);
            curvalid = isValidEntry!=0;
            if (!curvalid)
                return false;
            buf.read(curisdir);
            buf.read(cursize);
            curdt.deserialize(buf);
            buf.read(curname);
            // kludge for bug in old linux jlibs
            if (strchr(curname,'\\')&&(getPathSepChar(dir)=='/')) {
                StringBuffer temp(curname);
                temp.replace('\\','/');
                curname.set(temp.str());
            }
            if ((mask==0)||(getFlags()&mask))
                break;
        }
        return true;
    }

    bool isValid()
    {
        return curvalid;
    }
    IFile & query()
    {
        if (!cur) {
            StringBuffer full(dir);
            addPathSepChar(full).append(curname);
            if (ep.isNull())
                cur.setown(createIFile(full.str()));
            else {
                RemoteFilename rfn;
                rfn.setPath(ep,full.str());
                cur.setown(createIFile(rfn));
            }
        }
        return *cur;
    }
    StringBuffer &getName(StringBuffer &buf)
    {
        return buf.append(curname);
    }
    bool isDir()
    {
        return curisdir;
    }

    __int64 getFileSize()
    {
        if (curisdir)
            return -1;
        return cursize;
    }


    bool getModifiedTime(CDateTime &ret)
    {
        ret = curdt;
        return true;
    }

    void setMask(unsigned _mask)
    {
        mask = _mask;
    }

    virtual unsigned getFlags()
    {
        if (flags&&(curidx<numflags))
            return flags[curidx];
        return 0;
    }

    static bool serialize(MemoryBuffer &mb,IDirectoryIterator *iter, size32_t bufsize, bool first)
    {
        bool ret = true;
        byte b=1;
        StringBuffer tmp;
        if (first ? iter->first() : iter->next()) {
            for (;;) {
                mb.append(b);
                bool isdir = iter->isDir();
                __int64 sz = isdir?0:iter->getFileSize();
                CDateTime dt;
                iter->getModifiedTime(dt);
                iter->getName(tmp.clear());
                mb.append(isdir).append(sz);
                dt.serialize(mb);
                mb.append(tmp.str());
                if (bufsize&&(mb.length()>=bufsize-1)) {
                    ret = false;
                    break;
                }
                if (!iter->next())
                    break;
            }
        }
        b = 0;
        mb.append(b);
        return ret;
    }

    static void serializeDiff(MemoryBuffer &mb,IDirectoryDifferenceIterator *iter)
    {
        // bit slow
        MemoryBuffer flags;
        ForEach(*iter)
            flags.append((byte)iter->getFlags());
        if (flags.length()) {
            byte b = 2;
            mb.append(b).append((unsigned)flags.length()).append(flags);
        }
        serialize(mb,iter,0,true);
    }

    void serialize(MemoryBuffer &mb,bool isdiff)
    {
        byte b;
        if (isdiff&&numflags&&flags) {
            b = 2;
            mb.append(b).append(numflags).append(numflags,flags);
        }
        serialize(mb,this,0,true);
    }
};

IDirectoryIterator *createRemoteDirectorIterator(const SocketEndpoint &ep, const char *name, MemoryBuffer &state)
{
    Owned<CRemoteDirectoryIterator> di = new CRemoteDirectoryIterator(ep, name);
    di->appendBuf(state);
    return di.getClear();
}

bool serializeRemoteDirectoryIterator(MemoryBuffer &tgt, IDirectoryIterator *iter, size32_t bufsize, bool first)
{
    return CRemoteDirectoryIterator::serialize(tgt, iter, bufsize, first);
}

void serializeRemoteDirectoryDiff(MemoryBuffer &tgt, IDirectoryDifferenceIterator *iter)
{
    CRemoteDirectoryIterator::serializeDiff(tgt, iter);
}

class CCritTable;
class CEndpointCS : public CriticalSection, public CInterface
{
    CCritTable &table;
    const SocketEndpoint ep;
public:
    CEndpointCS(CCritTable &_table, const SocketEndpoint &_ep) : table(_table), ep(_ep) { }
    const void *queryFindParam() const { return &ep; }

    virtual void beforeDispose();
};

class CCritTable : private SimpleHashTableOf<CEndpointCS, const SocketEndpoint>
{
    typedef SimpleHashTableOf<CEndpointCS, const SocketEndpoint> PARENT;
    CriticalSection crit;
public:
    CEndpointCS *getCrit(const SocketEndpoint &ep)
    {
        CriticalBlock b(crit);
        CEndpointCS * clientCrit = find(ep);
        if (!clientCrit || !clientCrit->isAliveAndLink()) // if !isAliveAndLink(), then it is in the process of being destroyed/removed.
        {
            clientCrit = new CEndpointCS(*this, ep);
            replace(*clientCrit); // NB table doesn't own
        }
        return clientCrit;
    }
    unsigned getHashFromElement(const void *e) const
    {
        const CEndpointCS &elem=*(const CEndpointCS *)e;
        return getHashFromFindParam(elem.queryFindParam());
    }

    unsigned getHashFromFindParam(const void *fp) const
    {
        return ((const SocketEndpoint *)fp)->hash(0);
    }

    void removeExact(CEndpointCS *clientCrit)
    {
        CriticalBlock b(crit);
        PARENT::removeExact(clientCrit); // NB may not exist, could have been replaced if detected !isAlive() in getCrit()
    }
} *dirCSTable;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    dirCSTable = new CCritTable;
    return true;
}
MODULE_EXIT()
{
    delete dirCSTable;
}

void CEndpointCS::beforeDispose()
{
    table.removeExact(this);
}

//---------------------------------------------------------------------------

class CRemoteFile : public CRemoteBase, implements IFile
{
    StringAttr remotefilename;
    unsigned flags;
    bool isShareSet;

    void commonInit()
    {
        flags = ((unsigned)IFSHread)|((S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH)<<16);
        isShareSet = false;
        if (filename.length()>2 && isPathSepChar(filename[0]) && isShareChar(filename[2]))
        {
            VStringBuffer winDriveFilename("%c:%s", filename[1], filename+3);
            filename.set(winDriveFilename);
        }
    }
public:
    IMPLEMENT_IINTERFACE_O_USING(CRemoteBase);

    CRemoteFile(const SocketEndpoint &_ep, const char * _filename)
        : CRemoteBase(_ep, _filename)
    {
        commonInit();
    }

    CRemoteFile(const SocketEndpoint &ep, const char *filename, const char *storageSecret)
        : CRemoteBase(ep, storageSecret, filename)
    {
        commonInit();
    }

    bool exists()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCexists).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        bool ok;
        replyBuffer.read(ok);
        return ok;
    }

    bool getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime)
    {
        CDateTime dummyTime;
        if (!createTime)
            createTime = &dummyTime;
        if (!modifiedTime)
            modifiedTime = &dummyTime;
        if (!accessedTime)
            accessedTime = &dummyTime;
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCgettime).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        bool ok;
        replyBuffer.read(ok);
        if (ok) {
            createTime->deserialize(replyBuffer);
            modifiedTime->deserialize(replyBuffer);
            accessedTime->deserialize(replyBuffer);
        }
        return ok;
    }

    bool setTime(const CDateTime * createTime, const CDateTime * modifiedTime, const CDateTime * accessedTime)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCsettime).append(filename);
        if (createTime)
        {
            sendBuffer.append((bool)true);
            createTime->serialize(sendBuffer);
        }
        else
            sendBuffer.append((bool)false);
        if (modifiedTime)
        {
            sendBuffer.append((bool)true);
            modifiedTime->serialize(sendBuffer);
        }
        else
            sendBuffer.append((bool)false);
        if (accessedTime)
        {
            sendBuffer.append((bool)true);
            accessedTime->serialize(sendBuffer);
        }
        else
            sendBuffer.append((bool)false);
        sendRemoteCommand(sendBuffer, replyBuffer);

        bool ok;
        replyBuffer.read(ok);
        return ok;
    }

    fileBool isDirectory()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCisdirectory).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        unsigned ret;
        replyBuffer.read(ret);
        return (fileBool)ret;
    }


    fileBool isFile()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCisfile).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        unsigned ret;
        replyBuffer.read(ret);
        return (fileBool)ret;
    }

    fileBool isReadOnly()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCisreadonly).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        unsigned ret;
        replyBuffer.read(ret);
        return (fileBool)ret;
    }

    IFileIO * open(IFOmode mode,IFEflags extraFlags=IFEnone);
    IFileIO * openShared(IFOmode mode,IFSHmode shmode,IFEflags extraFlags=IFEnone);
    IFileAsyncIO * openAsync(IFOmode mode) { return NULL; } // not supported

    const char * queryFilename()
    {
        if (remotefilename.isEmpty()) {
            RemoteFilename rfn;
            rfn.setPath(ep,filename);
            StringBuffer path;
            rfn.getRemotePath(path);
            remotefilename.set(path);
        }
        return remotefilename.get();
    }

    void resetLocalFilename(const char *name)
    {
        remotefilename.clear();
        filename.set(name);
    }

    bool remove()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCremove).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        bool ok;
        replyBuffer.read(ok);
        return ok;
    }

    void rename(const char *newname)
    {
    // currently ignores directory on newname (in future versions newname will be required to be tail only and not full path)
        StringBuffer path;
        splitDirTail(filename,path);
        StringBuffer newdir;
        path.append(splitDirTail(newname,newdir));
        if (newdir.length()&&(strcmp(newdir.str(),path.str())!=0))
            WARNLOG("CRemoteFile::rename passed full path '%s' that may not to match original directory '%s'",newname,path.str());
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCrename).append(filename).append(path);
        sendRemoteCommand(sendBuffer, replyBuffer);
        filename.set(path);
        remotefilename.clear();
    }

    void move(const char *newname)
    {
        // like rename except between directories
        // first create replote path
        if (!newname||!*newname)
            return;
        RemoteFilename destrfn;
        if (isPathSepChar(newname[0])&&isPathSepChar(newname[1])) {
            destrfn.setRemotePath(newname);
            if (!destrfn.queryEndpoint().ipequals(ep)) {
                StringBuffer msg;
                msg.appendf("IFile::move %s to %s, destination node must match source node", queryFilename(), newname);
                throw createDafsException(RFSERR_MoveFailed,msg.str());
            }
        }
        else
            destrfn.setPath(ep,newname);
        StringBuffer dest;
        newname = destrfn.getLocalPath(dest).str();
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        StringBuffer path;
        splitDirTail(filename,path);
        StringBuffer newdir;
        const char *newtail = splitDirTail(newname,newdir);
        if (strcmp(newdir.str(),path.str())==0)
        {
            path.append(newtail);
            newname = path;
            sendBuffer.append((RemoteFileCommandType)RFCrename);    // use rename if we can (supported on older dafilesrv)
        }
        else
            sendBuffer.append((RemoteFileCommandType)RFCmove);
        sendBuffer.append(filename).append(newname);
        sendRemoteCommand(sendBuffer, replyBuffer);
        filename.set(newname);
        remotefilename.clear();
    }

    void setReadOnly(bool set)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCsetreadonly).append(filename).append(set);
        sendRemoteCommand(sendBuffer, replyBuffer);
    }

    void setFilePermissions(unsigned fPerms)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCsetfileperms).append(filename).append(fPerms);
        try
        {
            sendRemoteCommand(sendBuffer, replyBuffer);
        }
        catch (IDAFS_Exception *e)
        {
            if (e->errorCode() == RFSERR_InvalidCommand)
            {
                WARNLOG("umask setFilePermissions (0%o) not supported on remote server", fPerms);
                e->Release();
            }
            else
                throw;
        }

    }

    offset_t size()
    {
#if 1 // faster method (consistant with IFile)
        // do this by using dir call (could be improved with new function but this not *too* bad)
        if (isSpecialPath(filename))
            return 0;   // queries deemed to always exist (though don't know size).
                        // if needed to get size I guess could use IFileIO method and cache (bit of pain though)
        StringBuffer dir;
        const char *tail = splitDirTail(filename,dir);
        if (!dir.length())
            return false;
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        bool includedirs = true;
        bool sub=false;
        {
            //Could be removed with new dafilesrv change [ (stream != 0) ], since this is not streaming.
            Owned<CEndpointCS> crit = dirCSTable->getCrit(ep); // NB dirCSTable doesn't own, last reference will remove from table
            CriticalBlock block(*crit);
            sendBuffer.append((RemoteFileCommandType)RFCgetdir).append(dir).append(tail).append(includedirs).append(sub);
            try
            {
                sendRemoteCommand(sendBuffer, replyBuffer);
            }
            catch (IDAFS_Exception * e)
            {
                if (e->errorCode() == RFSERR_GetDirFailed)
                {
                    e->Release();
                    return (offset_t)-1;
                }
                else
                    throw e;
            }
        }
        // now should be 0 or 1 files returned
        Owned<CRemoteDirectoryIterator> iter = new CRemoteDirectoryIterator(ep, dir.str());
        iter->appendBuf(replyBuffer);
        if (!iter->first())
            return (offset_t)-1;
        return (offset_t) iter->getFileSize();
#else
        IFileIO * io = open(IFOread);
        offset_t length = (offset_t)-1;
        if (io)
        {
            length = io->size();
            io->Release();
        }
        return length;
#endif
    }

    bool createDirectory()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCcreatedir).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        bool ok;
        replyBuffer.read(ok);
        return ok;
    }

    virtual IDirectoryIterator *directoryFiles(const char *mask,bool sub,bool includedirs)
    {
        if (mask&&!*mask)
            return createDirectoryIterator("",""); // NULL iterator

        CRemoteDirectoryIterator *ret = new CRemoteDirectoryIterator(ep, filename);
        byte stream = (sub || !mask || containsFileWildcard(mask)) ? 1 : 0; // no point in streaming if mask without wildcards or sub, as will only be <= 1 match.

        Owned<CEndpointCS> crit = dirCSTable->getCrit(ep); // NB dirCSTable doesn't own, last reference will remove from table
        CriticalBlock block(*crit);
        for (;;)
        {
            MemoryBuffer sendBuffer;
            initSendBuffer(sendBuffer);
            MemoryBuffer replyBuffer;
            sendBuffer.append((RemoteFileCommandType)RFCgetdir).append(filename).append(mask?mask:"").append(includedirs).append(sub).append(stream);
            sendRemoteCommand(sendBuffer, replyBuffer);
            if (ret->appendBuf(replyBuffer))
                break;
            stream = 2; // NB: will never get here if streaming was off (if stream==0 above)
        }
        return ret;
    }

    IDirectoryDifferenceIterator *monitorDirectory(
                                  IDirectoryIterator *prev=NULL,    // in (NULL means use current as baseline)
                                  const char *mask=NULL,
                                  bool sub=false,
                                  bool includedirs=false,
                                  unsigned checkinterval=60*1000,
                                  unsigned timeout=(unsigned)-1,
                                  Semaphore *abortsem=NULL) // returns NULL if timed out
    {
        // abortsem not yet supported
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCmonitordir).append(filename).append(mask?mask:"").append(includedirs).append(sub);
        sendBuffer.append(checkinterval).append(timeout);
        __int64 cancelid=0; // not yet used
        sendBuffer.append(cancelid);
        byte isprev=(prev!=NULL)?1:0;
        sendBuffer.append(isprev);
        if (prev)
            CRemoteDirectoryIterator::serialize(sendBuffer,prev,0,true);
        sendRemoteCommand(sendBuffer, replyBuffer);
        byte status;
        replyBuffer.read(status);
        if (status==1)
        {
            CRemoteDirectoryIterator *iter = new CRemoteDirectoryIterator(ep, filename);
            iter->appendBuf(replyBuffer);
            return iter;
        }
        return NULL;
    }

    bool getInfo(bool &isdir,offset_t &size,CDateTime &modtime)
    {
        // do this by using dir call (could be improved with new function but this not *too* bad)
        StringBuffer dir;
        const char *tail = splitDirTail(filename,dir);
        if (!dir.length())
            return false;
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        bool includedirs = true;
        bool sub=false;
        {
            //Could be removed with new dafilesrv change [ (stream != 0) ], since this is not streaming.
            Owned<CEndpointCS> crit = dirCSTable->getCrit(ep); // NB dirCSTable doesn't own, last reference will remove from table
            CriticalBlock block(*crit);
            sendBuffer.append((RemoteFileCommandType)RFCgetdir).append(dir).append(tail).append(includedirs).append(sub);
            sendRemoteCommand(sendBuffer, replyBuffer);
        }
        // now should be 0 or 1 files returned
        Owned<CRemoteDirectoryIterator> iter = new CRemoteDirectoryIterator(ep, dir.str());
        iter->appendBuf(replyBuffer);
        if (!iter->first())
            return false;
        isdir = iter->isDir();
        size = (offset_t) iter->getFileSize();
        iter->getModifiedTime(modtime);
        return true;
    }



    bool setCompression(bool set)
    {
        assertex(!"Need to implement compress()");
        return false;
    }

    offset_t compressedSize()
    {
        assertex(!"Need to implement actualSize()");
        return (offset_t)-1;
    }

    void serialize(MemoryBuffer &tgt)
    {
        throwUnexpected();
    }

    void deserialize(MemoryBuffer &src)
    {
        throwUnexpected();
    }

    unsigned getCRC()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCgetcrc).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer, true, true);

        unsigned crc;
        replyBuffer.read(crc);
        return crc;
    }

    void setCreateFlags(unsigned short cflags)
    {
        flags &= 0xffff;
        flags |= ((unsigned)cflags<<16);
    }

    unsigned short getCreateFlags()
    {
        return (unsigned short)(flags>>16);
    }

    void setShareMode(IFSHmode shmode)
    {
        flags &= ~(IFSHfull|IFSHread);
        flags |= (unsigned)(shmode&(IFSHfull|IFSHread));
        isShareSet = true;
    }

    unsigned short getShareMode()
    {
        return (unsigned short)(flags&0xffff);
    }

    bool getIsShareSet()
    {
        return isShareSet;
    }

    void remoteExtractBlobElements(const char * prefix, ExtractedBlobArray & extracted)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        sendBuffer.append((RemoteFileCommandType)RFCextractblobelements).append(prefix).append(filename);
        MemoryBuffer replyBuffer;
        sendRemoteCommand(sendBuffer, replyBuffer, true, true); // handles error code
        unsigned n;
        replyBuffer.read(n);
        for (unsigned i=0;i<n;i++) {
            ExtractedBlobInfo *item = new ExtractedBlobInfo;
            item->deserialize(replyBuffer);
            extracted.append(*item);
        }
    }

    bool copySectionAsync(const char *uuid,const RemoteFilename &dest, offset_t toOfs, offset_t fromOfs, offset_t size, ICopyFileProgress *progress, unsigned timeout)
    {
        // now if we get here is it can be assumed the source file is local to where we send the command
        StringBuffer tos;
        dest.getRemotePath(tos);
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCcopysection).append(uuid).append(queryLocalName()).append(tos).append(toOfs).append(fromOfs).append(size).append(timeout);
        sendRemoteCommand(sendBuffer, replyBuffer);
        unsigned status;
        replyBuffer.read(status);
        if (progress)
        {
            offset_t sizeDone;
            offset_t totalSize;
            replyBuffer.read(sizeDone).read(totalSize);
            progress->onProgress(sizeDone,totalSize);
        }
        return (AsyncCommandStatus)status!=ACScontinue; // should only otherwise be done as errors raised by exception
    }

    void copySection(const RemoteFilename &dest, offset_t toOfs, offset_t fromOfs, offset_t size, ICopyFileProgress *progress, CFflags copyFlags=CFnone)
    {
        StringBuffer uuid;
        genUUID(uuid,true);
        unsigned timeout = 60*1000; // check every minute
        while(!copySectionAsync(uuid.str(),dest,toOfs,fromOfs,size,progress,timeout));
    }

    void copyTo(IFile *dest, size32_t buffersize, ICopyFileProgress *progress, bool usetmp, CFflags copyFlags=CFnone);

    virtual IMemoryMappedFile *openMemoryMapped(offset_t ofs, memsize_t len, bool write)
    {
        return NULL;
    }
};

class CRemoteFileIO : public CInterfaceOf<IFileIO>
{
protected:
    Linked<CRemoteFile> parent;
    RemoteFileIOHandle  handle;
    std::atomic<cycle_t> ioReadCycles;
    std::atomic<cycle_t> ioWriteCycles;
    std::atomic<__uint64> ioReadBytes;
    std::atomic<__uint64> ioWriteBytes;
    std::atomic<__uint64> ioReads;
    std::atomic<__uint64> ioWrites;
    std::atomic<unsigned> ioRetries;
    IFOmode mode;
    compatIFSHmode compatmode;
    IFEflags extraFlags = IFEnone;
    bool disconnectonexit;
public:
    CRemoteFileIO(CRemoteFile *_parent)
        : parent(_parent), ioReadCycles(0), ioWriteCycles(0), ioReadBytes(0), ioWriteBytes(0), ioReads(0), ioWrites(0), ioRetries(0)
    {
        handle = 0;
        disconnectonexit = false;
    }

    ~CRemoteFileIO()
    {
        if (handle) {
            try {
                close();
            }
            catch (IException *e) {
                StringBuffer s;
                e->errorMessage(s);
                WARNLOG("CRemoteFileIO close file: %s",s.str());
                e->Release();
            }
        }
        if (disconnectonexit)
            parent->disconnect();
    }

    void close()
    {
        if (handle)
        {
            try
            {
                MemoryBuffer sendBuffer;
                initSendBuffer(sendBuffer);
                sendBuffer.append((RemoteFileCommandType)RFCcloseIO).append(handle);
                parent->sendRemoteCommand(sendBuffer,false);
            }
            catch (IDAFS_Exception *e)
            {
                if ((e->errorCode()!=RFSERR_InvalidFileIOHandle)&&(e->errorCode()!=RFSERR_NullFileIOHandle))
                    throw;
                e->Release();
            }
            handle = 0;
        }
    }
    RemoteFileIOHandle getHandle() const { return handle; }
    bool open(IFOmode _mode,compatIFSHmode _compatmode,IFEflags _extraFlags=IFEnone)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        const char *localname = parent->queryLocalName();
        localname = skipSpecialPath(localname);
        // also send _extraFlags
        // then also send sMode, cFlags
        unsigned short sMode = parent->getShareMode();
        unsigned short cFlags = parent->getCreateFlags();
        if (!(parent->getIsShareSet()))
        {
            switch ((compatIFSHmode)_compatmode)
            {
                case compatIFSHnone:
                    sMode = IFSHnone;
                    break;
                case compatIFSHread:
                    sMode = IFSHread;
                    break;
                case compatIFSHwrite:
                    sMode = IFSHfull;
                    break;
                case compatIFSHall:
                    sMode = IFSHfull;
                    break;
            }
        }
        sendBuffer.append((RemoteFileCommandType)RFCopenIO).append(localname).append((byte)_mode).append((byte)_compatmode).append((byte)_extraFlags).append(sMode).append(cFlags);
        parent->sendRemoteCommand(sendBuffer, replyBuffer);

        replyBuffer.read(handle);
        if (!handle)
            return false;
        switch (_mode) {
        case IFOcreate:
            mode = IFOwrite;
            break;
        case IFOcreaterw:
            mode = IFOreadwrite;
            break;
        default:
            mode = _mode;
            break;
        }
        compatmode = _compatmode;
        extraFlags = _extraFlags;
        return true;
    }

    bool reopen()
    {
        StringBuffer s;
        PROGLOG("Attempting reopen of %s on %s",parent->queryLocalName(),parent->queryEp().getEndpointHostText(s).str());
        if (open(mode,compatmode,extraFlags))
            return true;
        return false;

    }


    offset_t size()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCsize).append(handle);
        parent->sendRemoteCommand(sendBuffer, replyBuffer, false);
        // Retry using reopen TBD

        offset_t ret;
        replyBuffer.read(ret);
        return ret;
    }

    virtual unsigned __int64 getStatistic(StatisticKind kind)
    {
        switch (kind)
        {
        case StCycleDiskReadIOCycles:
            return ioReadCycles.load(std::memory_order_relaxed);
        case StCycleDiskWriteIOCycles:
            return ioWriteCycles.load(std::memory_order_relaxed);
        case StTimeDiskReadIO:
            return cycle_to_nanosec(ioReadCycles.load(std::memory_order_relaxed));
        case StTimeDiskWriteIO:
            return cycle_to_nanosec(ioWriteCycles.load(std::memory_order_relaxed));
        case StSizeDiskRead:
            return ioReadBytes.load(std::memory_order_relaxed);
        case StSizeDiskWrite:
            return ioWriteBytes.load(std::memory_order_relaxed);
        case StNumDiskReads:
            return ioReads.load(std::memory_order_relaxed);
        case StNumDiskWrites:
            return ioWrites.load(std::memory_order_relaxed);
        case StNumDiskRetries:
            return ioRetries.load(std::memory_order_relaxed);
        }
        return 0;
    }

    size32_t read(offset_t pos, size32_t len, void * data)
    {
        if (0 == len)
            return 0;
        dbgassertex(data);
        size32_t got;
        MemoryBuffer replyBuffer;
        CCycleTimer timer;
        const void *b;
        try
        {
            b = doRead(pos,len,replyBuffer,got,data);
        }
        catch (...)
        {
            ioReadCycles.fetch_add(timer.elapsedCycles());
            throw;
        }
        ioReadCycles.fetch_add(timer.elapsedCycles());
        ioReadBytes.fetch_add(got);
        ++ioReads;
        if (b!=data)
            memcpy(data,b,got);
        return got;
    }

    virtual void flush()
    {
    }

    const void *doRead(offset_t pos, size32_t len, MemoryBuffer &replyBuffer, size32_t &got, void *dstbuf)
    {
        unsigned tries=0;
        for (;;)
        {
            try
            {
                MemoryBuffer sendBuffer;
                initSendBuffer(sendBuffer);
                replyBuffer.clear();
                sendBuffer.append((RemoteFileCommandType)RFCread).append(handle).append(pos).append(len);
                parent->sendRemoteCommand(sendBuffer, replyBuffer,false);
                // kludge dafilesrv versions <= 1.5e don't return error correctly
                if (replyBuffer.length()>len+sizeof(size32_t)+sizeof(unsigned))
                {
                    size32_t save = replyBuffer.getPos();
                    replyBuffer.reset(len+sizeof(size32_t)+sizeof(unsigned));
                    unsigned errCode;
                    replyBuffer.read(errCode);
                    if (errCode)
                    {
                        StringBuffer msg;
                        parent->ep.getEndpointHostText(msg.append('[')).append("] ");
                        if (replyBuffer.getPos()<replyBuffer.length())
                        {
                            StringAttr s;
                            replyBuffer.read(s);
                            msg.append(s);
                        }
                        else
                            msg.append("ERROR #").append(errCode);
                        throw createDafsException(errCode, msg.str());
                    }
                    else
                        replyBuffer.reset(save);
                }
                replyBuffer.read(got);
                if ((got>replyBuffer.remaining())||(got>len))
                {
                    PROGLOG("Read beyond buffer %d,%d,%d",got,replyBuffer.remaining(),len);
                    throw createDafsException(RFSERR_ReadFailed, "Read beyond buffer");
                }
                return replyBuffer.readDirect(got);
            }
            catch (IJSOCK_Exception *e)
            {
                EXCLOG(e,"CRemoteFileIO::read");
                if (++tries > 3)
                {
                    ioRetries.fetch_add(tries);
                    throw;
                }
                WARNLOG("Retrying read of %s (%d)",parent->queryLocalName(),tries);
                Owned<IException> exc = e;
                if (!reopen())
                {
                    ioRetries.fetch_add(tries);
                    throw exc.getClear();
                }
            }
        }
        if (tries)
            ioRetries.fetch_add(tries);
        got = 0;
        return NULL;
    }


    size32_t write(offset_t pos, size32_t len, const void * data)
    {
        unsigned tries=0;
        size32_t ret = 0;
        CCycleTimer timer;
        for (;;)
        {
            try
            {
                MemoryBuffer replyBuffer;
                MemoryBuffer sendBuffer;
                initSendBuffer(sendBuffer);
                sendBuffer.append((RemoteFileCommandType)RFCwrite).append(handle).append(pos).append(len).append(len, data);
                parent->sendRemoteCommand(sendBuffer, replyBuffer, false, true);
                replyBuffer.read(ret);
                break;
            }
            catch (IJSOCK_Exception *e)
            {
                EXCLOG(e,"CRemoteFileIO::write");
                if (++tries > 3)
                {
                    ioRetries.fetch_add(tries);
                    ioWriteCycles.fetch_add(timer.elapsedCycles());
                    throw;
                }
                WARNLOG("Retrying write(%" I64F "d,%d) of %s (%d)",pos,len,parent->queryLocalName(),tries);
                Owned<IException> exc = e;
                if (!reopen())
                {
                    ioRetries.fetch_add(tries);
                    ioWriteCycles.fetch_add(timer.elapsedCycles());
                    throw exc.getClear();
                }
            }
        }

        if (tries)
            ioRetries.fetch_add(tries);

        ioWriteCycles.fetch_add(timer.elapsedCycles());
        ioWriteBytes.fetch_add(ret);
        ++ioWrites;
        if ((ret==(size32_t)-1) || (ret < len))
            throw createDafsException(DISK_FULL_EXCEPTION_CODE,"write failed, disk full?");
        return ret;
    }

    offset_t appendFile(IFile *file,offset_t pos,offset_t len)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        const char * fname = file->queryFilename();
        sendBuffer.append((RemoteFileCommandType)RFCappend).append(handle).append(fname).append(pos).append(len);
        parent->sendRemoteCommand(sendBuffer, replyBuffer, false, true); // retry not safe

        offset_t ret;
        replyBuffer.read(ret);

        if ((ret==(offset_t)-1) || ((len != ((offset_t)-1)) && (ret < len)))
            throw createDafsException(DISK_FULL_EXCEPTION_CODE,"append failed, disk full?");    // though could be file missing TBD
        return ret;
    }


    void setSize(offset_t size)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCsetsize).append(handle).append(size);
        parent->sendRemoteCommand(sendBuffer, replyBuffer, false, true);
        // retry using reopen TBD


    }

    void setDisconnectOnExit(bool set) { disconnectonexit = set; }

    void sendRemoteCommand(MemoryBuffer & sendBuffer, MemoryBuffer & replyBuffer, bool retry=true, bool lengthy=false, bool handleErrCode=true)
    {
        parent->sendRemoteCommand(sendBuffer, replyBuffer, retry, lengthy, handleErrCode);
    }
};


IFileIO *CRemoteFile::openShared(IFOmode mode,IFSHmode shmode,IFEflags extraFlags)
{
    // 0x0, 0x8, 0x10 and 0x20 are only share modes supported in this assert
    // currently only 0x0 (IFSHnone), 0x8 (IFSHread) and 0x10 (IFSHfull) are used so this could be 0xffffffe7
    // note: IFSHfull also includes read sharing (ie write|read)
    assertex(((unsigned)shmode&0xffffffc7)==0);
    compatIFSHmode compatmode;
    unsigned fileflags = (flags>>16) &  (S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IWGRP|S_IXGRP|S_IROTH|S_IWOTH|S_IXOTH);
    if (fileflags&S_IXUSR)                      // this is bit hit and miss but backward compatible
        compatmode = compatIFSHexec;
    else if (fileflags&(S_IWGRP|S_IWOTH))
        compatmode = compatIFSHall;
    else if (shmode&IFSHfull)
        compatmode = compatIFSHwrite;
    else if (((shmode&(IFSHread|IFSHfull))==0) && ((fileflags&(S_IRGRP|S_IROTH))==0))
        compatmode = compatIFSHnone;
    else
        compatmode = compatIFSHread;
    Owned<CRemoteFileIO> res = new CRemoteFileIO(this);
    if (res->open(mode,compatmode,extraFlags))
        return res.getClear();
    return NULL;
}

IFileIO * CRemoteFile::open(IFOmode mode,IFEflags extraFlags)
{
    return openShared(mode,(IFSHmode)(flags&(IFSHread|IFSHfull)),extraFlags);
}

//---------------------------------------------------------------------------

void CRemoteFile::copyTo(IFile *dest, size32_t buffersize, ICopyFileProgress *progress, bool usetmp, CFflags copyFlags)
{
    CRemoteFile *dstfile = QUERYINTERFACE(dest,CRemoteFile);
    if (dstfile&&(!dstfile->queryEp().isLocal() || (dstfile->queryEp().port!=DAFILESRV_PORT && dstfile->queryEp().port!=SECURE_DAFILESRV_PORT))) {
        StringBuffer tmpname;
        Owned<IFile> destf;
        RemoteFilename dest;
        if (usetmp) {
            makeTempCopyName(tmpname,dstfile->queryLocalName());
            dest.setPath(dstfile->queryEp(),tmpname.str());
        }
        else
            dest.setPath(dstfile->queryEp(),dstfile->queryLocalName());
        destf.setown(createIFile(dest));
        try {
            // following may fail if new dafilesrv not deployed on src
            copySection(dest,(offset_t)-1,0,(offset_t)-1,progress,copyFlags);
            if (usetmp) {
                StringAttr tail(pathTail(dstfile->queryLocalName()));
                dstfile->remove();
                destf->rename(tail);
            }
            return;
        }
        catch (IException *e)
        {
            StringBuffer s;
            s.appendf("Remote File Copy (%d): ",e->errorCode());
            e->errorMessage(s);
            s.append(", retrying local");
            WARNLOG("%s",s.str());
            e->Release();
        }
        // delete dest
        try {
            destf->remove();
        }
        catch (IException *e)
        {
            EXCLOG(e,"Remote File Copy, Deleting temporary file");
            e->Release();
        }
    }
    // assumption if we get here that source remote, dest local (or equiv)
    class cIntercept: implements ICopyFileIntercept
    {
        MemoryAttr ma;
        MemoryBuffer mb;
        virtual offset_t copy(IFileIO *from, IFileIO *to, offset_t ofs, size32_t sz)
        {
            if (ma.length()<sz)
                ma.allocate(sz);    // may be not used
            void *buf = ma.bufferBase();
            size32_t got;
            CRemoteFileIO *srcio = QUERYINTERFACE(from,CRemoteFileIO);
            const void *dst;
            if (srcio)
                dst = srcio->doRead(ofs,sz,mb.clear(),got,buf);
            else {
                // shouldn't ever get here if source remote
                got = from->read(ofs, sz, buf);
                dst = buf;
            }
            if (got != 0)
                to->write(ofs, got, dst);
            return got;
        }
    } intercept;
    doCopyFile(dest,this,buffersize,progress,&intercept,usetmp,copyFlags);
}

////////////////



void remoteExtractBlobElements(const SocketEndpoint &ep,const char * prefix, const char * filename, ExtractedBlobArray & extracted)
{
    Owned<CRemoteFile> file = new CRemoteFile (ep,filename);
    file->remoteExtractBlobElements(prefix, extracted);
}




//---------------------------------------------------------------------------
// Local mount redirect

struct CLocalMountRec: public CInterface
{
    IpAddress ip;
    StringAttr dir;             // dir path on remote ip
    StringAttr local;           // local dir path
};

static CIArrayOf<CLocalMountRec> localMounts;
static CriticalSection           localMountCrit;

void setDafsLocalMountRedirect(const IpAddress &ip,const char *dir,const char *mountdir)
{
    CriticalBlock block(localMountCrit);
    ForEachItemInRev(i,localMounts) {
        CLocalMountRec &mount = localMounts.item(i);
        if (dir==NULL) { // remove all matching mount
            if (!mountdir)
                return;
            if (strcmp(mount.local,mountdir)==0)
                localMounts.remove(i);
        }
        else if (mount.ip.ipequals(ip)&&(strcmp(mount.dir,dir)==0)) {
            if (mountdir) {
                mount.local.set(mountdir);
                return;
            }
            else
                localMounts.remove(i);
        }
    }
    if (dir&&mountdir) {
        CLocalMountRec &mount = *new CLocalMountRec;
        mount.ip.ipset(ip);
        mount.dir.set(dir);
        mount.local.set(mountdir);
        localMounts.append(mount);
    }
}

IFile *createFileLocalMount(const IpAddress &ip, const char * filename)
{
    CriticalBlock block(localMountCrit);
    ForEachItemInRev(i,localMounts) {
        CLocalMountRec &mount = localMounts.item(i);
        if (mount.ip.ipequals(ip)) {
            size32_t bl = mount.dir.length();
            if (isPathSepChar(mount.dir[bl-1]))
                bl--;
            if ((memcmp((void *)filename,(void *)mount.dir.get(),bl)==0)&&(isPathSepChar(filename[bl])||!filename[bl])) { // match
                StringBuffer locpath(mount.local);
                if (filename[bl])
                    addPathSepChar(locpath).append(filename+bl+1);
                locpath.replace((PATHSEPCHAR=='\\')?'/':'\\',PATHSEPCHAR);
                return createIFile(locpath.str());
            }
        }
    }
    return NULL;
}

IFile * createRemoteFile(SocketEndpoint &ep, const char * filename)
{
    IFile *ret = createFileLocalMount(ep,filename);
    if (ret)
        return ret;
    return new CRemoteFile(ep, filename);
}


void clientDisconnectRemoteFile(IFile *file)
{
    CRemoteFile *cfile = QUERYINTERFACE(file,CRemoteFile);
    if (cfile)
        cfile->disconnect();
}

bool clientResetFilename(IFile *file, const char *newname) // returns false if not remote
{
    CRemoteFile *cfile = QUERYINTERFACE(file,CRemoteFile);
    if (!cfile)
        return false;
    cfile->resetLocalFilename(newname);
    return true;
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

IFile *createDaliServixFile(const RemoteFilename & file, const char *storageSecret)
{
    StringBuffer path;
    file.getLocalPath(path);
    return new CRemoteFile(file.queryEndpoint(), path.str(), storageSecret);
}

void clientDisconnectRemoteIoOnExit(IFileIO *fileio, bool set)
{
    CRemoteFileIO *cfileio = QUERYINTERFACE(fileio,CRemoteFileIO);
    if (cfileio)
        cfileio->setDisconnectOnExit(set);
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


extern bool clientAsyncCopyFileSection(const char *uuid,
                        IFile *from,                        // expected to be remote
                        RemoteFilename &to,
                        offset_t toOfs,                     // -1 created file and copies to start
                        offset_t fromOfs,
                        offset_t size,
                        ICopyFileProgress *progress,
                        unsigned timeout,
                        CFflags copyFlags)       // returns true when done
{
    CRemoteFile *cfile = QUERYINTERFACE(from,CRemoteFile);
    if (!cfile || to.isLocal()) {
        //local - ensure that the file copy is run locally rather than remote
        Owned<IFile> dest = createIFile(to);
        copyFileSection(from,dest,toOfs,fromOfs,size,progress,copyFlags);
        return true;
    }
    return cfile->copySectionAsync(uuid,to,toOfs,fromOfs, size, progress, timeout);

}

bool asyncCopyFileSection(const char *uuid,                 // from genUUID - must be same for subsequent calls
                            IFile *from,                        // expected to be remote
                            RemoteFilename &to,
                            offset_t toofs,                     // (offset_t)-1 created file and copies to start
                            offset_t fromofs,
                            offset_t size,                      // (offset_t)-1 for all file
                            ICopyFileProgress *progress,
                            unsigned timeout,                   // 0 to start, non-zero to wait
                            CFflags copyFlags
                        )
{
    return  clientAsyncCopyFileSection(uuid,from,to,toofs,fromofs,size,progress,timeout,copyFlags);
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
                const char *v = DAFILESRV_VERSIONSTRING;
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
                        StringBuffer wanted(v-DAFILESRV_VERSIONSTRING,DAFILESRV_VERSIONSTRING);
                        ver.setLength(rv-ver.str());
                        if (strcmp(ver.str(),wanted.str())<0) { // allow >
                            code = DAFS_VALIDATE_BAD_VERSION;
                            errstr.appendf("Mismatch dafilesrv version ");
                            errstr.append(rv-ver.str(),ver.str());
                            errstr.append(", wanted ");
                            errstr.append(v-DAFILESRV_VERSIONSTRING,DAFILESRV_VERSIONSTRING);
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
                        StringBuffer fullPath;
                        rfn.getRemotePath(fullPath);
                        recursiveCreateDirectoryForFile(fullPath);
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
                            ep.getHostText(s);
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

extern DAFSCLIENT_API void installFileHooks(const char *hookFileSpec)
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

void installDefaultFileHooks(IPropertyTree * config)
{
    StringBuffer hookdir;
    if (!config || !config->getProp("@fileHooks", hookdir))
    {
        getPackageFolder(hookdir);
        addPathSepChar(hookdir).append("filehooks");
        if (!checkFileExists(hookdir))
            return;
    }
    installFileHooks(hookdir);
}

typedef void (*HookInstallFunction)();

static void installFileHook(const char *hookFile)
{
    StringBuffer dirPath, dirTail, absolutePath;
    splitFilename(hookFile, &dirPath, &dirPath, &dirTail, &dirTail);
    makeAbsolutePath(dirPath.str(), absolutePath);
    if (!containsFileWildcard(dirTail))
    {
        addPathSepChar(absolutePath).append(dirTail);
        Owned<IFile> file = createIFile(absolutePath);
        if (file->isDirectory() == fileBool::foundYes)
        {
            installFileHooks(addPathSepChar(absolutePath).append('*'));
        }
        else if (file->isFile() == fileBool::foundYes)
        {
            HookInstallFunction hookInstall;
            SharedObject *so = new SharedObject(); // MORE - this leaks! Kind-of deliberate right now...
            if (so->load(file->queryFilename(), false) &&
                (hookInstall = (HookInstallFunction) GetSharedProcedure(so->getInstanceHandle(), "installFileHook")) != NULL)
            {
                hookInstall();
                hookDlls->append(so);
            }
            else
            {
                so->unload();
                delete so;
                DBGLOG("File hook library %s could not be loaded", hookFile);
            }
        }
        else
        {
            DBGLOG("File hook library %s not found", hookFile);
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

extern DAFSCLIENT_API void removeFileHooks()
{
    if (hookDlls)
    {
        ForEachItemIn(idx, *hookDlls)
        {
            SharedObject *so = hookDlls->item(idx);
            HookInstallFunction hookInstall = (HookInstallFunction) GetSharedProcedure(so->getInstanceHandle(), "removeFileHook");
            if (hookInstall)
                hookInstall();
            delete so;
        }
        delete hookDlls;
        hookDlls = NULL;
    }
}

MODULE_INIT(INIT_PRIORITY_DAFSCLIENT)
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
#ifndef _CONTAINERIZED
    const char *forceRemotePattern = queryEnvironmentConf().queryProp("forceRemotePattern");
    if (!isEmptyString(forceRemotePattern))
        queryDaFileSrvHook()->forceRemote(forceRemotePattern);
#endif
}

bool testForceRemote(const char *path)
{
#ifndef _CONTAINERIZED
    const char *forceRemotePattern = queryEnvironmentConf().queryProp("forceRemotePattern");
    return !isEmptyString(forceRemotePattern) && WildMatch(path, forceRemotePattern, false);
#else
    return false;
#endif
}


//// legacy implementation of streaming implementations, to be replaced by dafsstream.*
///
//

class CRemoteFilteredFileIOBase : public CRemoteBase, implements IRemoteFileIO
{
    typedef CRemoteBase PARENT;
public:
    IMPLEMENT_IINTERFACE_O_USING(CRemoteBase);

    // Really a stream, but life (maybe) easier elsewhere if looks like a file
    // Sometime should refactor to be based on IBufferedSerialInputStream instead - or maybe IRowStream.
    CRemoteFilteredFileIOBase(SocketEndpoint &ep, const char *filename, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, unsigned __int64 chooseN)
        : CRemoteBase(ep, filename)
    {
        // populate secret if there is one
        RemoteFilename rfn;
        rfn.setPath(ep, filename);
        queryDaFileSrvHook()->getSecretBased(storageSecret, rfn);

        // NB: inputGrouped == outputGrouped for now, but may want output to be ungrouped

        openRequest();
        if (queryOutputCompressionDefault())
        {
            expander.setown(getExpander(queryOutputCompressionDefault()));
            if (expander)
            {
                expandMb.setEndian(__BIG_ENDIAN);
                request.appendf("\"outputCompression\" : \"%s\",\n", queryOutputCompressionDefault());
            }
            else
                WARNLOG("Failed to created compression decompressor for: %s", queryOutputCompressionDefault());
        }

        request.appendf("\"format\" : \"binary\",\n"
            "\"node\" : {\n"
            " \"fileName\" : \"%s\"", filename);
        if (chooseN)
            request.appendf(",\n \"chooseN\" : \"%" I64F "u\"", chooseN);
        if (fieldFilters.numFilterFields())
        {
            request.append(",\n \"keyFilter\" : [\n  ");
            for (unsigned idx=0; idx < fieldFilters.numFilterFields(); idx++)
            {
                auto &filter = fieldFilters.queryFilter(idx);
                StringBuffer filterString;
                filter.serialize(filterString);
                if (idx)
                    request.append(",\n  ");
                request.append("\"");
                encodeJSON(request, filterString.length(), filterString.str());
                request.append("\"");
            }
            request.append("\n ]");
        }
        MemoryBuffer actualTypeInfo;
        if (!dumpTypeInfo(actualTypeInfo, actual->querySerializedDiskMeta()->queryTypeInfo()))
            throw createDafsException(DAFSERR_cmdstream_unsupported_recfmt, "Format not supported by remote read");
        request.append(",\n \"inputBin\" : \"");
        JBASE64_Encode(actualTypeInfo.toByteArray(), actualTypeInfo.length(), request, false);
        request.append("\"");
        if (actual != projected)
        {
            MemoryBuffer projectedTypeInfo;
            dumpTypeInfo(projectedTypeInfo, projected->querySerializedDiskMeta()->queryTypeInfo());
            if (actualTypeInfo.length() != projectedTypeInfo.length() ||
                memcmp(actualTypeInfo.toByteArray(), projectedTypeInfo.toByteArray(), actualTypeInfo.length()))
            {
                request.append(",\n \"outputBin\": \"");
                JBASE64_Encode(projectedTypeInfo.toByteArray(), projectedTypeInfo.length(), request, false);
                request.append("\"");
            }
        }
        bufPos = 0;
    }
    virtual size32_t read(offset_t pos, size32_t len, void * data) override
    {
        assertex(pos == bufPos);  // Must read sequentially
        if (!bufRemaining && !eof)
            refill();
        if (eof)
            return 0;
        if (len > bufRemaining)
            len = bufRemaining;
        bufPos += len;
        bufRemaining -= len;
        memcpy(data, reply.readDirect(len), len);
        return len;
    }
    virtual offset_t size() override { return -1; }
    virtual size32_t write(offset_t pos, size32_t len, const void * data) override { throwUnexpected(); }
    virtual offset_t appendFile(IFile *file,offset_t pos=0,offset_t len=(offset_t)-1) override { throwUnexpected(); }
    virtual void setSize(offset_t size) override { throwUnexpected(); }
    virtual void flush() override { throwUnexpected(); }
    virtual void close() override
    {
        PARENT::close(handle);
        handle = 0;
    }
    virtual unsigned __int64 getStatistic(StatisticKind kind) override
    {
        /* NB: Would need new stat. categories added for this to make sense,
         * but this class is implemented as a IFileIO for convenience for now,
         * it may be refactored into another form later.
         */
        return 0;
    }
// IRemoteFileIO
    virtual void addVirtualFieldMapping(const char *fieldName, const char *fieldValue) override
    {
        virtualFields[fieldName] = fieldValue;
    }
    virtual void ensureAvailable() override
    {
        if (firstRequest)
            handleFirstRequest();
    }
protected:
    StringBuffer &openRequest()
    {
        return request.append("{\n");
    }
    StringBuffer &closeRequest()
    {
        return request.append("\n }\n");
    }
    void addVirtualFields()
    {
        request.append(", \n \"virtualFields\" : {\n");
        bool first=true;
        for (auto &e : virtualFields)
        {
            if (!first)
                request.append(",\n");
            request.appendf("  \"%s\" : \"%s\"", e.first.c_str(), e.second.c_str());
            first = false;
        }
        request.append(" }");
    }
    void handleFirstRequest()
    {
        firstRequest = false;
        addVirtualFields();
        closeRequest();
        sendRequest(0, nullptr);
    }
    void refill()
    {
        if (firstRequest)
        {
            handleFirstRequest();
            return;
        }
        size32_t cursorLength;
        reply.read(cursorLength);
        if (!cursorLength)
        {
            eof = true;
            return;
        }
        MemoryBuffer mrequest;
        MemoryBuffer newReply;
        initSendBuffer(mrequest);
        mrequest.append((RemoteFileCommandType)RFCStreamRead);
        VStringBuffer json("{ \"handle\" : %u, \"format\" : \"binary\" }", handle);
        mrequest.append(json.length(), json.str());
        unsigned newHandle = 0;
        try
        {
            sendRemoteCommand(mrequest, newReply, false);
            newReply.read(newHandle);
        }
        catch (IJSOCK_Exception *e)
        {
            // will trigger new request with cursor
            EXCLOG(e, "CRemoteFilteredFileIOBase:: socket failure whilst streaming, will attempt to reconnect with cursor");
            newHandle = 0;
            e->Release();
        }
        if (newHandle == handle)
        {
            reply.swapWith(newReply);
            reply.read(bufRemaining);
            eof = (bufRemaining == 0);
            if (expander)
            {
                size32_t expandedSz = expander->init(reply.bytes()+reply.getPos());
                expandMb.clear().reserve(expandedSz);
                expander->expand(expandMb.bufferBase());
                expandMb.swapWith(reply);
            }
        }
        else
        {
            assertex(newHandle == 0);
            sendRequest(cursorLength, reply.readDirect(cursorLength));
        }
    }
    void sendRequest(unsigned cursorLen, const void *cursorData)
    {
        MemoryBuffer mrequest;
        initSendBuffer(mrequest);
        mrequest.append((RemoteFileCommandType)RFCStreamRead);
        mrequest.append(request.length(), request.str());
        if (cursorLen)
        {
            StringBuffer cursorInfo;
            cursorInfo.append(",\"cursorBin\": \"");
            JBASE64_Encode(cursorData, cursorLen, cursorInfo, false);
            cursorInfo.append("\"\n");
            mrequest.append(cursorInfo.length(), cursorInfo.str());
        }
        if (TF_TRACE_FULL)
            PROGLOG("req = <%s}>", request.str());
        mrequest.append(3, " \n}");
        sendRemoteCommand(mrequest, reply);
        reply.read(handle);
        reply.read(bufRemaining);
        eof = (bufRemaining == 0);
        if (expander)
        {
            size32_t expandedSz = expander->init(reply.bytes()+reply.getPos());
            expandMb.clear().reserve(expandedSz);
            expander->expand(expandMb.bufferBase());
            expandMb.swapWith(reply);
        }
    }
    StringBuffer request;
    MemoryBuffer reply;
    unsigned handle = 0;
    size32_t bufRemaining = 0;
    offset_t bufPos = 0;
    bool eof = false;

    bool firstRequest = true;
    std::unordered_map<std::string, std::string> virtualFields;
    Owned<IExpander> expander;
    MemoryBuffer expandMb;
};

class CRemoteFilteredFileIO : public CRemoteFilteredFileIOBase
{
public:
    // Really a stream, but life (maybe) easier elsewhere if looks like a file
    // Sometime should refactor to be based on IBufferedSerialInputStream instead - or maybe IRowStream.
    CRemoteFilteredFileIO(SocketEndpoint &ep, const char *filename, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, bool compressed, bool grouped, unsigned __int64 chooseN)
        : CRemoteFilteredFileIOBase(ep, filename, actual, projected, fieldFilters, chooseN)
    {
        // NB: inputGrouped == outputGrouped for now, but may want output to be ungrouped
        request.appendf(",\n \"kind\" : \"diskread\",\n"
            " \"compressed\" : \"%s\",\n"
            " \"inputGrouped\" : \"%s\",\n"
            " \"outputGrouped\" : \"%s\"", boolToStr(compressed), boolToStr(grouped), boolToStr(grouped));
    }
};

class CRemoteFilteredRowStream : public CRemoteFilteredFileIO, implements IRowStream
{
public:
    CRemoteFilteredRowStream(const RtlRecord &_recInfo, SocketEndpoint &ep, const char * filename, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, bool compressed, bool grouped)
        : CRemoteFilteredFileIO(ep, filename, actual, projected, fieldFilters, compressed, grouped, 0), recInfo(_recInfo)
    {
    }
    virtual const byte *queryNextRow()  // NOTE - rows returned must NOT be freed
    {
        if (!bufRemaining && !eof)
            refill();
        if (eof)
            return nullptr;
        unsigned len = recInfo.getRecordSize(reply.readDirect(0));
        bufPos += len;
        bufRemaining -= len;
        return reply.readDirect(len);
    }
    virtual void stop() override
    {
        close();
        eof = true;
    }
protected:
    const RtlRecord &recInfo;
};

static StringAttr remoteOutputCompressionDefault;
void setRemoteOutputCompressionDefault(const char *type)
{
    if (!isEmptyString(type))
        remoteOutputCompressionDefault.set(type);
}
const char *queryOutputCompressionDefault() { return remoteOutputCompressionDefault; }

extern IRemoteFileIO *createRemoteFilteredFile(SocketEndpoint &ep, const char * filename, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, bool compressed, bool grouped, unsigned __int64 chooseN)
{
    try
    {
        return new CRemoteFilteredFileIO(ep, filename, actual, projected, fieldFilters, compressed, grouped, chooseN);
    }
    catch (IException *e)
    {
        EXCLOG(e, nullptr);
        e->Release();
    }
    return nullptr;
}

class CRemoteFilteredKeyIO : public CRemoteFilteredFileIOBase
{
public:
    // Really a stream, but life (maybe) easier elsewhere if looks like a file
    // Sometime should refactor to be based on IBufferedSerialInputStream instead - or maybe IRowStream.
    CRemoteFilteredKeyIO(SocketEndpoint &ep, const char *filename, unsigned crc, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, unsigned __int64 chooseN)
        : CRemoteFilteredFileIOBase(ep, filename, actual, projected, fieldFilters, chooseN)
    {
        request.appendf(",\n \"kind\" : \"indexread\"");
        request.appendf(",\n \"crc\" : \"%u\"", crc);
    }
};

class CRemoteFilteredKeyCountIO : public CRemoteFilteredFileIOBase
{
public:
    // Really a stream, but life (maybe) easier elsewhere if looks like a file
    // Sometime should refactor to be based on IBufferedSerialInputStream instead - or maybe IRowStream.
    CRemoteFilteredKeyCountIO(SocketEndpoint &ep, const char *filename, unsigned crc, IOutputMetaData *actual, const RowFilter &fieldFilters, unsigned __int64 rowLimit)
        : CRemoteFilteredFileIOBase(ep, filename, actual, actual, fieldFilters, rowLimit)
    {
        request.appendf(",\n \"kind\" : \"indexcount\"");
        request.appendf(",\n \"crc\" : \"%u\"", crc);
    }
};

class CRemoteKey : public CSimpleInterfaceOf<IIndexLookup>
{
    Owned<IRemoteFileIO> iRemoteFileIO;
    offset_t pos = 0;
    Owned<ISourceRowPrefetcher> prefetcher;
    CThorContiguousRowBuffer prefetchBuffer;
    Owned<IBufferedSerialInputStream> strm;
    bool pending = false;
    SocketEndpoint ep;
    StringAttr filename;
    unsigned crc;
    Linked<IOutputMetaData> actual, projected;
    RowFilter fieldFilters;

public:
    CRemoteKey(SocketEndpoint &_ep, const char *_filename, unsigned _crc, IOutputMetaData *_actual, IOutputMetaData *_projected, const RowFilter &_fieldFilters, unsigned __int64 rowLimit)
        : ep(_ep), filename(_filename), crc(_crc), actual(_actual), projected(_projected)
    {
        for (unsigned f=0; f<_fieldFilters.numFilterFields(); f++)
            fieldFilters.addFilter(OLINK(_fieldFilters.queryFilter(f)));
        iRemoteFileIO.setown(new CRemoteFilteredKeyIO(ep, filename, crc, actual, projected, fieldFilters, rowLimit));
        if (!iRemoteFileIO)
            throwStringExceptionV(DAFSERR_cmdstream_openfailure, "Unable to open remote key part: '%s'", filename.get());
        strm.setown(createFileSerialStream(iRemoteFileIO));
        prefetcher.setown(projected->createDiskPrefetcher());
        assertex(prefetcher);
        prefetchBuffer.setStream(strm);
    }
// IIndexLookup
    virtual void ensureAvailable() override
    {
        iRemoteFileIO->ensureAvailable(); // will throw an exception if fails
    }
    virtual unsigned __int64 getCount() override
    {
        return checkCount(0);
    }
    virtual unsigned __int64 checkCount(unsigned __int64 limit) override
    {
        Owned<IFileIO> iFileIO = new CRemoteFilteredKeyCountIO(ep, filename, crc, actual, fieldFilters, limit);
        unsigned __int64 result;
        iFileIO->read(0, sizeof(result), &result);
        return result;
    }
    virtual const void *nextKey() override
    {
        if (pending)
            prefetchBuffer.finishedRow();
        if (prefetchBuffer.eos())
            return nullptr;
        prefetcher->readAhead(prefetchBuffer);
        pending = true;
        return prefetchBuffer.queryRow();
    }
};


extern IIndexLookup *createRemoteFilteredKey(SocketEndpoint &ep, const char * filename, unsigned crc, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, unsigned __int64 chooseN)
{
    try
    {
        return new CRemoteKey(ep, filename, crc, actual, projected, fieldFilters, chooseN);
    }
    catch (IException *e)
    {
        EXCLOG(e, nullptr);
        e->Release();
    }
    return nullptr;
}

