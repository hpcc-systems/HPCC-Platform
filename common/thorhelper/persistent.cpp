/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#include "persistent.hpp"
#include "jthread.hpp"
#include "jdebug.hpp"
#include "jlog.hpp"
#include <memory>
#include <unordered_set>
#ifdef _WIN32
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#define PERSILOG(loglevel, ...) if(static_cast<int>(loglevel) <= static_cast<int>(m_loglevel)) DBGLOG(__VA_ARGS__)
#define MAX_NO_CLEANUP_SOCKETSETS 1000

static inline StringBuffer& addKeySuffix(PersistentProtocol proto, StringBuffer& keystr)
{
    switch (proto)
    {
        case PersistentProtocol::ProtoTCP:
            break;
        case PersistentProtocol::ProtoTLS:
            keystr.append('~');
            break;
        default:
            throw makeStringException(-1, "New suffix should be defined");
    }
    return keystr;
}

class CPersistentInfo : implements IInterface, public CInterface
{
    friend class CPersistentHandler;
    friend class CAvailKeeper;
public:
    IMPLEMENT_IINTERFACE;
    CPersistentInfo(bool _inUse, unsigned _timeUsed, unsigned _useCount, SocketEndpoint* _ep, PersistentProtocol _proto, ISocket* _sock)
        : inUse(_inUse), timeUsed(_timeUsed), useCount(_useCount), ep(_ep?(new SocketEndpoint(*_ep)):nullptr), proto(_proto), sock(_sock)
    {
        if(_ep)
        {
            _ep->getEndpointHostText(epstr);
            keystr.set(epstr);
            addKeySuffix(proto, keystr);
        }
    }
protected:
    bool inUse;
    unsigned timeUsed;
    unsigned useCount;
    std::unique_ptr<SocketEndpoint> ep;
    StringBuffer epstr;
    StringBuffer keystr;
    PersistentProtocol proto;
    Linked<ISocket> sock;
};

struct LinkedPersistentInfoHash
{
    size_t operator()(const Linked<CPersistentInfo>& linkedinfo) const
    {
        return std::hash<CPersistentInfo*>()(linkedinfo);
    }
};

using SocketSet = std::unordered_set<Linked<CPersistentInfo>, LinkedPersistentInfoHash>;
using EpSocketSetMap = MapStringTo<OwnedPtr<SocketSet>, SocketSet*>;
using EpSocketSetMapping = MappingStringTo<OwnedPtr<SocketSet>, SocketSet*>;

class CAvailKeeper
{
private:
    SocketSet m_avail;
    EpSocketSetMap m_avail4ep;
public:
    void add(CPersistentInfo* sockinfo)
    {
        findSet(sockinfo, true)->insert(sockinfo);
        if (m_avail4ep.count() > MAX_NO_CLEANUP_SOCKETSETS)
            cleanup();
    }
    void remove(CPersistentInfo* sockinfo)
    {
        SocketSet* sset = findSet(sockinfo);
        if (sset)
            sset->erase(sockinfo);
    }
    CPersistentInfo* get(SocketEndpoint* ep, PersistentProtocol proto)
    {
        SocketSet* sset = findSet(ep, proto);
        if (sset)
        {
            //The first available socket will suffice
            auto iter = sset->begin();
            if (iter != sset->end())
            {
                Linked<CPersistentInfo> info = *iter;
                sset->erase(iter);
                return info.getClear();
            }
        }
        return nullptr;
    }
private:
    inline StringBuffer& calcKey(SocketEndpoint& ep, PersistentProtocol proto, StringBuffer& keystr)
    {
        ep.getEndpointHostText(keystr);
        return addKeySuffix(proto, keystr);
    }
    SocketSet* findSet(CPersistentInfo* info, bool create = false)
    {
        if (!info->ep.get())
            return &m_avail;
        return findSet(info->keystr.str(), create);
    }
    SocketSet* findSet(SocketEndpoint* ep, PersistentProtocol proto, bool create = false)
    {
        if (!ep)
            return &m_avail;
        StringBuffer keystr;
        calcKey(*ep, proto, keystr);
        return findSet(keystr.str(), create);
    }
    SocketSet* findSet(const char* key, bool create = false)
    {
        auto ptrptr = m_avail4ep.getValue(key);
        if (ptrptr)
            return *ptrptr;
        else if (create)
        {
            SocketSet* sset = new SocketSet();
            m_avail4ep.setValue(key, sset);
            return sset;
        }
        return nullptr;
    }
    void cleanup()
    {
        std::vector<EpSocketSetMapping*> elems;
        for (auto& e : m_avail4ep)
        {
            if (e.getValue()->empty())
                elems.push_back(&e);
        }
        for (auto& e : elems)
            m_avail4ep.removeExact(e);
    }
};

using SockInfoMap = MapBetween<Linked<ISocket>, ISocket*, Owned<CPersistentInfo>, CPersistentInfo*>;
using StringIntMap = MapStringTo<int, int>;

// Important data structures for the implementation:
// m_selecHandler: used to detect incoming data on a socket or socket closure from the other end
// m_infomap: keep track of the status of a socket, there's one entry for each reusable socket, no matter if the socket is being used or idle. The main purpose is to
//   keep track of the status and life span of the socket so that it can be recycled properly.
// m_availkeeper: keep track of available sockets that can be assigned for reusing. It's a map between <endpoint, protocal> and the set of available sockets. The
//   main purpose is to speed up finding an available socket
class CPersistentHandler : implements IPersistentHandler, implements ISocketSelectNotify, public Thread
{
private:
    static const int MAX_INFLIGHT_TIME = 1800;
    int m_maxIdleTime = DEFAULT_MAX_PERSISTENT_IDLE_TIME;
    int m_maxReqs = DEFAULT_MAX_PERSISTENT_REQUESTS;
    Owned<ISocketSelectHandler> m_selectHandler;
    IPersistentSelectNotify* m_notify;
    Semaphore m_waitsem;
    bool m_stop = false;
    SockInfoMap m_infomap;
    CAvailKeeper m_availkeeper;
    CriticalSection m_critsect;
    PersistentLogLevel m_loglevel = PersistentLogLevel::PLogNormal;
    static int CurID;
    int m_id = 0;
    bool m_enableDoNotReuseList = false;
    StringIntMap m_instantCloseCounts;
    StringIntMap m_doNotReuseList;
public:
    IMPLEMENT_IINTERFACE_USING(Thread);
    CPersistentHandler(IPersistentSelectNotify* notify, int maxIdleTime, int maxReqs, PersistentLogLevel loglevel, bool enableDoNotReuseList)
                        : m_maxIdleTime(maxIdleTime), m_maxReqs(maxReqs), m_notify(notify), m_stop(false), m_loglevel(loglevel), m_enableDoNotReuseList(enableDoNotReuseList)
    {
        m_id = ++CurID;
        m_selectHandler.setown(createSocketSelectHandler());
    }

    virtual ~CPersistentHandler()
    {
    }

    virtual void add(ISocket* sock, SocketEndpoint* ep, PersistentProtocol proto) override
    {
        if (!sock || !sock->isValid())
            return;
        PERSILOG(PersistentLogLevel::PLogMax, "PERSISTENT: adding socket %d to handler %d", sock->OShandle(), m_id);
        CriticalBlock block(m_critsect);
        if (m_enableDoNotReuseList && ep != nullptr)
        {
            StringBuffer epstr;
            ep->getEndpointHostText(epstr);
            if(m_doNotReuseList.getValue(epstr.str()) != nullptr)
            {
                PERSILOG(PersistentLogLevel::PLogNormal, "PERSISTENT: socket %d's target endpoint %s is in DoNotReuseList, will not add it.", sock->OShandle(), epstr.str());
                shutdownAndCloseNoThrow(sock);
                return;
            }
        }
        m_selectHandler->add(sock, SELECTMODE_READ, this);
        Owned<CPersistentInfo> info = new CPersistentInfo(false, msTick(), 0, ep, proto, sock);
        m_infomap.setValue(sock, info.getLink());
        m_availkeeper.add(info);
    }

    virtual void remove(ISocket* sock) override
    {
        if (!sock)
            return;
        PERSILOG(PersistentLogLevel::PLogMax, "PERSISTENT: Removing socket %d from handler %d", sock->OShandle(), m_id);
        CriticalBlock block(m_critsect);
        Owned<CPersistentInfo>* val = m_infomap.getValue(sock);
        CPersistentInfo* info = nullptr;
        if (val)
            info = *val;
        bool removedFromSelectHandler = info && info->inUse; //If inUse sock was already removed from select handler
        if (info)
        {
            if (!info->inUse)
                m_availkeeper.remove(info);
            m_infomap.remove(sock);
        }
        if (!removedFromSelectHandler)
            m_selectHandler->remove(sock);
    }

    virtual void doneUsing(ISocket* sock, bool keep, unsigned usesOverOne, unsigned overrideMaxRequests) override
    {
        PERSILOG(PersistentLogLevel::PLogMax, "PERSISTENT: Done using socket %d, keep=%s", sock->OShandle(), boolToStr(keep));
        CriticalBlock block(m_critsect);
        Owned<CPersistentInfo>* val = m_infomap.getValue(sock);
        CPersistentInfo* info = nullptr;
        if (val)
            info = *val;
        if (info)
        {
            info->useCount += usesOverOne;
            unsigned requestLimit = overrideMaxRequests ? overrideMaxRequests : m_maxReqs;
            bool reachedQuota = requestLimit > 0 && requestLimit <= info->useCount;
            if(!sock->isValid())
                keep = false;
            if (keep && !reachedQuota)
            {
                info->inUse = false;
                info->timeUsed = msTick();
                m_selectHandler->add(sock, SELECTMODE_READ, this);
                m_availkeeper.add(info);
            }
            else
            {
                if (reachedQuota)
                    PERSILOG(PersistentLogLevel::PLogNormal, "PERSISTENT: Socket %d reached quota", sock->OShandle());
                if(!keep)
                    PERSILOG(PersistentLogLevel::PLogMax, "PERSISTENT: Indicated not to keep socket %d", sock->OShandle());
                remove(sock);
            }
        }
    }

    virtual ISocket* getAvailable(SocketEndpoint* ep = nullptr, bool* pShouldClose = nullptr, PersistentProtocol proto =  PersistentProtocol::ProtoTCP) override
    {
        CriticalBlock block(m_critsect);
        Owned<CPersistentInfo> info = m_availkeeper.get(ep, proto);
        if (info)
        {
            Linked<ISocket> sock = info->sock;
            info->inUse = true;
            info->timeUsed = msTick();
            info->useCount++;
            if (pShouldClose != nullptr)
                *pShouldClose = m_maxReqs > 0 && m_maxReqs <= info->useCount;
            m_selectHandler->remove(sock);
            PERSILOG(PersistentLogLevel::PLogMax, "PERSISTENT: Obtained persistent socket %d from handler %d", info->sock->OShandle(), m_id);
            return sock.getClear();
        }
        return nullptr;
    }

    //ISocketSelectNotify
    bool notifySelected(ISocket *sock,unsigned selected) override
    {
        size32_t x = sock->avail_read();
        if (x == 0)
        {
            PERSILOG(PersistentLogLevel::PLogNormal, "PERSISTENT: Detected closing of connection %d from the other end", sock->OShandle());
            if (m_enableDoNotReuseList)
            {
                CriticalBlock block(m_critsect);
                Owned<CPersistentInfo>* val = m_infomap.getValue(sock);
                CPersistentInfo* info = nullptr;
                if (val)
                    info = *val;
                if (info && info->epstr.length() > 0)
                {
                    int* countptr = m_instantCloseCounts.getValue(info->epstr.str());
                    if (info->useCount == 0)
                    {
                        const static int MAX_INSTANT_CLOSES = 5;
                        int count = 1;
                        if (countptr)
                            count = (*countptr)+1;
                        if (count < MAX_INSTANT_CLOSES)
                            m_instantCloseCounts.setValue(info->epstr.str(), count);
                        else if (m_doNotReuseList.getValue(info->epstr.str()) == nullptr)
                        {
                            PERSILOG(PersistentLogLevel::PLogMin, "PERSISTENT: Endpoint %s has instantly closed connection for %d times in a row, adding it to DoNotReuseList", info->epstr.str(), MAX_INSTANT_CLOSES);
                            m_doNotReuseList.setValue(info->epstr.str(), 1);
                        }
                    }
                    else if (countptr)
                        m_instantCloseCounts.remove(info->epstr.str());
                }
            }
            remove(sock);
        }
        else if (m_notify != nullptr)
        {
            bool reachedQuota = false;
            bool ignore = false;
            Owned<ISocket> mysock(LINK(sock));
            PERSILOG(PersistentLogLevel::PLogMax, "Data arrived on persistent connection %d", sock->OShandle());
            {
                CriticalBlock block(m_critsect);
                Owned<CPersistentInfo>* val = m_infomap.getValue(sock);
                CPersistentInfo* info = nullptr;
                if (val)
                    info = *val;
                if (info)
                {
                    m_availkeeper.remove(info);
                    info->inUse = true;
                    info->timeUsed = msTick();
                    info->useCount++;
                    reachedQuota = m_maxReqs > 0 && m_maxReqs <= info->useCount;
                }
                else
                {
                    ignore = true;
                    PERSILOG(PersistentLogLevel::PLogMin, "PERSISTENT: No info found for socket %d, ignore data", sock->OShandle());
                }
                m_selectHandler->remove(sock);
            }
            if (!ignore)
                m_notify->notifySelected(sock, selected, this, reachedQuota);
        }
        else
        {
            PERSILOG(PersistentLogLevel::PLogNormal, "PERSISTENT: Unexpected data received on connection %d, so discard the connection.", sock->OShandle());
            remove(sock);
        }

        return false;
    }

    //Thread
    virtual void start(bool inheritThreadContext) override
    {
        m_selectHandler->start();
        Thread::start(inheritThreadContext);
        PERSILOG(PersistentLogLevel::PLogNormal, "PERSISTENT: Handler %d started with max idle time %d and max requests %d", m_id, m_maxIdleTime, m_maxReqs);
    }

    virtual int run() override
    {
        while (true)
        {
            m_waitsem.wait(1000);
            if (m_stop)
                break;
            unsigned now = msTick();
            CriticalBlock block(m_critsect);
            std::vector<ISocket*> socks1;
            std::vector<ISocket*> socks2;
            for (auto& si:m_infomap)
            {
                CPersistentInfo* info = si.getValue();
                if (!info)
                    continue;
                if(m_maxIdleTime > 0 && !info->inUse && now - info->timeUsed >= m_maxIdleTime*1000)
                    socks1.push_back(*(ISocket**)(si.getKey()));
                if(info->inUse && now - info->timeUsed >= MAX_INFLIGHT_TIME*1000)
                    socks2.push_back(*(ISocket**)(si.getKey()));
            }
            for (auto& s:socks1)
            {
                PERSILOG(PersistentLogLevel::PLogNormal, "PERSISTENT: Socket %d has been idle for %d seconds so remove it", s->OShandle(), m_maxIdleTime);
                remove(s);
            }
            for (auto& s:socks2)
            {
                PERSILOG(PersistentLogLevel::PLogMin, "PERSISTENT: Socket %d has been in flight for %d seconds, remove it", s->OShandle(), MAX_INFLIGHT_TIME);
                remove(s);
            }
        }
        return 0;
    }

    virtual void stop(bool wait) override
    {
        m_selectHandler->stop(wait);
        m_stop = true;
        m_waitsem.signal();
        if(wait)
            join(1000);
        PERSILOG(PersistentLogLevel::PLogNormal, "PERSISTENT: Handler %d stopped", m_id);
    }

    virtual bool inDoNotReuseList(SocketEndpoint* ep)
    {
        if (!ep)
            return false;
        StringBuffer epstr;
        ep->getEndpointHostText(epstr);
        if (epstr.length())
        {
            CriticalBlock block(m_critsect);
            if (m_doNotReuseList.getValue(epstr.str()) != nullptr)
                return true;
        }
        return false;
    }
};

bool isHttpPersistable(const char* httpVer, const char* conHeader)
{
    if (isEmptyString(httpVer))
        return false;
    if (!isEmptyString(conHeader))
    {
        if (strieq(conHeader, "close"))
            return false;
        else if (strieq(conHeader, "Keep-Alive"))
            return true;
    }
    return !streq(httpVer, "1.0");
}

int CPersistentHandler::CurID = 0;

IPersistentHandler* createPersistentHandler(IPersistentSelectNotify* notify, int maxIdleTime, int maxReqs, PersistentLogLevel loglevel, bool enableDoNotReuseList)
{
    Owned<CPersistentHandler> handler = new CPersistentHandler(notify, maxIdleTime, maxReqs, loglevel, enableDoNotReuseList);
    handler->start(false);
    return handler.getClear();
}
