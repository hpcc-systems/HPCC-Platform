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

#define PERSILOG(loglevel, ...) if(static_cast<int>(loglevel) <= static_cast<int>(m_loglevel)) DBGLOG(__VA_ARGS__)

class CPersistentInfo : implements IInterface, public CInterface
{
    friend class CPersistentHandler;
public:
    IMPLEMENT_IINTERFACE;
    CPersistentInfo(bool _inUse, unsigned _timeUsed, unsigned _useCount, SocketEndpoint* _ep)
        : inUse(_inUse), timeUsed(_timeUsed), useCount(_useCount), ep(_ep?(new SocketEndpoint(*_ep)):nullptr)
    {
    }
    virtual ~CPersistentInfo() { } //TODO remove trace
protected:
    bool inUse;
    unsigned timeUsed;
    unsigned useCount;
    std::unique_ptr<SocketEndpoint> ep;
};

using SockInfoMap = MapBetween<Linked<ISocket>, ISocket*, Owned<CPersistentInfo>, CPersistentInfo*>;

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
    Mutex m_mutex;
    PersistentLogLevel m_loglevel = PersistentLogLevel::PLogNormal;
    static int CurID;
    int m_id = 0;
public:
    IMPLEMENT_IINTERFACE;
    CPersistentHandler(IPersistentSelectNotify* notify, int maxIdleTime, int maxReqs, PersistentLogLevel loglevel)
                        : m_stop(false), m_notify(notify), m_maxIdleTime(maxIdleTime), m_maxReqs(maxReqs), m_loglevel(loglevel)
    {
        m_id = ++CurID;
        m_selectHandler.setown(createSocketSelectHandler());
    }

    virtual ~CPersistentHandler()
    {
    }

    virtual void add(ISocket* sock, SocketEndpoint* ep = nullptr) override
    {
        if (!sock)
            return;
        PERSILOG(PersistentLogLevel::PLogNormal, "PERSISTENT: adding socket %d to handler %d", sock->OShandle(), m_id);
        synchronized block(m_mutex);
        m_selectHandler->add(sock, SELECTMODE_READ, this);
        m_infomap.setValue(sock, new CPersistentInfo(false, usTick()/1000, 0, ep));
    }

    virtual void remove(ISocket* sock) override
    {
        if (!sock)
            return;
        PERSILOG(PersistentLogLevel::PLogNormal, "PERSISTENT: Removing socket %d from handler %d", sock->OShandle(), m_id);
        synchronized block(m_mutex);
        Owned<CPersistentInfo>* val = m_infomap.getValue(sock);
        CPersistentInfo* info = nullptr;
        if (val)
            info = *val;
        if (!info || !info->inUse) //If inUse sock was already removed from select handler
            m_selectHandler->remove(sock);
        if (info)
            m_infomap.remove(sock);
    }

    virtual void doneUsing(ISocket* sock, bool keep, unsigned usesOverOne) override
    {
        synchronized block(m_mutex);
        Owned<CPersistentInfo>* val = m_infomap.getValue(sock);
        CPersistentInfo* info = nullptr;
        if (val)
            info = *val;
        if (info)
        {
            info->useCount += usesOverOne;
            bool quotaReached = m_maxReqs > 0 && m_maxReqs <= info->useCount;
            if (keep && !quotaReached)
            {
                info->inUse = false;
                info->timeUsed = usTick()/1000;
                m_selectHandler->add(sock, SELECTMODE_READ, this);
            }
            else
            {
                if (quotaReached)
                    PERSILOG(PersistentLogLevel::PLogMin, "PERSISTENT: Socket %d reached quota", sock->OShandle());
                if(!keep)
                    PERSILOG(PersistentLogLevel::PLogMax, "PERSISTENT: Indicated not to keep socket %d", sock->OShandle());
                remove(sock);
            }
        }
    }

    virtual Linked<ISocket> getAvailable(SocketEndpoint* ep = nullptr) override
    {
        synchronized block(m_mutex);
        for (auto si:m_infomap)
        {
            CPersistentInfo* info = si.getValue();
            if (info && !info->inUse && (ep == nullptr || (info->ep != nullptr && *(info->ep) == *ep)))
            {
                ISocket* sock = *(ISocket**)(si.getKey());
                if (sock)
                {
                    info->inUse = true;
                    info->timeUsed = usTick()/1000;
                    info->useCount++;
                    m_selectHandler->remove(sock);
                    PERSILOG(PersistentLogLevel::PLogMax, "PERSISTENT: Obtained persistent socket %d from handler %d", sock->OShandle(), m_id);
                    return sock;
                }
            }
        }
        return nullptr;
    }

    //ISocketSelectNotify
    bool notifySelected(ISocket *sock,unsigned selected) override
    {
        size32_t x = sock->avail_read();
        if (x == 0)
        {
            PERSILOG(PersistentLogLevel::PLogMin, "PERSISTENT: Detected closing of connection %d from the other end", sock->OShandle());
            remove(sock);
        }
        else if (m_notify != nullptr)
        {
            bool ignore = false;
            Owned<ISocket> mysock(LINK(sock));
            PERSILOG(PersistentLogLevel::PLogMax, "Data arrived on persistent connection %d", sock->OShandle());
            {
                synchronized block(m_mutex);
                Owned<CPersistentInfo>* val = m_infomap.getValue(sock);
                CPersistentInfo* info = nullptr;
                if (val)
                    info = *val;
                if (info)
                {
                    info->inUse = true;
                    info->timeUsed = usTick()/1000;
                    info->useCount++;
                }
                else
                {
                    ignore = true;
                    PERSILOG(PersistentLogLevel::PLogMin, "PERSISTENT: No info found for socket %d, ignore data", sock->OShandle());
                }
                m_selectHandler->remove(sock);
            }
            if (!ignore)
                m_notify->notifySelected(sock, selected, this);
        }
        return false;
    }

    //Thread
    virtual void start() override
    {
        m_selectHandler->start();
        Thread::start();
        PERSILOG(PersistentLogLevel::PLogNormal, "PERSISTENT: Handler %d started with max idle time %d and max requests %d", m_id, m_maxIdleTime, m_maxReqs);
    }

    virtual int run() override
    {
        while (true)
        {
            m_waitsem.wait(1000);
            if (m_stop)
                break;
            unsigned now = usTick()/1000;
            synchronized block(m_mutex);
            std::vector<ISocket*> socks1;
            std::vector<ISocket*> socks2;
            for (auto& si:m_infomap)
            {
                CPersistentInfo* info = si.getValue();
                if (!info)
                    continue;
                if(m_maxIdleTime > 0 && !info->inUse && info->timeUsed + m_maxIdleTime*1000 < now)
                    socks1.push_back(*(ISocket**)(si.getKey()));
                if(info->inUse && info->timeUsed + MAX_INFLIGHT_TIME*1000 < now)
                    socks2.push_back(*(ISocket**)(si.getKey()));
            }
            for (auto s:socks1)
            {
                PERSILOG(PersistentLogLevel::PLogMin, "PERSISTENT: Socket %d has been idle for %d seconds so remove it", s->OShandle(), m_maxIdleTime);
                remove(s);
            }
            for (auto s:socks2)
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
};

int CPersistentHandler::CurID = 0;

IPersistentHandler* createPersistentHandler(IPersistentSelectNotify* notify, int maxIdleTime, int maxReqs, PersistentLogLevel loglevel)
{
    Owned<CPersistentHandler> handler = new CPersistentHandler(notify, maxIdleTime, maxReqs, loglevel);
    handler->start();
    return handler.getClear();
}
