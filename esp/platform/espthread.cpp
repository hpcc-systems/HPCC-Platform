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

#pragma warning (disable : 4786)

#ifdef _WIN32
#ifdef ESPHTTP_EXPORTS
    #define esp_http_decl __declspec(dllexport)
#endif
#endif

//Jlib
#include "jliball.hpp"

//CRT / OS
#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <signal.h>  
#include <errno.h>
#else
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stddef.h>
#include <errno.h>
#endif

//SCM Interfaces
//#include "IAEsp.hpp"
//#include "IAEsc.hpp"

//ESP Core
#include "espthread.hpp"


CEspProtocolThread::CEspProtocolThread(const char *name)
: Thread("CEspProtocolThread"), m_name(name)
{
    terminating = false;
}


CEspProtocolThread::CEspProtocolThread(ISocket *sock, const char *name)
: Thread("CEspProtocolThread"), m_name(name)
{
    terminating = false;
    setSocket(sock);
}


CEspProtocolThread::~CEspProtocolThread()
{
}

void CEspProtocolThread::start()
{
    Thread::start();
}


const char *CEspProtocolThread::getServiceName()
{
    return m_name.get();
}


bool CEspProtocolThread::onRequest()
{
    return false;
}


void CEspProtocolThread::setSocket(ISocket *sock)
{
    CriticalBlock block(sect);
    
    m_socket.setown(sock);
    //DBGLOG("%s -- serving socket(%d):", this->getServiceName(), sock->OShandle());
    
    char xname[256]={0};
    
    m_socket->name(xname, 256);
    //DBGLOG("... on net address(%s)", xname);
    
    m_socket->peer_name(xname, 256);
    //DBGLOG("... from net address(%s)", xname);
    
    ticksem.signal();
}


void CEspProtocolThread::stop(bool wait)
{
    terminating = true;
    
    ticksem.signal();
    
    //if (wait)
    //  join();
}

//Unused code cause Linker warning LNK4089: 
// all references to 'dynamic-link library' discarded by /OPT:REF

#if 0
bool wait_to_exit(bool can_continue, ISocket* sock, int timeoutsecs)
{
    if(sock == NULL)
        return false;

    int sockid = sock->OShandle();
    fd_set rdfds;
    timeval selecttimeout;
    
    FD_ZERO(&rdfds);
    FD_SET(sockid, &rdfds);

    selecttimeout.tv_sec = timeoutsecs;
    selecttimeout.tv_usec = 0;

    int availread = 0;
    int n = select(sockid + 1, &rdfds, NULL, NULL, &selecttimeout);
    if (n < 0) 
    {
        if (errno != EINTR) {
            DBGLOG("error selecting in wait_to_exit() error %d\n", errno);
            return false;
        }
    }
    else if(n > 0)
    {
        DBGLOG("Select on socket %d returns before timeout", sockid);
        availread = sock->avail_read();
        if(availread > 0)
        {
            // This is the only place that can possibly return true.
            return can_continue;
        }
        else
        {
            DBGLOG("The remote side of the socket %d has been closed.", sockid);
            return false;
        }

    }
    else
    {
        DBGLOG("Select returns after timeout");
        return false;
    }

    return false;
}
#endif

int CEspProtocolThread::run()
{
    Link();
    try 
    {
        bool can_continue = false;      
        do
        {
            can_continue = onRequest();
        }
        while (can_continue);
    }
    catch (IException *e) 
    {
        StringBuffer estr;
        ERRLOG("Exception(%d, %s) in CEspProtocolThread::run while processing request.", e->errorCode(), e->errorMessage(estr).str());
        e->Release();
    }
    catch(...)
    {
        ERRLOG("Unknown Exception in CEspProtocolThread::run while processing request.");
    }

    try
    {
        m_socket->shutdown();
        m_socket->close();
    }
    catch (IException *e) 
    {
        StringBuffer estr;
        DBGLOG("Exception(%d, %s) - in CEspProtocolThread::run while closing socket.", e->errorCode(), e->errorMessage(estr).str());
        e->Release();
    }
    catch(...)
    {
        DBGLOG("General Exception - in CEspProtocolThread::run while closing socket.");
    }

    Release();
    return 0;
}

