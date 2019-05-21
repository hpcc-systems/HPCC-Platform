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

#define USING_SCM_WITH_STL

#include "jlib.hpp"
#include "jlog.hpp"
#include "jiface.hpp"
#include "jstring.hpp"
#include "jexcept.hpp"
#include "jthread.hpp"
#include "jsocket.hpp"
#include "jptree.hpp"

//#include "IAEsp.hpp"
//#include "IAEsc.hpp"

#include "espthread.hpp"

#include <iostream>
using namespace std;

#include <conio.h>




CEspServiceThread::CEspServiceThread(const char *name)
    : Thread("CEspServiceThread"), m_name(name)
{
    terminating = false;
}

CEspServiceThread::CEspServiceThread(ISocket *sock, const char *name)
    : Thread("CEspServiceThread"), m_name(name)
{
    terminating = false;

   setSocket(sock);
}

CEspServiceThread::~CEspServiceThread()
{
}

const char *CEspServiceThread::getServiceName()
{
   return m_name.get();
}

bool CEspServiceThread::onRequest()
{
   return false;
}

void CEspServiceThread::setSocket(ISocket *sock)
{
    CriticalBlock block(sect);

   m_socket.set(sock);
   DBGLOG("%s -- serving socket(%d):", this->getServiceName(), sock->OShandle());

   char xname[256]={0};

   m_socket->name(xname, 256);
   DBGLOG("... on net address(%s)", xname);

   m_socket->peer_name(xname, 256);
   DBGLOG("... from net address(%s)", xname);

   ticksem.signal();
}

void CEspServiceThread::stop(bool wait)
{
    terminating = true;
    
   ticksem.signal();
    
   if (wait)
        join();
}

int CEspServiceThread::run()
{
   Link();

   try 
   {
      while(onRequest());
    }
    catch (IException *e) 
   {
      StringBuffer estr;
      IERRLOG("Exception(%d, %s) socket(%d).", e->errorCode(), e->errorMessage(estr).str(), m_socket->OShandle());
      e->Release();
    }

   DBGLOG("Closing Socket(%d)", m_socket->OShandle());

   m_socket->shutdown();
   m_socket->close();

   Release();
   return 0;
}
