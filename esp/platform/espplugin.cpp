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
      ERRLOG("Exception(%d, %s) socket(%d).", e->errorCode(), e->errorMessage(estr).str(), m_socket->OShandle());
      e->Release();
    }

   ERRLOG("Closing Socket(%d)", m_socket->OShandle());

   m_socket->shutdown();
   m_socket->close();

   Release();
   return 0;
}
