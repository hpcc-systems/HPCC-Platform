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

#ifndef __ESPTHREAD_HPP__
#define __ESPTHREAD_HPP__

#define TERMINATE_EXITCODE -99 // the working process use it to tell the monitor to exit too.


class CEspProtocolThread: public Thread
{
protected:
   bool terminating;
    
   CriticalSection sect;
    
   Semaphore ticksem;
    
   Owned<ISocket> m_socket;

   StringAttr m_name;

    int run();

public:
    IMPLEMENT_IINTERFACE;
    
   CEspProtocolThread(const char *name = "Unkown service type");
   CEspProtocolThread(ISocket *sock, const char *name = "Unkown service type");

   virtual ~CEspProtocolThread();
   virtual void start();
   void setSocket(ISocket *sock);
    void stop(bool wait);
   
   virtual const char *getServiceName();
   virtual bool onRequest();
};



#endif //__ESPTHREAD_HPP__
