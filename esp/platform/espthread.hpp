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
    bool keepAlive;
public:
    IMPLEMENT_IINTERFACE;
    
   CEspProtocolThread(const char *name = "Unknown service type");
   CEspProtocolThread(ISocket *sock, const char *name = "Unknown service type");

   virtual ~CEspProtocolThread();
   virtual void start();
   void setSocket(ISocket *sock);
    void stop(bool wait);
   
   virtual const char *getServiceName();
   virtual bool onRequest();
};



#endif //__ESPTHREAD_HPP__
