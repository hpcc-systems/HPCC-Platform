/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include "jlib.hpp"

#include "hrpcsock.hpp"
#include "hrpcutil.hpp"
#include "jsocket.hpp"
#include "jmutex.hpp"
#include "jthread.hpp"

void SplitIpPort(StringAttr & ip, unsigned & port, const char * address)
{
  const char * colon = strchr(address, ':');
  if (colon)
  {
    ip.set(address,colon-address);
    if (strcmp(ip, ".")==0)
        ip.set(GetCachedHostName());
    port = atoi(colon+1);
  }
  else
    ip.set(address);
}


IHRPCtransport *MakeTcpTransportFromUrl(const char *target, unsigned defaultPort)
{
  StringAttr ip;
  SplitIpPort(ip, defaultPort, target);
  return MakeTcpTransport(ip, defaultPort);
}

void TcpWhoAmI(StringAttr & out)
{
  out.set(GetCachedHostName());
}


void ListenUntilDead(HRPCserver & server, const char * errorMessage)
{
  ListenUntilDead(server, NULL, errorMessage);
}

void ListenUntilDead(HRPCserver & server, IHRPCtransport * transport, const char * errorMessage)
{
  bool alive = true;
  while (alive)
  {
    try
    {
      server.Listen(transport);
      alive=false;
    }
    catch(IHRPC_Exception *e) 
    { 
      switch (e->errorCode()) 
      {
      case HRPCERR_lost_connection:
      case HRPCERR_transport_not_open:
        PrintExceptionLog(e,"Listening connection lost - listen again");
        //MORE: The owner went down...
        e->Release();
        break;
      default:
        if (errorMessage)
          PrintExceptionLog(e,errorMessage);
        e->Release();
        alive = false;
        break;
      }
    }
  }
}

IHRPCtransport * TryMakeServerTransport(unsigned port, const char * errorMessage)
{
  IHRPCtransport *transport = NULL;
  try
  {
    transport = MakeTcpTransport(NULL,port);
  }
  catch(IHRPC_Exception *e) 
  { 
        switch (e->errorCode()) 
    {
        case HRPCERR_transport_port_in_use:
    default:
      if (errorMessage)
        pexception(errorMessage, e);
      e->Release();
      break;
    }
  }
  return transport;
}

#define MAXCONNECTIONS 16

static CriticalSection mcsect;


bool FastMultipleConnect(unsigned n,HRPCmodule **modules,bool *done,int timeout)
{
    CriticalSection sect;
    PointerIArrayOf<ISocket> sockets;
    SocketEndpointArray eps;
    unsigned i;
    for (i=0;i<n;i++) {
        SocketEndpoint ep;
        if (!getTcpTarget(modules[i]->queryTransport(),ep))
            return false;
        eps.append(ep);
        done[i] = false;
    }
    multiConnect(eps,sockets,60*1000);
    assertex(n==sockets.ordinality());
    bool ret = true;
    for (i=0;i<n;i++) {
        ISocket *sock = sockets.item(i);
        if (sock) {
            modules[i]->AttachConnect(sock,true);
            done[i] = true;
        }
        else {
            StringBuffer epstr;
            eps.item(i).getUrlStr(epstr);
            //ERRLOG("Failed to connect to %s",epstr.str());
            ret = false;
        }
    }
    return ret;
}

void MultipleConnect(unsigned n,HRPCmodule **modules,int timeout,bool fast)
{
    CriticalBlock block(mcsect);
    bool *done = new bool[n];
    unsigned i;
    for (i=0;i<n;i++) 
        done[i] = false;
    if (fast) {
        if (FastMultipleConnect(n,modules,done,timeout)) {
            delete [] done;
            return;
        }
        WARNLOG("FastMultipleConnect failed, falling back to MultipleConnect");
    }
    class casyncfor: public CAsyncFor
    {
        HRPCmodule **modules;
        bool *done;
        int timeout;
    public:
        casyncfor(HRPCmodule **_modules,bool *_done,int _timeout)
        { 
            modules = _modules;
            done = _done;
            timeout = _timeout;
        }
        void Do(unsigned idx)
        {
            if (!done[idx])
                modules[idx]->TryConnect(timeout,true,true);
        }
    } afor(modules,done,timeout);
    afor.For(n,MAXCONNECTIONS,false,true);
    delete [] done;
}


void MultipleConnect(unsigned n,HRPCmodule *modules,int timeout,bool fast)
{
    HRPCmodule **moduleptrs = (HRPCmodule **)malloc(n*sizeof(HRPCmodule *));
    for (unsigned i=0;i<n;i++) 
        moduleptrs[i] = &modules[i];
    MultipleConnect(n,moduleptrs,timeout,fast);
    free(moduleptrs);
}

