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

#include "jliball.hpp"

#include "esp.hpp"

#include <assert.h>
#include <string>
#include <iostream>

using namespace std;

IEspService& ImageAccessSF(const char* ipLocalSync, int portLocalSync, 
                           const char* ipPeerSync,  int portPeerSync);

IEspRpcBinding &GetSOAPBindingFor_ImageAccess();



class CEdgeServicePlatform
{
private:
   Owned<IEspServiceEntry> m_services[256];
   int m_current;

public:
   CEdgeServicePlatform(){m_current=0;}

   void addService(IEspServiceEntry &service)
   {
      m_services[m_current++].set(&service);
   }

   int run()
   {
      //temporarily only exucute 1 service
         return (!m_services[0]) ? -1 : m_services[0]->run();
   }
   int stop()
   {
      //temporarily only exucute 1 service
         return (!m_services[0]) ? -1 : m_services[0]->stop();
   }
};



class CEdgeServiceEntry : public CInterface, 
   implements IEspServiceEntry
{
private:
   char m_name[256];
   
   Owned<IEspService> m_service;
   Owned<IEspRpcBinding> m_bindings[8];

public:
   IMPLEMENT_IINTERFACE;

   CEdgeServiceEntry(CEdgeServicePlatform &platform, const char *name, IEspService &service)
   {
      m_service.set(&service);
      platform.addService(*this);

      strncpy(m_name, (name != NULL) ? name : "", 256);
   }


//interface IEspServiceEntry
   void addRpcBinding(IEspRpcBinding &binding, const char *host, unsigned short port)
   {
      binding.addService(m_name, host, port, *m_service.get());
      m_bindings[0].set(&binding);
   }

   int run()
   {
      //temporarily only exucute 1 binding   
      return (!m_bindings[0]) ? -1 : m_bindings[0]->run();
   }
   int stop()
   {
      //temporarily only exucute 1 binding   
      return (!m_bindings[0]) ? -1 : m_bindings[0]->stop();
   }

};


void SplitIpPort(StringAttr & ip, unsigned & port, const char * address)
{
  char * colon = strchr(address, ':');
  if (colon)
  {
    ip.set(address,colon-address);
    port = atoi(colon+1);
  }
  else
    ip.set(address);
}

int g_topology = 0;
int g_it_port = 1500;

int main(int argc, char **argv)
{
   if (argc == 2 && !strcmp(argv[1], "-?"))
   {
      fprintf(stderr, "syntax: %s <soapip:port> <topology> <itport> <syncIp:port> <peerIp:port>", argv[0]);
      return -1;
   }

#ifndef _WIN32
   InitModuleObjects();
#endif
   PREPLOG();

    DBGLOG("Starting Image Access Server...");

   StringAttr ipPeer("");
   unsigned portPeer = 5001;

   if (argc > 5)
      SplitIpPort(ipPeer, portPeer, argv[4]);

   StringAttr ipSync("");
   unsigned portSync = 5000;

   if (argc > 4)
      SplitIpPort(ipSync, portSync, argv[1]);

   if (argc > 3)
   {
      g_it_port = atoi(argv[3]);
   }
   if (argc > 2)
   {
      g_topology = atoi(argv[2]);
   }
   
   StringAttr ip("");
   unsigned port = 4242;

   if (argc > 1)
      SplitIpPort(ip, port, argv[1]);

   CEdgeServicePlatform platform;
   
   IEspService& iaService = ImageAccessSF(ipSync.get(), portSync, ipPeer, portPeer);
   
   CEdgeServiceEntry servImageToken(platform, "ImageAccess", iaService);

   servImageToken.addRpcBinding(GetSOAPBindingFor_ImageAccess(), ip.get(), port);

   while(1)
   {
      try
      {
         platform.run();
      }
      catch(IException &expt)
      {
         StringBuffer msg;
            DBGLOG("IException: %d == %s", expt.errorCode(), expt.errorMessage(msg).str());
      }
      catch(...)
      {
            DBGLOG("unknown exception caught");
      }

      platform.stop();
   }   
   
   iaService.Release();
   return 0;
}
