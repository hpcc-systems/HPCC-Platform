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

/*
    Copyright (C) 2001 Seisint, Inc.
    All rights reserved.

   Portions Copyright (C) 2001 Robert A. van Engelen, Florida State University.
*/

#include "jliball.hpp"
#include "esp.hpp"
#include "IAEsp.hpp"


#include "../ImageAccess/IASoap.h"
#include "../ImageAccess/gSOAP_ia.h"

extern Owned<IEspSoapBinding> g_binding;


int soap_run(const char *host, short port)
{
   int m, hsocket;

   m = soap_bind((char *)host, port, 100);

   if (m < 0)
   {
      soap_print_fault(stderr);
      return (-1);
   }

   fprintf(stderr, "Socket connection successful: master socket = %d\n", m);

   for (int i = 1; ; i++)
   {
      hsocket = soap_accept();

      if (hsocket < 0)
      {
         soap_print_fault(stderr);
         return (-1);
      }

      fprintf(stderr, "%d: accepted connection from IP = %d.%d.%d.%d socket = %d\n", i, 
         (soap_ip<<24)&0xFF, (soap_ip<<16)&0xFF, (soap_ip<<8)&0xFF, soap_ip&0xFF, hsocket);

      soap_serve(); // process RPC skeletons

      fprintf(stderr, "request served\n");
      soap_end(); // clean up everything and close socket
   }

   return 0;
}


int soap_serve()
{
    soap_begin();
    if (soap_envelope_begin_in())
        return soap_return_fault();
    if (soap_recv_header())
        return soap_return_fault();
    if (soap_body_begin_in())
        return soap_return_fault();

   if (g_binding)
      soap_error = g_binding->processRequest();

   if (soap_error)
        return soap_return_fault();
    return soap_error;
}

