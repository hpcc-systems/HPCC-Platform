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

#include <snmpwalker.hpp>
#include <stdio.h>

class CSnmpWalkerCallback : public CInterface,
                            implements ISnmpWalkerCallback
{
public:
   IMPLEMENT_IINTERFACE;

   CSnmpWalkerCallback(){}
   virtual ~CSnmpWalkerCallback(){}

    virtual void processValue(const char *oid, const char *value)
   {
       printf (">>> %s = '%s'\n", oid, value);
   }

private:
   CSnmpWalkerCallback(const CSnmpWalkerCallback&);
};


int main (int argc, char** argv)
{
   const char* ip = "10.150.51.50";
   if (argc > 1)
      ip = argv[1];

   CSnmpWalkerCallback callback;
   ISnmpWalker* pSnmpWalker = createWalker(ip, &callback, "1.3.6.1.4.1.12723.6.16.1.4.1.2", "M0n1T0r");
    printf ("Walk returned %d objects\n\n", pSnmpWalker->walk());
   pSnmpWalker->Release();
   
    return (0);
}
