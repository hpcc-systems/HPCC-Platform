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
