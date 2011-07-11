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

#include "platform.h"
#include "jlib.hpp"
#include "jlog.hpp"
#include "jmisc.hpp"

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>

#define ASSERT(a) { if (!(a)) CPPUNIT_ASSERT(a); }

void loadDll(const char *name)
{
    SharedObject *so = new SharedObject;
    so->load(name, true);
}

void loadDlls()
{
    loadDll("jhtree");
}

int main(int argc, char* argv[])
{
    loadDlls();
    InitModuleObjects();
    queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_time);
    CppUnit::TextUi::TestRunner runner;
    if (argc==1)
    {
        CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
        runner.addTest( registry.makeTest() );
    }
    else 
    {
        for (int name = 1; name < argc; name++)
        {
            CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry(argv[name]);
            runner.addTest( registry.makeTest() );
        }
    }
    bool wasSucessful = runner.run( "", false );
    ExitModuleObjects();
    releaseAtoms();
    return wasSucessful;
}

