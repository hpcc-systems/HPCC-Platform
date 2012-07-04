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

/*
 * This is the main unittest driver for HPCC. From here,
 * all unit tests, be them internal or external (API).
 *
 * All internal unit tests, written on the same source
 * files as the implementation they're testing, can be
 * dynamically linked via the helper class below.
 *
 * All external unit tests (API tests, test-driven
 * development, interface documentation and general
 * usability tests) should be implemented as source
 * files within the same directory as this file, and
 * statically linked together.
 *
 * CPPUnit will automaticall recognise and run them all.
 */

/*
 * Helper class to unload libraries at the end
 * and make sure the SharedObject gets deleted
 * correctly.
 *
 * This is important to run valgrind tests and not
 * having to care about which memory leaks are "good"
 * and which are not.
 */
class LoadedObject : public IInterface, CInterface {
    SharedObject *so;
public:
    IMPLEMENT_IINTERFACE;

    LoadedObject(const char * name)
    {
        so = new SharedObject;
        so->load(name, true);
    }
    ~LoadedObject()
    {
        so->unload();
        delete so;
    }
};

int main(int argc, char* argv[])
{
    InitModuleObjects();
    // These are the internal unit tests covered by other modules and libraries
    Array objects;
    objects.append(*(new LoadedObject ("jhtree")));
    objects.append(*(new LoadedObject ("roxiemem")));
    objects.append(*(new LoadedObject ("thorhelper")));

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

