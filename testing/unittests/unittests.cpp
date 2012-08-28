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

#ifdef _USE_CPPUNIT
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
 * all unit tests, be they internal or external (API),
 * will run.
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
 * CPPUnit will automatically recognise and run them all.
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

#endif // _USE_CPPUNIT
