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

#include "platform.h"
#include "jlib.hpp"
#include "jlog.hpp"
#include "jmisc.hpp"

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>

#define ASSERT(a) { if (!(a)) CPPUNIT_ASSERT(a); }
#define ASSERT_EQUAL(a, b) { CPPUNIT_ASSERT_EQUAL(a, b); }

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
    SharedObject so;
public:
    IMPLEMENT_IINTERFACE;

    LoadedObject(const char * name)
    {
        so.load(name, true);
    }
};
