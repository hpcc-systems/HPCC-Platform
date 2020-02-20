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
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jlog.hpp"
#include "eclhelper.hpp"

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#endif

extern int STARTQUERY_API eclagent_main(int argc, const char *argv[], StringBuffer * embeddedWU, bool standAlone);

int main(int argc, const char *argv[])
{
#ifdef _USE_CPPUNIT
    if (argc>=2 && stricmp(argv[1], "-selftest")==0)
    {
        InitModuleObjects();
        queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_time|MSGFIELD_prefix);
        CppUnit::TextUi::TestRunner runner;
        if (argc==2)
        {
            CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
            runner.addTest( registry.makeTest() );
        }
        else 
        {
            for (int name = 2; name < argc; name++)
            {
                CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry(argv[name]);
                runner.addTest( registry.makeTest() );
            }
        }
        bool wasSucessful = runner.run( "", false );
        releaseAtoms();
        return wasSucessful;
    }
#endif

    EnableSEHtoExceptionMapping();
    InitModuleObjects();
    int ret = 0;
    try
    {
        ret = eclagent_main(argc, argv, NULL, false);
    }
    catch (IException *E)
    {
        EXCLOG(E, "Eclagent execution error");
        E->Release();
        ret = 2;
    }
    catch (...)
    {
        IERRLOG("Eclagent execution error: Unexpected exception");
        ret = 2;
    }
    releaseAtoms();
    ExitModuleObjects();
    return ret;
}
