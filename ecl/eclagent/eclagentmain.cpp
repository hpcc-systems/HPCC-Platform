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
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jlog.hpp"
#include "eclhelper.hpp"

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#endif

void usage()
{
    printf("USAGE: eclagent WUID=wuid options\n"
           "       eclagent PROCESS=dllname options\n"
           "options are:\n"
           "       DALISERVERS=daliEp,daliEp\n"
           "       VERSIONS=1 (prints full version info)\n"
           "       WFRESET=1  (performs workflow reset on starting)\n"
           "       NORETRY=1  (immediately fails if workunit is in failed state)\n");
}

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

    if (argc==1 || *argv[1]=='?' || *argv[1]=='-')
    {
        usage();
        return 2;
    }
    else
    {
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
            ERRLOG("Eclagent execution error: Unexpected exception");
            ret = 2;
        }
        releaseAtoms();
        ExitModuleObjects();
        return ret;
    }
}
