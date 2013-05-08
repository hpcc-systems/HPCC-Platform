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

#include "platform.h"

#include "jlib.hpp"
#include "jfile.hpp"
#include "jlog.hpp"
#include "jmisc.hpp"
#include "rmtssh.hpp"

int main( int argc, char *argv[] )
{
    int res=0;
    if (argc < 3)
    {
        printf
            ("frunssh <nodelistfile> \"command\" [options] \n"
            "    options: -i:<identity-file> \n"
            "             -u:<user> \n"
            "             -n:<number_of_threads>\n"
            "             -t:<connect-timeout-secs>\n"
            "             -a:<connect-attempts>\n"
            "             -d:<working_directory>\n"
            "             -s                -- strict, must match known_hosts\n"
            "             -b                -- background\n"
            "             -pw:<password>    -- INSECURE: requires pssh (NB identity file preferred)\n"
            "             -pe:<password>    -- INSECURE: as -pw except encrypted password\n"
            "             -pl               -- use plink (on windows)\n"
            "             -v                -- verbose, lists commands run\n"
            "             -d                -- dry run (for testing, enables verbose)\n"
            );
        return 255;
    }


    InitModuleObjects();

#ifndef __64BIT__
    Thread::setDefaultStackSize(0x10000);   // NB under windows requires linker setting (/stack:)
#endif

    try  {
        StringBuffer logname;
        splitFilename(argv[0], NULL, NULL, &logname, NULL);

        Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator("frunssh");
        lf->setCreateAliasFile(false);
        lf->setMsgFields(MSGFIELD_prefix);
        lf->beginLogging();

        Owned<IFRunSSH> runssh = createFRunSSH();
        runssh->init(argc,argv);
        runssh->exec();
    }
    catch(IException *e)
    {
        EXCLOG(e,"frunssh");
        e->Release();
        res=255;
    }
    releaseAtoms();
    return res;
}


