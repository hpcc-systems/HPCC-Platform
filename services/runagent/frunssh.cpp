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
    try  {
        StringBuffer logname;
        splitFilename(argv[0], NULL, NULL, &logname, NULL);
        StringBuffer logdir;
        if (getConfigurationDirectory(NULL,"log","frunssh",logname.str(),logdir)) {
            recursiveCreateDirectory(logdir.str());
            StringBuffer tmp(logname);
            addPathSepChar(logname.clear().append(logdir)).append(tmp);
        }
        addFileTimestamp(logname, true);
        logname.append(".log");
        appendLogFile(logname.str(),0,false);
        queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_prefix);

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


