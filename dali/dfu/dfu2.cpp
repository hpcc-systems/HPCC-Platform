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

#include <platform.h>
#include <stdio.h>
#include <limits.h>
#include "jexcept.hpp"
#include "jptree.hpp"
#include "jsocket.hpp"
#include "jstring.hpp"
#include "jmisc.hpp"

#include "daclient.hpp"
#include "dadfs.hpp"
#include "dasds.hpp"
#include "dalienv.hpp"
#include "jio.hpp"
#include "daft.hpp"
#include "daftcfg.hpp"
#include "fterror.hpp"
#include "rmtfile.hpp"
#include "daftprogress.hpp"
#include "dfurun.hpp"
#include "dfuwu.hpp"

#include "jprop.hpp"

#include "dfuerror.hpp"

IProperties *iniFile;

#define MAX_COPIES 4

bool verbose = true;
enum display_type {display_header, display_file, display_directory};


//============================================================================

void handleSyntax()
{
    printf("Usage:\n");
    printf("   DFU2 SERVER DALISERVERS=<ip> <queue-name> { <queue-name> } -- starts DFU server\n\n");
}

void cmdServer(int argc, const char *argv[])
{
    Owned<IDFUengine> engine = createDFUengine();
    if (argc<3)
        printf("No queue specified for server");
    else {
        unsigned i;
        for (i=2;i<argc;i++) {
            engine->startListener(argv[i]);
        }
        engine->joinListeners();
    }
}


bool actionOnAbort()
{
    LOG(MCprogress, unknownJob, "Exiting");
    closeEnvironment();
    closedownClientProcess();
    releaseAtoms();
    return true;
} 


int main(int argc, const char *argv[])
{
    InitModuleObjects();
    StringBuffer daliServer;
    // look for overrides
    unsigned i;
    unsigned j=1;
    for (i=1;i<argc;i++) {
        if ((strlen(argv[i])>12)&&memicmp(argv[i],"DALISERVERS=",12)==0) 
            daliServer.append(strlen(argv[i])-12,argv[i]+12);
        else 
            argv[j++] = argv[i];
    }
    argc = j;
    if (argc < 2 || argv[1][0]=='/' && argv[1][1]=='?')
    {
        handleSyntax();
        return argc<2 ? DFUERR_InvalidCommandSyntax : 0;
    }
    

    int ret = 0;

    try {
        iniFile = createProperties("dfu2.ini",true);
        StringBuffer logFile;
        if (!iniFile->getProp("LOGFILE", logFile)) 
            logFile.append("DFU2.log");
        attachStandardFileLogMsgMonitor(logFile.str(), NULL, MSGFIELD_STANDARD, MSGAUD_all, MSGCLS_all, TopDetail, false);

        unsigned userDetail = iniFile->getPropInt("DETAIL", DefaultDetail);
        ILogMsgFilter * userOperator = getCategoryLogMsgFilter(MSGAUD_all, MSGCLS_disaster|MSGCLS_error|MSGCLS_warning, userDetail, true);
        ILogMsgFilter * errWarn = getCategoryLogMsgFilter(MSGAUD_operator|MSGAUD_user, MSGCLS_all, TopDetail, true);
        ILogMsgFilter * combinedFilter = getOrLogMsgFilterOwn(userOperator, errWarn);
        
        queryLogMsgManager()->changeMonitorFilterOwn(queryStderrLogMsgHandler(), combinedFilter);
        queryStderrLogMsgHandler()->setMessageFields(0);

        if (!daliServer.length()&&!iniFile->getProp("DALISERVERS", daliServer)) 
            throwError(DFUERR_NoDaliServerList);
        Owned<IGroup> serverGroup = createIGroup(daliServer.str(),DALI_SERVER_PORT);
        initClientProcess(serverGroup, 0, NULL, NULL, MP_WAIT_FOREVER, true);               // so that 
        setPasswordsFromSDS(); 
            
        DFUcmd cmd = decodeDFUcommand(argv[1]);
        addAbortHandler(actionOnAbort);

        try {
            switch(cmd) {
            case DFUcmd_server:
                cmdServer(argc, argv);
                break;
            case DFUcmd_none:
                break;
            default:
                // chain to DFUcmd for rest TBD
                printf("DFU2 %s not yet implemented.\n",argv[1]);
            }
        }
        catch(IException *e)
        { 
            EXCLOG(e, "DFU Exception: ");
            ret = e->errorCode();
            e->Release();
        }
        catch (const char *s) {
            ERRLOG("DFU: %s",s);
        }
    }
    catch(IException *e){ 
        EXCLOG(e, "DFU Exception: ");
        ret = e->errorCode();
        if (e->errorCode() == DFUERR_InvalidCommandSyntax || e->errorCode() == DFUERR_TooFewArguments)
            handleSyntax();
        e->Release();
    }
    catch (const char *s) {
        ERRLOG("DFU: %s",s);
    }
    
    closeEnvironment();
    closedownClientProcess();
    releaseAtoms();
    return ret;
}

