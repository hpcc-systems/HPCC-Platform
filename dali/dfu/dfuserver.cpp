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
#include "thirdparty.h"
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
#include "rmtspawn.hpp"
#include "daftprogress.hpp"
#include "dfurun.hpp"
#include "dfuwu.hpp"
#include "dfurepl.hpp"
#include "mplog.hpp"

#include "jprop.hpp"

#include "dfuerror.hpp"

static Owned<IPropertyTree> globals;
ILogMsgHandler * fileMsgHandler;
Owned<IDFUengine> engine;

#define DEFAULT_PERF_REPORT_DELAY 60

//============================================================================

void usage()
{
    printf("Usage:\n");
    printf("  DFUSERVER DALISERVERS=<ip> QUEUE=<q-name>          -- starts DFU Server\n\n");
    printf("  DFUSERVER DALISERVERS=<ip> STOP=1 QUEUE=<q-name>   -- stops DFU Server\n\n");
}



static bool exitDFUserver()
{
    engine->abortListeners();
    return false;
}

inline void XF(IPropertyTree &pt,const char *p,IProperties &from,const char *fp)
{
    const char * v = from.queryProp(fp);
    if (v&&*v)
        pt.setProp(p,v);
}

IPropertyTree *readOldIni()
{
    IPropertyTree *ret = createPTree("DFUSERVER",true);
    ret->setProp("@name","mydfuserver");
    ret->addPropTree("SSH",createPTree("SSH",true));
    Owned<IProperties> props = createProperties("dfuserver.ini", true);
    if (props) {
        XF(*ret,"@name",*props,"name");
        XF(*ret,"@daliservers",*props,"daliservers");
        XF(*ret,"@enableSNMP",*props,"enableSNMP");
        XF(*ret,"@enableSysLog",*props,"enableSysLog");
        XF(*ret,"@queue",*props,"queue");
        XF(*ret,"@monitorqueue",*props,"monitorqueue");
        XF(*ret,"@monitorinterval",*props,"monitorinterval");
        XF(*ret,"@transferBufferSize",*props,"transferBufferSize");
        XF(*ret,"@replicatequeue",*props,"replicatequeue");
        XF(*ret,"@log_dir",*props,"log_dir");
    }
    return ret;
}


int main(int argc, const char *argv[])
{
    InitModuleObjects();
    EnableSEHtoExceptionMapping();

    NoQuickEditSection xxx;

    Owned<IFile> file = createIFile("dfuserver.xml");
    if (file->exists())
        globals.setown(createPTreeFromXMLFile("dfuserver.xml", true));
    else
        globals.setown(readOldIni());

    for (unsigned i=1;i<(unsigned)argc;i++) {
        const char *arg = argv[i];
        StringBuffer prop("@");
        StringBuffer val;
        while (*arg && *arg != '=')
            prop.append(*arg++);
        if (*arg) {
            arg++;
            while (isspace(*arg))
                arg++;
            val.append(arg);
            prop.clip();
            val.clip();
            if (prop.length()>1)
                globals->setProp(prop.str(), val.str());
        }
    }
    StringBuffer daliServer;
    StringBuffer queue;
    if (!globals->getProp("@DALISERVERS", daliServer)||!globals->getProp("@QUEUE", queue)) {
        usage();
        globals.clear();
        releaseAtoms();
        return 1;
    }
    Owned<IFile> sentinelFile;
    bool stop = globals->getPropInt("@STOP",0)!=0;
    if (!stop) {
        sentinelFile.setown(createSentinelTarget(argv[0], "dfuserver"));
        sentinelFile->remove();

        StringBuffer logname;
        StringBuffer logdir;
        if (!getConfigurationDirectory(globals->queryPropTree("Directories"),"log","dfuserver",globals->queryProp("@name"),logdir))
            globals->getProp("@LOG_DIR", logdir);
        if (logdir.length() && recursiveCreateDirectory(logdir.str()))
            logname.append(logdir);
        else
            getCurrentDirectory(logname, true);

        if (logname.length() && logname.charAt(logname.length()-1) != PATHSEPCHAR)
            logname.append(PATHSEPCHAR);
        logname.append("dfuserver");
        StringBuffer aliasLogName(logname);
        aliasLogName.append(".log");
        fileMsgHandler = getRollingFileLogMsgHandler(logname.str(), ".log", MSGFIELD_STANDARD, false, true, NULL, aliasLogName.str());
        queryLogMsgManager()->addMonitorOwn(fileMsgHandler, getCategoryLogMsgFilter(MSGAUD_all, MSGCLS_all, 1000));
    }
    StringBuffer ftslogdir;
    if (getConfigurationDirectory(globals->queryPropTree("Directories"),"log","ftslave",globals->queryProp("@name"),ftslogdir)) // NB instance deliberately dfuserver's
        setFtSlaveLogDir(ftslogdir.str());
    setRemoteSpawnSSH(
        globals->queryProp("SSH/@SSHidentityfile"),
        globals->queryProp("SSH/@SSHusername"),
        globals->queryProp("SSH/@SSHpassword"),
        globals->getPropInt("SSH/@SSHtimeout",0),
        globals->getPropInt("SSH/@SSHretries",3),
        "run_");
    bool enableSNMP = globals->getPropInt("@enableSNMP")!=0;
    CSDSServerStatus *serverstatus=NULL;
    Owned<IReplicateServer> replserver;
    try {
        Owned<IGroup> serverGroup = createIGroup(daliServer.str(),DALI_SERVER_PORT);
        initClientProcess(serverGroup, DCR_DfuServer, 0, NULL, NULL, stop?(1000*30):MP_WAIT_FOREVER);
        setPasswordsFromSDS();

        if(!stop)
        {
            if (globals->getPropBool("@enableSysLog",true))
                UseSysLogForOperatorMessages();

            serverstatus = new CSDSServerStatus("DFUserver");
            setDaliServixSocketCaching(true); // speeds up lixux operations

            startLogMsgParentReceiver();    // for auditing
            connectLogMsgManagerToDali();

            engine.setown(createDFUengine());
            addAbortHandler(exitDFUserver);
        }
        const char *q = queue.str();
        loop {
            StringBuffer subq;
            const char *comma = strchr(q,',');
            if (comma)
                subq.append(comma-q,q);
            else
                subq.append(q);
            if (stop) {
                stopDFUserver(subq.str());
            }
            else {
                StringBuffer mask;
                mask.appendf("Queue[@name=\"%s\"][1]",subq.str());
                IPropertyTree *t=serverstatus->queryProperties()->queryPropTree(mask.str());
                if (t)
                    t->setPropInt("@num",t->getPropInt("@num",0)+1);
                else {
                    t = createPTree(false);
                    t->setProp("@name",subq.str());
                    t->setPropInt("@num",1);
                    serverstatus->queryProperties()->addPropTree("Queue",t);
                }
                serverstatus->commitProperties();
                engine->setDefaultTransferBufferSize((size32_t)globals->getPropInt("@transferBufferSize"));
                engine->startListener(subq.str(),serverstatus);
            }
            if (!comma)
                break;
            q = comma+1;
            if (!*q)
                break;
        }
        q = globals->queryProp("@MONITORQUEUE");
        if (q&&*q) {
            if (stop) {
                stopDFUserver(q);
            }
            else {
                IPropertyTree *t=serverstatus->queryProperties()->addPropTree("MonitorQueue",createPTree(false));
                t->setProp("@name",q);
                engine->startMonitor(q,serverstatus,globals->getPropInt("@MONITORINTERVAL",60)*1000);
            }
        }
        q = globals->queryProp("@REPLICATEQUEUE");
        if (q&&*q) {
            if (stop) {
                // TBD?
            }
            else {
                replserver.setown(createReplicateServer(q));
                replserver->runServer();
            }
        }
        if (!stop) {
            serverstatus->commitProperties();

            writeSentinelFile(sentinelFile);

            engine->joinListeners();
            if (replserver.get())
                replserver->stopServer();
            LOG(MCprogress, unknownJob, "Exiting");
        }

    }
    catch(IException *e){
        EXCLOG(e, "DFU Server Exception: ");
        e->Release();
    }
    catch (const char *s) {
        WARNLOG("DFU: %s",s);
    }

    delete serverstatus;
    if (stop)
        Sleep(2000);    // give time to stop
    engine.clear();
    globals.clear();
    closeEnvironment();
    closedownClientProcess();
    UseSysLogForOperatorMessages(false);
    setDaliServixSocketCaching(false);
    releaseAtoms();
    return 0;
}

