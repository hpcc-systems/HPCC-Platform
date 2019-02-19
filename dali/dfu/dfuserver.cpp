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
    IPropertyTree *ret = createPTree("DFUSERVER", ipt_caseInsensitive);
    ret->setProp("@name","mydfuserver");
    ret->addPropTree("SSH",createPTree("SSH", ipt_caseInsensitive));
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
    for (unsigned i=0;i<(unsigned)argc;i++) {
        if (streq(argv[i],"--daemon") || streq(argv[i],"-d")) {
            if (daemon(1,0) || write_pidfile(argv[++i])) {
                perror("Failed to daemonize");
                return EXIT_FAILURE;
            }
            break;
        }
    }
    InitModuleObjects();
    EnableSEHtoExceptionMapping();

    NoQuickEditSection xxx;

    Owned<IFile> file = createIFile("dfuserver.xml");
    if (file->exists())
        globals.setown(createPTreeFromXMLFile("dfuserver.xml", ipt_caseInsensitive));
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
        sentinelFile.setown(createSentinelTarget());
        removeSentinelFile(sentinelFile);
        Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(globals, "dfuserver");
        lf->setMaxDetail(1000);
        fileMsgHandler = lf->beginLogging();
    }
    StringBuffer ftslogdir;
    const char* name = globals->queryProp("@name");
    if (getConfigurationDirectory(globals->queryPropTree("Directories"),"log","ftslave",name,ftslogdir)) // NB instance deliberately dfuserver's
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
            engine->setDFUServerName(name);
            addAbortHandler(exitDFUserver);
        }
        const char *q = queue.str();
        for (;;) {
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
                    t = createPTree();
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
                IPropertyTree *t=serverstatus->queryProperties()->addPropTree("MonitorQueue",createPTree());
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

            enableForceRemoteReads(); // forces file reads to be remote reads if they match environment setting 'forceRemotePattern' pattern.

            engine->joinListeners();
            if (replserver.get())
                replserver->stopServer();
            LOG(MCuserProgress, unknownJob, "Exiting");
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

