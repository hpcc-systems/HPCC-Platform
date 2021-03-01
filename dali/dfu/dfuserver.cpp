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
#include "environment.hpp"
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
#include "daqueue.hpp"

ILogMsgHandler * fileMsgHandler;
Owned<IDFUengine> engine;

#define DEFAULT_PERF_REPORT_DELAY 60

//============================================================================

void usage()
{
    printf("Usage:\n");
    printf("  dfuserver --daliServers=<ip> --queue=<q-name>          -- starts DFU Server\n\n");
    printf("  dfuserver --daliServers=<ip> --queue=<q-name> --stop=1 -- stops DFU Server\n\n");
}



static bool exitDFUserver()
{
    engine->abortListeners();
    return false;
}

#ifndef _CONTAINERIZED
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
#endif

static constexpr const char * defaultYaml = R"!!(
version: "1.0"
dfuserver:
  name: dfuserver
  monitorInterval: 900
  maxJobs: 1
)!!";

int main(int argc, const char *argv[])
{
    if (!checkCreateDaemon(argc, argv))
        return EXIT_FAILURE;

    InitModuleObjects();
    EnableSEHtoExceptionMapping();

    NoQuickEditSection xxx;

    Owned<IPropertyTree> globals;
    try
    {
        globals.setown(loadConfiguration(defaultYaml, argv, "dfuserver", "DFUSERVER", "dfuserver.xml", nullptr));
    }
    catch (IException * e)
    {
        OERRLOG(e);
        e->Release();
        return 1;
    }
    catch(...)
    {
        OERRLOG("Failed to load configuration");
        return 1;
    }

    StringBuffer name, daliServers;
    if (!globals->getProp("@daliServers", daliServers))
    {
        PROGLOG("daliServers not specified in configuration");
        return 1;
    }
    if (!globals->getProp("@name", name))
    {
        PROGLOG("name not specified in configuration");
        return 1;
    }
    Owned<IFile> sentinelFile;
#ifndef _CONTAINERIZED
    bool stop = globals->getPropBool("@stop", false);
#else
    bool stop = false; // In containerized, this is never true: queues stopped when container stopped
#endif
    if (!stop) {
        sentinelFile.setown(createSentinelTarget());
        removeSentinelFile(sentinelFile);
    }
#ifdef _CONTAINERIZED
    setupContainerizedLogMsgHandler();
#else
    if (!stop)
    {
        Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(globals, "dfuserver");
        lf->setMaxDetail(1000);
        fileMsgHandler = lf->beginLogging();
    }
    StringBuffer ftslogdir;
    if (getConfigurationDirectory(globals->queryPropTree("Directories"),"log","ftslave",name,ftslogdir)) // NB instance deliberately dfuserver's
        setFtSlaveLogDir(ftslogdir.str());
    setRemoteSpawnSSH(
        globals->queryProp("SSH/@SSHidentityfile"),
        globals->queryProp("SSH/@SSHusername"),
        globals->queryProp("SSH/@SSHpassword"),
        globals->getPropInt("SSH/@SSHtimeout",0),
        globals->getPropInt("SSH/@SSHretries",3),
        "run_");
#endif
    CSDSServerStatus *serverstatus=NULL;
    Owned<IReplicateServer> replserver;
    try {
        Owned<IGroup> serverGroup = createIGroupRetry(daliServers,DALI_SERVER_PORT);
        initClientProcess(serverGroup, DCR_DfuServer, 0, NULL, NULL, stop?(1000*30):MP_WAIT_FOREVER);

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

            IPropertyTree * config = nullptr;
            installDefaultFileHooks(config);
        }
        StringBuffer queue, monitorQueue;
#ifndef _CONTAINERIZED
        if (!globals->getProp("@queue", queue))
        {
            PROGLOG("queue not specified in configuration");
            return 1;
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
        globals->getProp("@monitorQueue", monitorQueue);
#else
        getDfuQueueName(queue, name);
        unsigned maxJobs = globals->getPropInt("@maxJobs", 1);
        if (maxJobs<1)
        {
            OERRLOG("maxJobs is %u", maxJobs);
            return 1;
        }
        IPropertyTree *t = createPTree();
        t->setProp("@name",queue);
        t->setPropInt("@num", maxJobs);
        serverstatus->queryProperties()->addPropTree("Queue",t);
        serverstatus->commitProperties();
        engine->setDefaultTransferBufferSize((size32_t)globals->getPropInt("@transferBufferSize"));
        for (unsigned i=0; i<maxJobs; i++)
            engine->startListener(queue,serverstatus);
        getDfuMonitorQueueName(monitorQueue, name);
#endif
        if (monitorQueue.length()>0)
        {
            if (stop) {
                stopDFUserver(monitorQueue);
            }
            else {
                IPropertyTree *t=serverstatus->queryProperties()->addPropTree("monitorQueue",createPTree());
                t->setProp("@name",monitorQueue);
                int monitorInterval = globals->getPropInt("@monitorInterval", 60);
                engine->startMonitor(monitorQueue,serverstatus,monitorInterval*1000);
            }
        }
        const char *replicateQueue = globals->queryProp("@replicateQueue");
        if (!isEmptyString(replicateQueue))
        {
            if (stop) {
                // TBD?
            }
            else {
                replserver.setown(createReplicateServer(replicateQueue));
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
            LOG(MCprogress, unknownJob, "Exiting");
        }

    }
    catch(IException *e){
        EXCLOG(e, "DFU Server Exception: ");
        e->Release();
    }
    catch (const char *s) {
        OWARNLOG("DFU: %s",s);
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

