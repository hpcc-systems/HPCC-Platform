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

#pragma warning (disable : 4786)
#if defined(_WIN32) && defined(_DEBUG)
//#include <vld.h>
#endif
//Jlib
#include "jliball.hpp"
#include "jstats.h"

//CRT / OS
#ifndef _WIN32
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <sys/time.h>
#endif


//SCM Interfaces
#include "esp.hpp"

// We can use "work" as the first parameter when debugging.
#define ESP_SINGLE_PROCESS

//ESP Core
#include "espp.hpp"
#include "espcfg.ipp"
#include "esplog.hpp"
#include "espcontext.hpp"
#include "build-config.h"

#ifdef _WIN32
/*******************************************
  _WIN32
 *******************************************/

#ifdef ESP_SINGLE_PROCESS

int start_init_main(int argc, char** argv, int (*init_main_func)(int,char**))
{
    return init_main_func(argc, argv);
}

#define START_WORK_MAIN(config, server, result)  result = work_main((config), (server));
#define SET_ESP_SIGNAL_HANDLER(sig, handler)
#define RESET_ESP_SIGNAL_HANDLER(sig, handler)

#else //!ESP_SINGLE_PROCESS

#define SET_ESP_SIGNAL_HANDLER(sig, handler)
#define RESET_ESP_SIGNAL_HANDLER(sig, handler)

int start_init_main(int argc, char** argv, int (*init_main_func)(int, char**))
{ 
    if(argc > 1 && !strcmp(argv[1], "work")) 
    { 
        char** newargv = new char*[argc - 1]; 
        newargv[0] = argv[0]; 
        for(int i = 2; i < argc; i++) 
        { 
            newargv[i - 1] = argv[i]; 
        } 
        int rtcode = init_main_func(argc - 1, newargv); 
        delete[] newargv;
        return rtcode;
    } 
    else 
    { 
        StringBuffer command; 
        command.append(argv[0]); 
        command.append(" work "); 
        for(int i = 1; i < argc; i++) 
        { 
            command.append(argv[i]); 
            command.append(" "); 
        } 
        DWORD exitcode = 0; 
        while(true) 
        { 
            PROGLOG("Starting working process: %s", command.str()); 
            PROCESS_INFORMATION process; 
            STARTUPINFO si; 
            GetStartupInfo(&si); 
            if(!CreateProcess(NULL, (char*)command.str(), NULL, NULL, FALSE, 0, NULL, NULL, &si, &process)) 
            { 
                ERRLOG("Process failed: %d\r\n",GetLastError()); 
                exit(-1); 
            } 
            WaitForSingleObject(process.hProcess,INFINITE); 
            GetExitCodeProcess(process.hProcess, &exitcode); 
            PROGLOG("Working process exited, exitcode=%d", exitcode); 
            if(exitcode == TERMINATE_EXITCODE) 
            { 
                DBGLOG("This is telling the monitoring process to exit too. Exiting once and for all...."); 
                exit(exitcode); 
            } 
            CloseHandle(process.hProcess); 
            CloseHandle(process.hThread); 
            Sleep(1000);
        } 
    }
}

#define START_WORK_MAIN(config, server, result)  result = work_main((config), (server));
#endif //!ESP_SINGLE_PROCESS

#else

/*****************************************************
     LINUX
 ****************************************************/
#define SET_ESP_SIGNAL_HANDLER(sig, handler) signal(sig, handler)
#define RESET_ESP_SIGNAL_HANDLER(sig, handler) signal(sig, handler)

int start_init_main(int argc, char** argv, int (*init_main_func)(int,char**))
{
    return init_main_func(argc, argv);
}

int work_main(CEspConfig& config, CEspServer& server);
int do_work_main(CEspConfig& config, CEspServer& server)
{
   int result; 
   int numchildren = 0; 
   pid_t childpid=0; 

createworker: 
   childpid = fork(); 
   if(childpid < 0) 
   { 
      ERRLOG("Unable to create new process"); 
      result = -1; 
   } 
   else if(childpid == 0) 
   {
        result = work_main(config, server);
   } 
   else 
   { 
      DBGLOG("New process generated, pid=%d", childpid); 
      numchildren++; 
      if(numchildren < MAX_CHILDREN) 
         goto createworker; 
      
      int status; 
      childpid = wait3(&status, 0, NULL); 
      DBGLOG("Attention: child process exited, pid = %d", childpid); 
      numchildren--; 
      DBGLOG("Bringing up a new process..."); 
      sleep(1); 
      goto createworker; 
   }

   return result;
}

#define START_WORK_MAIN(config, server, result) result = do_work_main(config, server)

#endif

void brokenpipe_handler(int sig)
{
   //Reset the signal first
   RESET_ESP_SIGNAL_HANDLER(SIGPIPE, brokenpipe_handler);

   DBGLOG("Broken Pipe - remote side closed the socket");
}


int work_main(CEspConfig& config, CEspServer& server)
{
    server.start();
    DBGLOG("ESP server started.");
    server.waitForExit(config);
    server.stop(true);
    config.stopping();
    config.clear();
    return 0;
}


void openEspLogFile(IPropertyTree* envpt, IPropertyTree* procpt)
{
    StringBuffer logdir;
    if(procpt->hasProp("@name"))
    {
        StringBuffer espNameStr;
        procpt->getProp("@name", espNameStr);
        if (!getConfigurationDirectory(envpt->queryPropTree("Software/Directories"), "log", "esp", espNameStr.str(), logdir))
        {
            logdir.clear();
        }
    }
    if(logdir.length() == 0)
    {
        if(procpt->hasProp("@logDir"))
            procpt->getProp("@logDir", logdir);
    }


    Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(logdir.str(), "esp");
    lf->setName("esp_main");//override default filename
    lf->setAliasName("esp");
    lf->beginLogging();

    if (procpt->getPropBool("@enableSysLog", false))
        UseSysLogForOperatorMessages();
}   

static void usage()
{
    puts("ESP - Enterprise Service Platform server. (C) 2001-2011, HPCC Systems®.");
    puts("Usage:");
    puts("  esp [options]");
    puts("Options:");
    puts("  -?/-h: show this help page");
    puts("  interactive: start in interactive mode (pop up error dialog when exception occurs)");
    puts("  config=<file>: specify the config file name [default: esp.xml]");
    puts("  process=<name>: specify the process name in the config [default: the 1st process]");
    exit(1);
}

bool ensureProcessSession(IPropertyTree* sessionTree, const char* name)
{
    if (!name || !*name)
        return false;

    if (!sessionTree)
        return false;

    VStringBuffer xpath("Process[@name=\"%s\"]", name);
    if (sessionTree->queryBranch(xpath.str()))
        return true;

    Owned<IPropertyTree> processSessionTree = createPTree();
    processSessionTree->addProp("@name", name);
    sessionTree->addPropTree("Process", LINK(processSessionTree));
    return true;
}

bool ensureSDSProcessSession(const char* name)
{
    if (!name || !*name)
        return false;

    Owned<IRemoteConnection> conn = querySDS().connect(SESSION_ROOT_PATH, myProcessSession(), RTM_LOCK_WRITE, SESSION_SDS_LOCK_TIMEOUT);
    if (!conn)
    {
        conn.setown(querySDS().connect("/", myProcessSession(), RTM_LOCK_WRITE, SESSION_SDS_LOCK_TIMEOUT));
        if (!conn)
            throw MakeStringException(-1, "Failed to connect SDS.");

        IPropertyTree* sdsRoot = conn->queryRoot();
        if (!sdsRoot)
            throw MakeStringException(-1, "Failed to get SDS.");

        Owned<IPropertyTree> sessionTree = createPTree();
        if (!ensureProcessSession(sessionTree, name))
            throw MakeStringException(-1, "Failed to add ProcessSession.");

        sdsRoot->addPropTree(SESSION_ROOT_PATH, LINK(sessionTree));
    }
    else
    {
        IPropertyTree* sessionTree = conn->queryRoot();
        if (!sessionTree)
            throw MakeStringException(-1, "Failed to get SDS session tree.");

        if (!ensureProcessSession(sessionTree, name))
            throw MakeStringException(-1, "Failed to add ProcessSession.");
    }

    conn->commit();
    conn->close(false);
    return true;
}

int init_main(int argc, char* argv[])
{
    InitModuleObjects();

    Owned<IProperties> inputs = createProperties(true);

    bool interactive = false;

    for (int i = 1; i < argc; i++)
    {
        if (stricmp(argv[i], "-?")==0 || stricmp(argv[i], "-h")==0 || stricmp(argv[i], "-help")==0
             || stricmp(argv[i], "/?")==0 || stricmp(argv[i], "/h")==0)
             usage();
        else if(stricmp(argv[i], "interactive") == 0)
            interactive = true;
        else if (strchr(argv[i],'='))
        {
            inputs->loadProp(argv[i]);
        }
        else
        {
            fprintf(stderr, "Unknown option: %s", argv[i]);
            return 0;
        }
    }

    int result = -1;

#ifdef _WIN32 
    if (!interactive)
        ::SetErrorMode(SEM_NOGPFAULTERRORBOX|SEM_FAILCRITICALERRORS);
#endif

    SET_ESP_SIGNAL_HANDLER(SIGPIPE, brokenpipe_handler);

    bool SEHMappingEnabled = false;

    CEspAbortHandler abortHandler;

    Owned<IFile> sentinelFile = createSentinelTarget();
    removeSentinelFile(sentinelFile);

    Owned<CEspConfig> config;
    Owned<CEspServer> server;
    StringBuffer processName;
    try
    {
        const char* cfgfile = NULL;
        const char* procname = NULL;
        if(inputs.get())
        {
            if(inputs->hasProp("config"))
                cfgfile = inputs->queryProp("config");
            if(inputs->hasProp("process"))
                procname = inputs->queryProp("process");
        }
        if(!cfgfile || !*cfgfile)
            cfgfile = "esp.xml";

        Owned<IPropertyTree> envpt= createPTreeFromXMLFile(cfgfile, ipt_caseInsensitive);
        Owned<IPropertyTree> procpt = NULL;
        if (envpt)
        {
            envpt->addProp("@config", cfgfile);
            StringBuffer xpath;
            if (procname==NULL || strcmp(procname, ".")==0)
                xpath.appendf("Software/EspProcess[1]");
            else
                xpath.appendf("Software/EspProcess[@name=\"%s\"]", procname);

            DBGLOG("Using ESP configuration section [%s]", xpath.str());
            procpt.set(envpt->queryPropTree(xpath.str()));
            if (!procpt)
                throw MakeStringException(-1, "Config section [%s] not found", xpath.str());
        }
        else
            throw MakeStringException(-1, "Failed to load config file %s", cfgfile);

        const char* build_ver = BUILD_TAG;
        setBuildVersion(build_ver);

        const char* build_level = BUILD_LEVEL;
        setBuildLevel(build_level);

        processName = procpt->queryProp("@name");
        setStatisticsComponentName(SCTesp, processName.str(), true);

        openEspLogFile(envpt.get(), procpt.get());

        DBGLOG("Esp starting %s", BUILD_TAG);

        StringBuffer componentfilesDir;
        if(procpt->hasProp("@componentfilesDir"))
            procpt->getProp("@componentfilesDir", componentfilesDir);
        if(componentfilesDir.length() > 0 && strcmp(componentfilesDir.str(), ".") != 0)
        {
            setCFD(componentfilesDir.str());
            DBGLOG("componentfiles are under %s", getCFD());
        }

        StringBuffer sehsetting;
        procpt->getProp("@enableSEHMapping", sehsetting);
        if(!interactive && sehsetting.length() > 0 && (stricmp(sehsetting.str(), "true") == 0 || stricmp(sehsetting.str(), "1") == 0))
            SEHMappingEnabled = true;
        if(SEHMappingEnabled)
            EnableSEHtoExceptionMapping();

        CEspConfig* cfg = new CEspConfig(inputs.getLink(), envpt.getLink(), procpt.getLink(), false);
        if(cfg && cfg->isValid())
        {
            config.setown(cfg);
            abortHandler.setConfig(cfg);
        }
    }
    catch(IException* e)
    {
        StringBuffer description;
        ERRLOG("ESP Unhandled IException (%d -- %s)", e->errorCode(), e->errorMessage(description).str());
        e->Release();
        return -1;
    }
    catch (...)
    {
        ERRLOG("ESP Unhandled General Exception.");
        return -1;
    }

    if (config && config->isValid())
    {
        PROGLOG("Configuring Esp Platform...");

        try
        {
            CEspServer *srv = new CEspServer(config);
            if(SEHMappingEnabled)
                srv->setSavedSEHHandler(SEHMappingEnabled);
            server.setown(srv);
            abortHandler.setServer(srv);
            setEspContainer(server.get());

            ensureSDSProcessSession(processName.str());

            config->loadAll();
            config->bindServer(*server.get(), *server.get()); 
            
        }
        catch(IException* e)
        {
            StringBuffer description;
            ERRLOG("ESP Unhandled IException (%d -- %s)", e->errorCode(), e->errorMessage(description).str());
            e->Release();
            return -1;
        }
        catch (...)
        {
            ERRLOG("ESP Unhandled General Exception.");
            return -1;
        }

        writeSentinelFile(sentinelFile);
        result = work_main(*config, *server.get());
    }
    else
    {
        ERRLOG("!!! Unable to load ESP configuration.");
    }
    
    return result;
}

//command line arguments:
// [pre] if "work", special init behavior, but removed before init_main  
// [1] process name
// [2] config location - local file name or dali address
// [3] config location type - "dali" or ""

int main(int argc, char* argv[])
{
    start_init_main(argc, argv, init_main);
    stopPerformanceMonitor();
    UseSysLogForOperatorMessages(false);
    releaseAtoms();
    return 0;
}

