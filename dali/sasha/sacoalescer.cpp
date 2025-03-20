#include "platform.h"
#include "jlib.hpp"
#include "jlog.ipp"
#include "jptree.hpp"
#include "jmisc.hpp"

#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "dasds.hpp"
#include "saserver.hpp"
#include "sautil.hpp"
#include "dalienv.hpp"

#define DEFAULT_INTERVAL                1       // 1 hour
#define RESTART_DELAY                   60      // seconds
#define DEFAULT_MINDELTASIZE            50000   // 50MB

CriticalSection *suspendResumeCrit;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    suspendResumeCrit = new CriticalSection;
    return true;
}
MODULE_EXIT()
{
    delete suspendResumeCrit;
}


void coalesceDatastore(IPropertyTree *coalesceProps, bool force)
{
    try
    {
        StringBuffer dataPath, backupPath;
        IPropertyTree &confProps = querySDS().queryProperties();
        confProps.getProp("@dataPathUrl", dataPath); 
        confProps.getProp("@backupPathUrl", backupPath); 
        unsigned keepStores = confProps.getPropInt("@keepStores");
        if (0 == dataPath.length())
        {
            OERRLOG("COALESCER: No dali data path found.");
            return;
        }
        
        if (backupPath.length())
            addPathSepChar(backupPath);
        addPathSepChar(dataPath);

        offset_t minDeltaSize = force?0:coalesceProps->getPropInt64("@minDeltaSize", DEFAULT_MINDELTASIZE);

        for (;;) {
            PROGLOG("COALESCER: dataPath=%s, backupPath=%s, minDeltaSize = %" I64F "dK", dataPath.str(), backupPath.str(), (unsigned __int64) minDeltaSize);
            unsigned configFlags = SH_External|SH_CheckNewDelta;
            configFlags |= coalesceProps->getPropBool("@recoverFromIncErrors", false) ? SH_RecoverFromIncErrors : 0;
            configFlags |= coalesceProps->getPropBool("@backupErrorFiles", true) ? SH_BackupErrorFiles : 0;
            bool stopped;
            Owned<IStoreHelper> iStoreHelper = createStoreHelper(NULL, dataPath, backupPath.str(), configFlags, keepStores, 5000, &stopped);
            unsigned baseEdition = iStoreHelper->queryCurrentEdition();

            if (minDeltaSize) 
            {
                StringBuffer detachPath;
                detachPath.append(dataPath);
                iStoreHelper->getDetachedDeltaName(detachPath);
                OwnedIFile detachedIFile = createIFile(detachPath.str());
                if (!detachedIFile->exists())
                {
                    StringBuffer deltaFilename(dataPath);
                    iStoreHelper->getCurrentDeltaFilename(deltaFilename);
                    OwnedIFile deltaIFile = createIFile(deltaFilename.str());
                    if (!deltaIFile->exists())
                        break;
                    offset_t dsz = deltaIFile->size();
                    if (minDeltaSize > dsz/1024)
                    {
                        PROGLOG("COALESCER: Delta size %" I64F "u less than minimum (%" I64F "u K), exiting", dsz, minDeltaSize);
                        break;
                    }
                }
            }


            StringBuffer storeFilename(dataPath);   
            iStoreHelper->getCurrentStoreFilename(storeFilename);
            StringBuffer memStr;
            getSystemTraceInfo(memStr.clear());
            MLOG("COALESCE: %s", memStr.str());
            Owned<IPropertyTree> _root;
            OwnedIFile storeIFile = createIFile(storeFilename.str());
            if (storeIFile->exists())
            {
                PROGLOG("Loading store: %s, size=%" I64F "d", storeFilename.str(), storeIFile->size());
                _root.setown(createPTreeFromXMLFile(storeFilename.str()));
                PROGLOG("Loaded: %s", storeFilename.str());
            }
            else
            {
                if (baseEdition==0) {
                    PROGLOG("Creating base store");
                    _root.setown(createPTree("SDS"));
                }
                else {
                    OERRLOG("Base store %d not found, exiting",baseEdition);
                    break; // don't think much point continuing is there?
                }
            }
            IPropertyTree *root = _root.get();
            getSystemTraceInfo(memStr.clear());
            MLOG("COALESCE: %s", memStr.str());

            PROGLOG("COALESCER: coalesce started");
            if (baseEdition != iStoreHelper->queryCurrentEdition())
            {
                PROGLOG("COALESCER: Store has changed by another process prior to coalesce, reloading changed store.");
                Sleep(1000*60);
                continue;
            }

            StringBuffer detachName;
            iStoreHelper->getDetachedDeltaName(detachName);
            StringBuffer detachPath(std::move(dataPath));
            detachPath.append(detachName);
            OwnedIFile detachedIFile = createIFile(detachPath.str());
            if (detachedIFile->exists() || iStoreHelper->detachCurrentDelta())
            {
                PROGLOG("COALESCER: Loading delta: %s, size=%" I64F "d", detachName.str(), detachedIFile->size());
                bool noError;
                Owned<IException> deltaE;
                try { noError = iStoreHelper->loadDelta(detachName.str(), detachedIFile, root); }
                catch (IException *e) { deltaE.setown(e); noError = false; }
                if (!noError && 0 != (SH_BackupErrorFiles & configFlags))
                {
                    iStoreHelper->backup(detachPath.str());
                    iStoreHelper->backup(storeFilename.str());
                    if (deltaE.get())
                        throw LINK(deltaE);
                }
                iStoreHelper->saveStore(root, &baseEdition);
            }
            PROGLOG("COALESCER: coalesce complete");

            if (coalesceProps->getPropBool("@leakStore", true))
            {
                _root->Link();
                enableMemLeakChecking(false);
            }
            else
            {
                getSystemTraceInfo(memStr.clear());
                MLOG("COALESCE: %s", memStr.str());
                PROGLOG("Clearing old store...");
                _root.clear();
                PROGLOG("old store cleared");
                getSystemTraceInfo(memStr.clear());
                MLOG("COALESCE: %s", memStr.str());
            }
            break;
        }
    }
    catch (IException *e)
    {
        LOG(MCoperatorError, e, "COALESCER: Unexpected exception, coalesce component halted");
        throw;
    }
}


class CSashaSDSCoalescingServer: public ISashaServer, public Thread
{
    std::atomic<bool> stopped;
    Semaphore stopsem;
    CriticalSection suspendResumeCrit;
    Linked<IPropertyTree> coalesceProps;
public:
    IMPLEMENT_IINTERFACE_USING(Thread);

    CSashaSDSCoalescingServer(IPropertyTree *_config)
        : Thread("CSashaSDSCoalescingServer"), coalesceProps(_config)
    {
        stopped.store(false);
    }

    ~CSashaSDSCoalescingServer()
    {
    }

    void start()
    {
        CriticalBlock b(suspendResumeCrit);
        stopped.store(false);
        Thread::start(false);
    }

    void ready()
    {
    }
    
    void stop()
    {
        CriticalBlock b(suspendResumeCrit);
        if (!stopped.load()) {
            stopped.store(true);
            stopsem.signal();
        }
        join();
    }

    void suspend() // stops and resets coalescer (free up memory)
    {
        CriticalBlock b(suspendResumeCrit);
        if (stopped.load()) return;
        PROGLOG("COALESCER: suspend");
        stopped.store(true);
        stopsem.signal();
        join();
    }

    void resume()
    {
        CriticalBlock b(suspendResumeCrit);
        if (!stopped.load()) return;
        PROGLOG("COALESCER: resume");
        stopped.store(false);
        Thread::start(false);
        ready();
    }

    int run()
    {
        do
        {
            unsigned interval = coalesceProps->getPropInt("@interval",DEFAULT_INTERVAL);
            if (!interval)
            {
                OERRLOG("COALESCER: disabled");
                return 0;
            }
            PROGLOG("COALESCER: min interval = %d hr", interval);
            CSashaSchedule schedule;
            schedule.init(coalesceProps,DEFAULT_INTERVAL,DEFAULT_INTERVAL/2);
            stopsem.wait(1000*60*60*DEFAULT_INTERVAL/2); // wait a bit til dali has started
            while (!stopped.load())
            {
                stopsem.wait(1000*60);
                if (!stopped.load() && schedule.ready())  {
                    CSuspendAutoStop suspendstop;
                    DWORD runcode;
                    HANDLE h;
                    StringBuffer cmd(sashaProgramName);
                    cmd.append(" --coalesce");
#ifdef _CONTAINERIZED
                    cmd.append(" --config=").append(coalesceProps->queryProp("@config"));
                    cmd.append(" --daliServers=").append(coalesceProps->queryProp("@daliServers"));
#endif                    
                    char cwd[1024];
                    if (GetCurrentDirectory(1024, cwd))
                        PROGLOG("COALESCE: Running '%s' in '%s'",cmd.str(),cwd);
                    else
                        OERRLOG("COALESCE: Running '%s' in unknown current directory",cmd.str());
                    if (!invoke_program(cmd.str(), runcode, false, NULL, &h)) 
                        OERRLOG("Could not run saserver in coalesce mode");
                    else {
                        PROGLOG("COALESCE: started pid = %d",(int)h);
                        while (!wait_program(h,runcode,false)) {
                            stopsem.wait(1000*60);
                            if (stopped.load()) {
                                interrupt_program(h, false);
                                break;
                            }
                            PROGLOG("COALESCE running");
                        }
                        if (stopped.load())
                            PROGLOG("COALESCE stopped");
                        else
                            PROGLOG("COALESCE returned %d",(int)runcode);
                    }
                }
            }

        }
        while (!stopped.load());
        return 0;
    }
} *sashaSDSCoalescingServer = NULL;

ISashaServer *createSashaSDSCoalescingServer()
{
    assertex(!sashaSDSCoalescingServer); // initialization problem
#ifdef _CONTAINERIZED
    Linked<IPropertyTree> config = serverConfig;
#else
    Owned<IPropertyTree> config = serverConfig->getPropTree("Coalescer");
    if (!config)
        config.setown(createPTree("Coalescer"));
#endif
    sashaSDSCoalescingServer = new CSashaSDSCoalescingServer(config);
    return sashaSDSCoalescingServer;
}

void suspendCoalescingServer()
{
    if (!sashaSDSCoalescingServer) return;
    sashaSDSCoalescingServer->suspend();
}

void resumeCoalescingServer()
{
    if (!sashaSDSCoalescingServer) return;
    sashaSDSCoalescingServer->resume();
}

