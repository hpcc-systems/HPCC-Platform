// TBD check min time from when *finished*

#include "platform.h"

#include "jlib.hpp"
#include "jiface.hpp"
// #include "jstring.hpp"
#include "jptree.hpp"
// #include "jmisc.hpp"
#include "jregexp.hpp"
// #include "jset.hpp"

// #include "mpbase.hpp"
// #include "mpcomm.hpp"
// #include "daclient.hpp"
#include "dadfs.hpp"
// #include "dautils.hpp"
// #include "dasds.hpp"
#include "dalienv.hpp"
// #include "rmtfile.hpp"

#include "saserver.hpp"
#include "sautil.hpp"
// #include "sacoalescer.hpp"
// #include "sacmd.hpp"

#define DEFAULT_MAXDIRTHREADS 500
#define DEFAULT_MAXMEMORY 4096

#define SDS_CONNECT_TIMEOUT (1000 * 60 * 60 * 2) // better than infinite
#define SDS_LOCK_TIMEOUT 300000

#define DEFAULT_EXPIRY_INTERVAL 0 // TODO:24 // hours

#define DEFAULT_EXPIRYDAYS 0 // TODO:7

#define LOGDBGHK "DEBUGHOUSEKEEPING: "

#define DEFAULT_RECENT_CUTOFF_DAYS 0 // TODO:1

// Debug Housekeeping monitor

class CSashaDebugHousekeepingServer : public ISashaServer, public Thread
{
    bool stopped;
    Semaphore stopsem;
    Mutex runmutex;
    Owned<IUserDescriptor> udesc;
    Linked<IPropertyTree> props;

public:
    IMPLEMENT_IINTERFACE_USING(Thread);

    CSashaDebugHousekeepingServer(IPropertyTree *_config)
        : Thread("CSashaDebugHousekeepingServer"), props(_config)
    {
        stopped = false;

        StringBuffer userName;
#ifdef _CONTAINERIZED
        props->getProp("@user", userName);
#else
        serverConfig->getProp("@sashaUser", userName);
#endif
        udesc.setown(createUserDescriptor());
        udesc->set(userName.str(), nullptr);
    }

    ~CSashaDebugHousekeepingServer()
    {
    }

    void start()
    {
        Thread::start(false);
    }

    void ready()
    {
    }

    void stop()
    {
        if (!stopped)
        {
            stopped = true;
            stopsem.signal();
        }
        synchronized block(runmutex); // hopefully stopped should stop
        if (!join(1000 * 60 * 3))
            OERRLOG("CSashaDebugHousekeepingServer aborted");
    }

    void runDebugHousekeeping()
    {
        synchronized block(runmutex);
        if (stopped)
            return;
        PROGLOG(LOGDBGHK "Started");
        unsigned defaultExpireDays = props->getPropInt("@expiryDefault", DEFAULT_EXPIRYDAYS);

        // get debug plane dir
        StringBuffer debugDir;
#ifdef _CONTAINERIZED
        StringBuffer planeName;
        if (!getDefaultPlane(planeName, "@debugPlane", "debug"))
        {
            WARNLOG("Exception handlers configured, but debug plane is missing");
            return;
        }
        Owned<IPropertyTree> plane = getStoragePlane(planeName);
        assertex(plane);
        verifyex(plane->getProp("@prefix", debugDir));
#else
        verifyex(getConfigurationDirectory(nullptr, "temp", nullptr, "debug", debugDir));
#endif

/*
        Owned<IPropertyTree> globals;
        StringBuffer debugDir;
        if (!getConfigurationDirectory(globals->queryPropTree("Directories"), "debug", "thor", globals->queryProp("@name"), debugDir))
        {
            if (!isContainerized())
            {
                appendCurrentDirectory(debugDir, false);
                addPathSepChar(debugDir);
                debugDir.append("debuginfo"); // use ./debuginfo in non-containerized mode
            }
            else
            {
                IWARNLOG("Failed to get debug directory");
                return;
            }
        }
        addPathSepChar(debugDir);
*/

        // iterate debug plane selecting post-mortem directories for housekeeping
        StringArray expiryFolderlist;
        Owned<IDirectoryIterator> pDirIter = createDirectoryIterator(debugDir.str(), "*", false, true);
        addPathSepChar(debugDir);
        ForEach(*pDirIter)
        {
            if (stopped)
                break;

            IFile& iFile = pDirIter->query();
            const char* dir = iFile.queryFilename();
    
            if (!dir || !*dir)
                continue;
    
            // Process directories, but not the "." and ".." directories
            if (iFile.isDirectory()==fileBool::foundYes && *dir != '.' && isPostMortemDir(dir) && isExpiredDir(dir, defaultExpireDays))
            {
                StringBuffer path(debugDir);
                path.append(dir);

                expiryFolderlist.append(path);
            }
        }
        pDirIter.clear();

        ForEachItemIn(i, expiryFolderlist)
        {
            if (stopped)
                break;
            const char *lfn = expiryFolderlist.item(i);
            PROGLOG(LOGDBGHK "Deleting %s", lfn);
            try
            {
                /* NB: 0 timeout, meaning fail and skip, if there is any locking contention.
                 * If the file is locked, it implies it is being accessed.
                 */
                queryDistributedFileDirectory().removeEntry(lfn, udesc, NULL, 0, true);
                PROGLOG(LOGDBGHK "Deleted %s", lfn);
            }
            catch (IException *e) // may want to just detach if fails
            {
                EXCLOG(e, LOGDBGHK "remove");
                e->Release();
            }
        }

        PROGLOG(LOGDBGHK "%s", stopped ? "Stopped" : "Done");
    }

    int run()
    {
        unsigned interval = props->getPropInt("@interval", DEFAULT_EXPIRY_INTERVAL);
        if (!interval)
            stopped = true;
        PROGLOG(LOGDBGHK "min interval = %d hr", interval);
        unsigned initinterval = (interval - 1) / 2; // wait a bit til dali has started
        CSashaSchedule schedule;
        if (interval)
            schedule.init(props, interval, initinterval);
        initinterval *= 60 * 60 * 1000; // ms
        unsigned started = msTick();
        while (!stopped)
        {
            stopsem.wait(1000 * 60);
            if (stopped)
                break;
            if (!interval || ((started != (unsigned)-1) && (msTick() - started < initinterval)))
                continue;
            started = (unsigned)-1;
            if (!schedule.ready())
                continue;
            try
            {
                runDebugHousekeeping();
            }
            catch (IException *e)
            {
                StringBuffer s;
                EXCLOG(e, LOGDBGHK);
                e->Release();
            }
        }
        PROGLOG(LOGDBGHK "Exit");
        return 0;
    }

private:
    bool isPostMortemDir(const char *dir)
    {
        // Expecting a directory name like "W20250225-101112"
        RegExpr RE("^W[0-9]{8}-[0-9]{6}$");
        if (RE.find(dir))
        {
            PROGLOG(LOGDBGHK "Post-mortem dir: %s", dir);

            return true;
        }
        else
        {
            PROGLOG(LOGDBGHK "Non-post-mortem dir: %s", dir);

            return false;
        }
    }

    bool isExpiredDir(const char *dir, const unsigned &defaultExpireDays)
    {
        // Directory name is like "W20250225-101112"
        StringBuffer formattedDateTimeString;
        formattedDateTimeString.appendf("%c%c%c%c-%c%c-%c%cT%c%c:%c%c:%c%c",
             dir[1], dir[2], dir[3], dir[4],
             dir[5], dir[6],
             dir[7], dir[8],
             dir[10], dir[11],
             dir[13], dir[14],
             dir[15], dir[16]);

        CDateTime now;
        now.setNow();

        CDateTime expires;
        expires.setString(formattedDateTimeString.str());
        expires.adjustTime(60 * 24 * defaultExpireDays);

        if (now.compare(expires, false) > 0)
        {
            PROGLOG(LOGDBGHK "Post-mortem dir: %s has expired", dir);

            return true;
        }
        else
        {
            return false;
        }
    }

} *sashaDebugHousekeepingServer = NULL;

ISashaServer *createSashaDebugHousekeepingServer()
{
    assertex(!sashaDebugHousekeepingServer); // initialization problem
#ifdef _CONTAINERIZED
    Linked<IPropertyTree> config = serverConfig;
#else
    Owned<IPropertyTree> config = serverConfig->getPropTree("DbgHk");
    if (!config)
        config.setown(createPTree("DbgHk"));
#endif
    sashaDebugHousekeepingServer = new CSashaDebugHousekeepingServer(config);
    return sashaDebugHousekeepingServer;
}
