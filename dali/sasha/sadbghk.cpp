#include "platform.h"

#include "jlib.hpp"
#include "jiface.hpp"
#include "jptree.hpp"
#include "jregexp.hpp"

#include "dadfs.hpp"
#include "dalienv.hpp"

#include "saserver.hpp"
#include "sautil.hpp"

#define DEFAULT_EXPIRY_INTERVAL 24 // hours

#define DEFAULT_EXPIRYDAYS 7

#define LOGDBGHK "DEBUGHOUSEKEEPING: "

// Debug Housekeeping monitor

class CSashaDebugHousekeepingServer : public ISashaServer, public Thread
{
    bool stopped;
    Semaphore stopsem;
    Mutex runmutex;
    Linked<IPropertyTree> props;

public:
    IMPLEMENT_IINTERFACE_USING(Thread);

    CSashaDebugHousekeepingServer(IPropertyTree *_config)
        : Thread("CSashaDebugHousekeepingServer"), props(_config)
    {
        stopped = false;

        StringBuffer userName;
        props->getProp("@user", userName);
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
        StringBuffer planeName;
        if (!getDefaultPlane(planeName, "@debugPlane", "debug"))
        {
            WARNLOG("Exception handlers configured, but debug plane is missing");
            return;
        }
        Owned<IPropertyTree> plane = getStoragePlane(planeName);
        assertex(plane);
        verifyex(plane->getProp("@prefix", debugDir));

        // iterate debug plane selecting post-mortem directories for housekeeping
        Owned<IDirectoryIterator> pDirIter = createDirectoryIterator(debugDir.str(), "*", false, true);
        ForEach(*pDirIter)
        {
            if (stopped)
                break;

            IFile &iDirFile = pDirIter->query();
            const char *dirPath = iDirFile.queryFilename();

            if (!dirPath || !*dirPath)
                continue;

            // Process directories, exclude the ".", "..", non post-mortem and not expired post-mortem directories
            if (iDirFile.isDirectory() == fileBool::foundYes && *dirPath != '.')
            {
                // Ensure directory name only
                StringBuffer dirNameOnly;
                String dirNameStr(dirPath);
                int fwdSlashIndex = dirNameStr.lastIndexOf('/');
                if (fwdSlashIndex > -1)
                {
                    String *tmpStr = dirNameStr.substring(fwdSlashIndex + 1);
                    dirNameOnly.append(*tmpStr);
                    delete tmpStr;
                }
                else
                {
                    dirNameOnly.append(dirNameStr);
                }

                if (isPostMortemDirPath(dirNameOnly) && isExpiredDirPath(dirNameOnly, defaultExpireDays))
                {
                    recursiveRemoveDirectory(dirPath);
                    PROGLOG(LOGDBGHK "Deleted %s", dirPath);
                }
            }
        }
        pDirIter.clear();

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
    bool isPostMortemDirPath(const StringBuffer &dirName)
    {
        // Expecting a directory name like "W20250225-101112"
        RegExpr postMortemDirRegEx("^W[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9][0-9][0-9]$");
        if (postMortemDirRegEx.find(dirName.str()))
        {
            PROGLOG(LOGDBGHK "Post-mortem dir: %s", dirName.str());

            return true;
        }
        else
        {
            PROGLOG(LOGDBGHK "Non post-mortem dir: %s", dirName.str());

            return false;
        }
    }

    bool isExpiredDirPath(StringBuffer &dirName, const unsigned &defaultExpireDays)
    {
        // Directory name is like "W20250225-101112"
        StringBuffer cDateTimeoprmattedString;
        cDateTimeoprmattedString.appendf("%c%c%c%c-%c%c-%c%cT%c%c:%c%c:%c%c",
                                         dirName.charAt(1), dirName.charAt(2), dirName.charAt(3), dirName.charAt(4),
                                         dirName.charAt(5), dirName.charAt(6),
                                         dirName.charAt(7), dirName.charAt(8),
                                         dirName.charAt(10), dirName.charAt(11),
                                         dirName.charAt(12), dirName.charAt(13),
                                         dirName.charAt(14), dirName.charAt(15));

        CDateTime now;
        now.setNow();

        CDateTime expires;
        expires.setString(cDateTimeoprmattedString.str());
        expires.adjustTime(60 * 24 * defaultExpireDays);

        if (now.compare(expires, false) > 0)
        {
            PROGLOG(LOGDBGHK "Post-mortem dir: %s has expired", dirName.str());

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
    Linked<IPropertyTree> config = serverConfig;
    sashaDebugHousekeepingServer = new CSashaDebugHousekeepingServer(config);
    return sashaDebugHousekeepingServer;
}
