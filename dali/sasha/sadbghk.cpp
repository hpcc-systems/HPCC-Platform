#include "platform.h"

#include "jlib.hpp"
#include "jiface.hpp"
#include "jptree.hpp"
#include "jstring.hpp"

#include "dadfs.hpp"
#include "dalienv.hpp"

#include "saserver.hpp"
#include "sautil.hpp"

#define DEFAULT_HOUSEKEEPING_INTERVAL_HOURS 24

#define LOGDBGHK "DEBUGPLANEHOUSEKEEPING: "

// Debug Housekeeping monitor

class CSashaDebugPlaneHousekeepingServer : public ISashaServer, public Thread
{
    bool stopped{false};
    Semaphore stopsem;
    Mutex runmutex;
    StringBuffer debugDir;
    unsigned expiryDays{0};

public:
    IMPLEMENT_IINTERFACE_USING(Thread);

    CSashaDebugPlaneHousekeepingServer()
        : Thread("CSashaDebugPlaneHousekeepingServer")
    {
    }


    virtual void start() override
    {
        Thread::start(false);

        expiryDays = getComponentConfig()->getPropInt("@expiryDays");

        // get debug plane dir
        StringBuffer planeName;
        if (!getDefaultPlane(planeName, "@debugPlane", "debug"))
        {
            WARNLOG("Exception handlers configured, but debug plane is missing");
            return;
        }
        Owned<IPropertyTree> plane = getStoragePlane(planeName);
        assertex(plane);
        verifyex(plane->getProp("@prefix", debugDir));
    }

    virtual void ready() override
    {
    }

    virtual void stop() override
    {
        if (!stopped)
        {
            stopped = true;
            stopsem.signal();
        }
        synchronized block(runmutex); // hopefully stopped should stop
        if (!join(1000 * 60 * 3))
            OERRLOG("CSashaDebugPlaneHousekeepingServer aborted");
    }

    void runDebugHousekeeping()
    {
        synchronized block(runmutex);
        if (stopped)
            return;

        // iterate debug plane selecting post-mortem directories for housekeeping
        Owned<IDirectoryIterator> pDirIter = createDirectoryIterator(debugDir.str(), "*", false, true);
        ForEach(*pDirIter)
        {
            if (stopped)
                break;

            IFile &iFile = pDirIter->query();
            const char *filePath = iFile.queryFilename();

            // Process directories, exclude the ".", "..", non post-mortem and not expired post-mortem directories
            if (iFile.isDirectory() == fileBool::foundYes)
            {
                if (streq(filePath, ".") || streq(filePath, ".."))
                    continue;

                // Ensure directory name only
                StringBuffer dirNameOnly;
                String dirNameStr(filePath);
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

                if (isExpiredModifiedDateTime(filePath))
                {
                    recursiveRemoveDirectory(filePath);
                }
            }
            else
            {
                if (isExpiredModifiedDateTime(filePath))
                {
                    iFile.remove();
                }
            }
        }
        pDirIter.clear();
    }

    virtual int run() override
    {
        CSashaSchedule schedule;
        unsigned interval = getComponentConfig()->getPropInt("@interval",DEFAULT_HOUSEKEEPING_INTERVAL_HOURS);
        if (interval == 0)
        {
            stopped = true;
            return 0;
        }
        schedule.init(getComponentConfig(), interval);
        while (!stopped)
        {
            stopsem.wait(1000 * 60);
            if (stopped)
                break;
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
        return 0;
    }

private:
    bool isExpiredModifiedDateTime(const char *filePath)
    {
        Owned<IFile> file = createIFile(filePath);
        CDateTime expiredModifiedTime;
        file->getTime( nullptr,  &expiredModifiedTime, nullptr);
        expiredModifiedTime.adjustTime(60 * 24 * expiryDays);

        CDateTime now;
        now.setNow();

        if (now.compare(expiredModifiedTime, false) > 0)
            return true;
        else
            return false;
    }

} *sashaDebugPlaneHousekeepingServer = NULL;

ISashaServer *createSashaDebugPlaneHousekeepingServer()
{
    assertex(isContainerized());
    assertex(!sashaDebugPlaneHousekeepingServer); // initialization problem
    sashaDebugPlaneHousekeepingServer = new CSashaDebugPlaneHousekeepingServer();
    return sashaDebugPlaneHousekeepingServer;
}
