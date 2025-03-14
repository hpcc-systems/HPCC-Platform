#include "platform.h"

#include "jlib.hpp"
#include "jiface.hpp"
#include "jptree.hpp"
#include "jstring.hpp"

#include "dadfs.hpp"
#include "dalienv.hpp"

#include "saserver.hpp"
#include "sautil.hpp"

static constexpr unsigned defaultHouseKeepingIntervalHours = 24;

#define LOGDBGHK "DEBUGPLANEHOUSEKEEPING: "

// Debug Housekeeping monitor

class CSashaDebugPlaneHousekeepingServer : public ISashaServer, public Thread
{
    std::atomic<bool> stopped{false};
    Semaphore stopSem;
    Mutex runMutex;
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

        expiryDays = getComponentConfigSP()->getPropInt("@expiryDays");

        // get debug plane dir
        StringBuffer planeName;
        if (!getDefaultPlane(planeName, "@debugPlane", "debug"))
        {
            WARNLOG("Failed to get default debug plane");
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
            stopSem.signal();
        }
        synchronized block(runMutex); // hopefully stopped should stop
        if (!join(1000 * 60 * 3))
            OERRLOG("CSashaDebugPlaneHousekeepingServer aborted");
    }

    void runDebugHousekeeping()
    {
        synchronized block(runMutex);
        if (stopped)
            return;

        // iterate debug plane selecting files and directories for housekeeping
        Owned<IDirectoryIterator> pDirIter = createDirectoryIterator(debugDir.str(), "*", false, true);
        ForEach(*pDirIter)
        {
            if (stopped)
                break;

            IFile &iFile = pDirIter->query();
            const char *filePath = iFile.queryFilename();

            if (iFile.isDirectory() == fileBool::foundYes)
            {
                // Process directories, exclude the ".", "..", and non expired directories from housekeeping
                if (streq(filePath, ".") || streq(filePath, ".."))
                        continue;

                if (hasExpired(iFile))
                {
                    recursiveRemoveDirectory(&iFile);
                }
            }
            else
            {
                // Process file exclude non expired files from housekeeping
                if (hasExpired(iFile))
                {
                    iFile.remove();
                }
            }
        }
    }

    virtual int run() override
    {
        Owned<IPropertyTree> compConfig = getComponentConfig();
        CSashaSchedule schedule;
        unsigned interval = compConfig->getPropInt("@interval", defaultHouseKeepingIntervalHours);
        if (interval == 0)
        {
            stopped = true;
            return 0;
        }
        schedule.init(compConfig, interval);
        while (!stopped)
        {
            stopSem.wait(1000 * 60);
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
                EXCLOG(e, LOGDBGHK);
                e->Release();
            }
        }
        return 0;
    }

private:
    bool hasExpired(IFile &iFile)
    {
        CDateTime expiredModifiedTime;
        iFile.getTime(nullptr, &expiredModifiedTime, nullptr);
        if (expiredModifiedTime.isNull())
        {
            WARNLOG("Failed to get modified time for file %s", iFile.queryFilename());
            return false;
        }
        expiredModifiedTime.adjustTime(60 * 24 * expiryDays);

        CDateTime now;
        now.setNow();

        if (now.compare(expiredModifiedTime, false) > 0)
            return true;
        else
            return false;
    }

} *sashaDebugPlaneHousekeepingServer = nullptr;

ISashaServer *createSashaDebugPlaneHousekeepingServer()
{
    assertex(isContainerized());
    assertex(!sashaDebugPlaneHousekeepingServer); // initialization problem
    sashaDebugPlaneHousekeepingServer = new CSashaDebugPlaneHousekeepingServer();
    return sashaDebugPlaneHousekeepingServer;
}
