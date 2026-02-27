/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

#include "platform.h"
#include "jlib.hpp"
#include "jlog.ipp"
#include "jtime.hpp"

#include "saserver.hpp"
#include "sautil.hpp"
#include "saglobalmsg.hpp"
#include "sysinfologger.hpp"

static constexpr unsigned defaultHouseKeepingIntervalHours = 24;
static constexpr unsigned defaultGlobalMsgRetentionDays = 30;

/**
 * CSashaGlobalMessageServer
 *
 * Sasha server for managing the lifecycle of global messages stored in the
 * system:  global messages are deleted after a configurable retention period.
 *
 * Responsibilities:
 *  - Periodically scan persisted global messages at a configurable interval.
 *  - Permanently delete messages that exceed the configured retention period
 *
 * Configuration:
 *  - Housekeeping interval in hours (intervalHours).
 *  - Message retention period in days (retentionPeriodDays).

 */
class CSashaGlobalMessageServer : public CInterfaceOf<ISashaServer>, public Thread
{
    std::atomic<bool> stopped{true};
    Semaphore stopSem;
    Mutex runMutex;
    
    unsigned intervalHours = defaultHouseKeepingIntervalHours;
    unsigned retentionPeriodDays = defaultGlobalMsgRetentionDays; // Messages older than this are deleted

    void calculateCutoffDate(unsigned cutoffDays, unsigned &year, unsigned &month, unsigned &day) const
    {
        CDateTime cutoff;
        cutoff.setNow();
        __int64 minutesOffset = - (static_cast<__int64>(cutoffDays) * 24 * 60);
        cutoff.adjustTime(minutesOffset);
        cutoff.getDate(year, month, day);
    }

private:
    bool loadConfiguration()
    {
        Owned<IPropertyTree> compConfig = getComponentConfig();

        intervalHours = compConfig->getPropInt("@interval", defaultHouseKeepingIntervalHours);
        if (!intervalHours)
            return false; // service disabled if interval is zero

        retentionPeriodDays = compConfig->getPropInt("@retentionDays", defaultGlobalMsgRetentionDays);

        return true;
    }

    void runGlobalMessageMaintenance()
    {
        synchronized block(runMutex);
        if (stopped)
            return;

        PROGLOG("Starting Global Message maintenance cycle");
        unsigned deletedCount = 0;

        try
        {
            unsigned year, month, day;
            calculateCutoffDate(retentionPeriodDays, year, month, day);
            deletedCount += deleteOlderThanLogSysInfoMsg(false, false, year, month, day, nullptr);
        }
        catch (IException *e)
        {
            EXCLOG(e, "Error during Global Message maintenance");
            e->Release();
        }
        catch (...)
        {
            ERRLOG("Unknown error during Global Message maintenance");
        }
        if (deletedCount)
            PROGLOG("Global Message maintenance: deleted %u messages", deletedCount);
    }

    virtual int run() override
    {
        CSashaSchedule schedule;
        schedule.init(getComponentConfigSP(), intervalHours);

        PROGLOG("Sasha Global Message Server started");

        while (!stopped)
        {
            stopSem.wait(1000 * 60); // Wait 1 minute
            if (stopped)
                break;
            if (!schedule.ready())
                continue;

            try
            {
                runGlobalMessageMaintenance();
            }
            catch (IException *e) 
            {
                EXCLOG(e, "Global Message Server maintenance error");
                e->Release();
            }
        }

        PROGLOG("Sasha Global Message Server stopped");
        return 0;
    }

public:
    CSashaGlobalMessageServer()
        : Thread("CSashaGlobalMessageServer")
    {
    }

    virtual void start() override
    {
        if (!loadConfiguration())
        {
            PROGLOG("Sasha Global Message Server disabled");
            return;
        }

        stopped = false;
        Thread::start(false);
    }

    virtual void ready() override
    {
    }

    virtual void stop() override
    {
        if (stopped)
            return;

        stopped = true;
        stopSem.signal();
        synchronized block(runMutex); // hopefully stopped should stop
        if (!join(1000 * 60 * 3))
            OERRLOG("CSashaGlobalMessageServer aborted");
    }
};

ISashaServer *createSashaGlobalMessageServer()
{
    return new CSashaGlobalMessageServer();
}
