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
#include "jfile.hpp"
#include "jplane.hpp"

#include "saserver.hpp"
#include "sautil.hpp"
#include "saglobalmsg.hpp"
#include "sysinfologger.hpp"
#include "salds.hpp"

static constexpr unsigned defaultHouseKeepingIntervalHours = 24;
static constexpr unsigned defaultGlobalMsgRetentionDays = 30;
static constexpr unsigned defaultGlobalMsgArchiveAfterDays = 14;
static constexpr bool defaultEnableArchiving = false;
static constexpr bool defaultEnableAutoDelete = false;

/**
 * CSashaGlobalMessageServer
 *
 * Implements the Sasha server responsible for managing the lifecycle of
 * global messages stored in the system. This component runs as a background
 * housekeeping thread and is exposed via the ISashaServer interface.
 *
 * Responsibilities:
 *  - Periodically scan persisted global messages at a configurable interval.
 *  - Archive older messages to a filesystem location, if archiving is enabled.
 *  - Permanently delete messages that exceed the configured retention period,
 *    if automatic deletion is enabled.
 *
 * Configuration:
 *  - Housekeeping interval in hours (intervalHours).
 *  - Message retention period in days (retentionPeriodDays).
 *  - Archive-after age in days (archiveAfterDays) when enableArchiving is true.
 *  - Archive destination path (archivePath) for stored message archives.
 *  - Flags to enable/disable archiving and auto-deletion.
 *
 * The server loads these settings from the Sasha configuration (see
 * loadConfiguration()) and then executes maintenance operations on global
 * messages until stopped. An instance is created via createSashaGlobalMessageServer()
 * and typically managed as a singleton within the Sasha framework.
 */
class CSashaGlobalMessageServer : public ISashaServer, public Thread
{
    std::atomic<bool> stopped{true};
    Semaphore stopSem;
    Mutex runMutex;
    
    unsigned intervalHours = defaultHouseKeepingIntervalHours;
    unsigned retentionPeriodDays = defaultGlobalMsgRetentionDays; // Messages older than this are deleted
    StringBuffer archivePath;
    bool enableArchiving = defaultEnableArchiving;
    bool enableAutoDelete = defaultEnableAutoDelete;
    unsigned archiveAfterDays = defaultGlobalMsgArchiveAfterDays;

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
        if (!compConfig)
            throw makeStringException(-1, "Missing sasha component configuration");

        intervalHours = compConfig->getPropInt("@interval", defaultHouseKeepingIntervalHours);
        enableArchiving = compConfig->getPropBool("@enableArchiving", defaultEnableArchiving);
        enableAutoDelete = compConfig->getPropBool("@enableAutoDelete", defaultEnableAutoDelete);
        if (!intervalHours || (!enableArchiving && !enableAutoDelete))
        {
            PROGLOG("Sasha Global Message Server disabled (%s)", !intervalHours ? "interval is 0" : "both archiving and auto-delete are disabled");
            return false;
        }
        StringBuffer archivePlane;
        if (enableArchiving)
        {
            if (isContainerized())
            {
                const char *planeName = compConfig->queryProp("@archivePlane");
                if (isEmptyString(planeName))
                    planeName = "sasha";
                archivePlane.set(planeName);
                Owned<const IPropertyTree> plane = getStoragePlaneConfig(archivePlane.str(), true);
                verifyex(plane->getProp("@prefix", archivePath));
                addPathSepChar(archivePath).append("globalmessages");
            }
            else
            {
                if (!compConfig->getProp("@archivePath", archivePath))
                    throw makeStringException(-1, "No archive path configured (@archivePath)");
                getLdsPath("globalmessages", archivePath);
            }
        }

        retentionPeriodDays = compConfig->getPropInt("@retentionDays", defaultGlobalMsgRetentionDays);
        archiveAfterDays = compConfig->getPropInt("@archiveAfterDays", defaultGlobalMsgArchiveAfterDays);

        PROGLOG("Sasha Global Message Server Configuration:");
        PROGLOG("  Interval: %u hours", intervalHours);
        PROGLOG("  Archiving: %s", enableArchiving ? "enabled" : "disabled");
        if (enableArchiving)
        {
            if (isContainerized())
                PROGLOG("  Archive Plane: %s", archivePlane.str());
            PROGLOG("  Archive Path: %s", archivePath.str());
            PROGLOG("  Archive after: %u days", archiveAfterDays);
        }
            
        PROGLOG("  Auto Delete: %s", enableAutoDelete ? "enabled" : "disabled");
        if (enableAutoDelete)
            PROGLOG("  Retention Period: %u days (message deletion threshold)", retentionPeriodDays);

        if (enableArchiving && enableAutoDelete && retentionPeriodDays <= archiveAfterDays)
        {
            OWARNLOG("retentionPeriodDays (%u) should be greater than archiveAfterDays (%u). Adjusting retentionPeriodDays to %u days", retentionPeriodDays, archiveAfterDays, archiveAfterDays + 1);
            retentionPeriodDays = archiveAfterDays + 1;
        }
        return true;
    }

    void runGlobalMessageMaintenance()
    {
        synchronized block(runMutex);
        if (stopped)
            return;

        PROGLOG("Starting Global Message maintenance cycle");
        unsigned archivedCount = 0; 
        unsigned deletedCount = 0;

        try
        {
            // Archive old messages if archiving is enabled
            if (enableArchiving)
            {
                unsigned archivedDeletedCount = 0;
                archiveOldMessages(archiveAfterDays, archivedCount, archivedDeletedCount);
                deletedCount += archivedDeletedCount;
            }

            // Delete messages outside retention period if auto-delete is enabled
            if (enableAutoDelete)
                deletedCount += deleteOldMessages(retentionPeriodDays);

            if (archivedCount || deletedCount)
                PROGLOG("Global Message maintenance: archived %u, deleted %u messages", archivedCount, deletedCount);
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
    }

    /**
     * Archive global messages older than a given age into CSV files and remove them from storage.
     *
     * The cutoff date is computed as "now minus cutoffDays". All messages strictly older than this
     * cutoff are selected using a SysInfoLogger message filter and written to one or more CSV
     * files under the configured archive path (archivePath). The archive files follow a
     * predictable naming convention (typically including timestamp and/or date information) so
     * that multiple maintenance runs produce distinct archive files without overwriting previous
     * archives.
     *
     * After a message is successfully written to an archive file, that message is deleted from
     * the underlying message storage so that only the archived copy remains. This function
     * therefore both exports and prunes old messages.
     *
     * @param cutoffDays          Number of days back from the current time; messages older than this many
     *                            days are archived and removed. For example, a value of 14 archives
     *                            messages older than 14 days.
     * @param archivedCount       Output parameter that receives the number of messages archived.
     * @param deletedCount        Output parameter that receives the number of messages deleted from storage.
     */
    void archiveOldMessages(unsigned cutoffDays, unsigned &archivedCount, unsigned &deletedCount)
    {
        try
        {
            archivedCount = 0;
            deletedCount = 0;

            unsigned cutoffYear, cutoffMonth, cutoffDay;
            calculateCutoffDate(cutoffDays, cutoffYear, cutoffMonth, cutoffDay);

            {
                // Create filter with cutoff date and iterate through messages older than cutoff
                Owned<ISysInfoLoggerMsgFilter> filter = createSysInfoLoggerMsgFilter();
                filter->setOlderThanDate(cutoffYear, cutoffMonth, cutoffDay);
                Owned<ISysInfoLoggerMsgIterator> iter = createSysInfoLoggerMsgIterator(filter);

                // Check if there are any messages to archive (so as to avoid creating empty archive files)
                if (!iter->first())
                {
                    PROGLOG("No global messages found to archive (older than %u days)", cutoffDays);
                    return;
                }

                // Prepare archive filename with timestamp
                CDateTime now;
                now.setNow();
                unsigned year, month, day, hour, minute, second, nano;
                now.getDate(year, month, day);
                now.getTime(hour, minute, second, nano);

                StringBuffer archiveFile(archivePath);
                addPathSepChar(archiveFile).appendf("globalmessages_%04d%02d%02d_%02d%02d%02d.txt",
                                year, month, day, hour, minute, second);

                recursiveCreateDirectoryForFile(archiveFile.str());
                Owned<IFile> file = createIFile(archiveFile.str());
                Owned<IFileIO> fileIO = file->open(IFOcreate);
                if (!fileIO)
                {
                    OWARNLOG("Failed to create archive file: %s", archiveFile.str());
                    return;
                }

                // Write archive header
                StringBuffer header;
                header.appendf("# HPCC Global Messages Archive\n");
                header.appendf("# Created: %04d-%02d-%02d %02d:%02d:%02d\n", year, month, day, hour, minute, second);
                header.appendf("# Messages older than: %04d-%02d-%02d\n", cutoffYear, cutoffMonth, cutoffDay);
                header.appendf("# Format: ID,Timestamp,Source,Severity,Code,Hidden,Message\n\n");
                offset_t writeOffset = 0;
                fileIO->write(writeOffset, header.length(), header.str());
                writeOffset += header.length();

                do
                {
                    const ISysInfoLoggerMsg &msg = iter->query();

                    // Encode message content for CSV format
                    StringBuffer escapedMsg;
                    const char *msgText = msg.queryMsg();
                    if (msgText && *msgText)
                        encodeCSVColumn(escapedMsg, msgText);

                    StringBuffer line;
                    line.appendf("%" I64F "u,%" I64F "u,%s,%s,%u,%s,%s\n",
                                msg.queryLogMsgId(),
                                msg.queryTimeStamp(),
                                msg.querySource(),
                                LogMsgClassToFixString(msg.queryClass()),
                                msg.queryLogMsgCode(),
                                msg.queryIsHidden() ? "true" : "false",
                                escapedMsg.str());

                    fileIO->write(writeOffset, line.length(), line.str());
                    writeOffset += line.length();
                    archivedCount++;
                }
                while (iter->next());

                fileIO.clear();
                file.clear();
                PROGLOG("Archived %u global messages to: %s", archivedCount, archiveFile.str());
            }

            // Delete the archived messages
            deletedCount = deleteOlderThanLogSysInfoMsg(false, false, cutoffYear, cutoffMonth, cutoffDay, nullptr);
            PROGLOG("Deleted %u archived messages from storage", deletedCount);
        }
        catch (IException *e)
        {
            EXCLOG(e, "Error archiving global messages");
            e->Release();
        }
    }

    unsigned deleteOldMessages(unsigned cutoffDays)
    {
        try
        {
            unsigned year, month, day;
            calculateCutoffDate(cutoffDays, year, month, day);
            return deleteOlderThanLogSysInfoMsg(false, false, year, month, day, nullptr);
        }
        catch (IException *e)
        {
            EXCLOG(e, "Error deleting old global messages");
            e->Release();
            return 0;
        }
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
    IMPLEMENT_IINTERFACE_USING(Thread);

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