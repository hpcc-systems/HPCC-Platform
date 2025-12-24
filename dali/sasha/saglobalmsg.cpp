/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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

#include "saserver.hpp"
#include "sautil.hpp"
#include "saglobalmsg.hpp"
#include "sysinfologger.hpp"

static constexpr unsigned defaultHouseKeepingIntervalHours = 24;
static constexpr unsigned defaultGlobalMsgRetentionDays = 30;
static constexpr unsigned defaultGlobalMsgHideAfterDays = 7;
static constexpr unsigned defaultGlobalMsgArchiveAfterDays = 14;
static constexpr const char * defaultGlobalMsgArchivePath = "/var/lib/HPCCSystems/globalmessages";
static constexpr bool defaultEnableArchiving = false;
static constexpr bool defaultEnableAutoDelete = false;
static constexpr bool defaultEnableHideOldMessages = true;

/**
 * CSashaGlobalMessageServer
 *
 * Implements the Sasha server responsible for managing the lifecycle of
 * global messages stored in the system. This component runs as a background
 * housekeeping thread and is exposed via the ISashaServer interface.
 *
 * Responsibilities:
 *  - Periodically scan persisted global messages at a configurable interval.
 *  - Hide old messages after a configurable number of days, if enabled.
 *  - Archive older messages to a filesystem location, if archiving is enabled.
 *  - Permanently delete messages that exceed the configured retention period,
 *    if automatic deletion is enabled.
 *
 * Configuration:
 *  - Housekeeping interval in hours (intervalHours).
 *  - Message retention period in days (retentionPeriodDays).
 *  - Hide-after age in days (hideAfterDays) when enableHideOldMessages is true.
 *  - Archive-after age in days (archiveAfterDays) when enableArchiving is true.
 *  - Archive destination path (archivePath) for stored message archives.
 *  - Flags to enable/disable archiving, auto-deletion and hiding of old messages.
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
    bool enableHideOldMessages = defaultEnableHideOldMessages;
    unsigned hideAfterDays = defaultGlobalMsgHideAfterDays;
    unsigned archiveAfterDays = defaultGlobalMsgArchiveAfterDays;
    
public:
    IMPLEMENT_IINTERFACE_USING(Thread);

    CSashaGlobalMessageServer()
        : Thread("CSashaGlobalMessageServer")
    {
    }

    void loadConfiguration()
    {
        Owned<IPropertyTree> compConfig = getComponentConfig();
        if (!compConfig)
            return;

        // Load interval (in hours, like debug housekeeping)
        intervalHours = compConfig->getPropInt("@interval", defaultHouseKeepingIntervalHours);
        if (intervalHours == 0)
        {
            PROGLOG("Sasha Global Message Server disabled (interval 0)");
            return; // Disabled if interval is 0
        }

        // Load retention settings - messages older than retentionPeriodDays are deleted
        retentionPeriodDays = compConfig->getPropInt("@retentionDays", defaultGlobalMsgRetentionDays);
        hideAfterDays = compConfig->getPropInt("@hideAfterDays", defaultGlobalMsgHideAfterDays);
        archiveAfterDays = compConfig->getPropInt("@archiveAfterDays", defaultGlobalMsgArchiveAfterDays);
        
        // Load archival settings
        archivePath.set(compConfig->queryProp("@archivePath", defaultGlobalMsgArchivePath));
        
        enableArchiving = compConfig->getPropBool("@enableArchiving", defaultEnableArchiving);
        enableAutoDelete = compConfig->getPropBool("@enableAutoDelete", defaultEnableAutoDelete);
        enableHideOldMessages = compConfig->getPropBool("@enableHideOldMessages", defaultEnableHideOldMessages);

        PROGLOG("Sasha Global Message Server Configuration:");
        PROGLOG("  Interval: %u hours", intervalHours);
        PROGLOG("  Archiving: %s", enableArchiving ? "enabled" : "disabled");
        PROGLOG("  Archive Path: %s", archivePath.str());
        PROGLOG("  Archive after: %u days", archiveAfterDays);
        PROGLOG("  Auto Delete: %s", enableAutoDelete ? "enabled" : "disabled");
        PROGLOG("  Retention Period: %u days (message deletion threshold)", retentionPeriodDays);
        PROGLOG("  Auto Hide: %s", enableHideOldMessages ? "enabled" : "disabled");
        PROGLOG("  Hide After: %u days", hideAfterDays);
    }

    virtual void start() override
    {
        loadConfiguration();
        if (intervalHours == 0)
            return; // Service disabled
            
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

    void runGlobalMessageMaintenance()
    {
        synchronized block(runMutex);
        if (stopped)
            return;
            
        PROGLOG("Starting Global Message maintenance cycle");
        
        unsigned hiddenCount = 0;
        unsigned archivedCount = 0; 
        unsigned deletedCount = 0;

        if (enableArchiving && enableAutoDelete && retentionPeriodDays <= archiveAfterDays)
        {
            WARNLOG("Global Message maintenance: retentionPeriodDays (%u) should be greater than archiveAfterDays (%u). Adjusting retentionPeriodDays to %u days for this run", retentionPeriodDays, archiveAfterDays, archiveAfterDays + 1);
            retentionPeriodDays = archiveAfterDays + 1;
        }
        try {
            // Archive old messages if archiving is enabled
            if (enableArchiving)
                archivedCount = archiveOldMessages(archiveAfterDays);

            // Delete messages outside retention period if auto-delete is enabled
            if (enableAutoDelete)
                deletedCount = deleteOldMessages(retentionPeriodDays);

            // Hide old messages if auto-hide is enabled
            if (enableHideOldMessages)
                hiddenCount = hideOldMessages(hideAfterDays);

            if (hiddenCount || archivedCount || deletedCount)
                PROGLOG("Global Message maintenance: hidden %u, archived %u, deleted %u messages", hiddenCount, archivedCount, deletedCount);
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
     * Helper function to escape a string field for CSV output with backslash escaping.
     * Quotes the field and escapes special characters: quotes (doubled), backslashes, newlines, and carriage returns.
     */
    void escapeCsvField(StringBuffer & out, const char * text)
    {
        if (!text || !*text)
            return;
        
        out.append('"');
        for (const char *p = text; *p; p++)
        {
            if (*p == '"')
                out.append("\"\""); // Escape quotes by doubling them
            else if (*p == '\\')
                out.append("\\\\"); // Escape backslashes
            else if (*p == '\r')
            {
                if (*(p+1) == '\n')
                {
                    out.append("\\n"); // Handle Windows line endings as single unit
                    p++; // Skip the \n
                }
                else
                    out.append("\\r"); // Escape standalone carriage returns
            }
            else if (*p == '\n')
                out.append("\\n"); // Escape Unix newlines
            else
                out.append(*p);
        }
        out.append('"');
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
     * @param cutoffDays  Number of days back from the current time; messages older than this many
     *                    days are archived and removed. For example, a value of 14 archives
     *                    messages older than 14 days.
     *
     * @return The number of messages that were successfully archived (and deleted). If an error
     *         occurs during archiving, the error is logged, any thrown IException is caught and
     *         released, processing is aborted, and 0 is returned. Archive files that were already
     *         written may remain on disk even if a later error occurs.
     */
    unsigned archiveOldMessages(unsigned cutoffDays)
    {
        try {
            // Calculate cutoff date
            CDateTime cutoff;
            cutoff.setNow();
            cutoff.adjustTime(-cutoffDays * 24 * 60); // Messages older than cutoff period
            unsigned cutoffYear, cutoffMonth, cutoffDay;
            cutoff.getDate(cutoffYear, cutoffMonth, cutoffDay);
            
            // Create filter with cutoff date and iterate through messages older than cutoff
            Owned<ISysInfoLoggerMsgFilter> filter = createSysInfoLoggerMsgFilter();
            filter->setOlderThanDate(cutoffYear, cutoffMonth, cutoffDay);
            Owned<ISysInfoLoggerMsgIterator> iter = createSysInfoLoggerMsgIterator(filter);
            
            // Check if there are any messages to archive (so as to avoid creating empty archive files)
            if (!iter->first())
            {
                PROGLOG("No global messages found to archive (older than %u days)", cutoffDays);
                return 0;
            }
            
            // Prepare archive filename with timestamp
            CDateTime now;
            now.setNow();
            StringBuffer archiveFile;
            unsigned year, month, day, hour, minute, second, nano;
            now.getDate(year, month, day);
            now.getTime(hour, minute, second, nano);
            
            archiveFile.appendf("%s/globalmessages_%04d%02d%02d_%02d%02d%02d.txt",
                              archivePath.str(), year, month, day, hour, minute, second);

            // Ensure archive directory exists
            recursiveCreateDirectoryForFile(archiveFile.str());

            // Create archive file
            Owned<IFile> file = createIFile(archiveFile.str());
            Owned<IFileIO> fileIO = file->open(IFOcreate);
            if (!fileIO)
            {
                OWARNLOG("Failed to create archive file: %s", archiveFile.str());
                return 0;
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
            
            unsigned archivedCount = 0;
            
            // Process messages one by one and write to archive file
            do
            {
                const ISysInfoLoggerMsg &msg = iter->query();
                
                // Escape string fields for CSV format
                StringBuffer escapedSource, escapedMsg;
                escapeCsvField(escapedSource, msg.querySource());
                escapeCsvField(escapedMsg, msg.queryMsg());
                
                StringBuffer line;
                line.appendf("%" I64F "u,%" I64F "u,%s,%s,%u,%s,%s\n",
                            msg.queryLogMsgId(),
                            msg.queryTimeStamp(),
                            escapedSource.length() ? escapedSource.str() : "\"\"",
                            LogMsgClassToFixString(msg.queryClass()),
                            msg.queryLogMsgCode(),
                            msg.queryIsHidden() ? "true" : "false",
                            escapedMsg.length() ? escapedMsg.str() : "\"\"");
                
                fileIO->write(writeOffset, line.length(), line.str());
                writeOffset += line.length();
                archivedCount++;
            }
            while (iter->next());
            
            // Close the file
            fileIO.clear();
            file.clear();
            
            PROGLOG("Archived %u global messages to: %s", archivedCount, archiveFile.str());
            
            // Delete the archived messages
            unsigned deletedCount = deleteOlderThanLogSysInfoMsg(false, false, cutoffYear, cutoffMonth, cutoffDay, nullptr);
            PROGLOG("Deleted %u archived messages from storage", deletedCount);
            
            return archivedCount;
        }
        catch (IException *e)
        {
            EXCLOG(e, "Error archiving global messages");
            e->Release();
            return 0;
        }
    }

    unsigned hideOldMessages(unsigned cutoffDays)
    {
        try
        {
            // Calculate cutoff date
            CDateTime cutoff;
            cutoff.setNow();
            cutoff.adjustTime(-cutoffDays * 24 * 60); // Messages older than cutoff period
            
            Owned<ISysInfoLoggerMsgFilter> filter = createSysInfoLoggerMsgFilter();
            
            unsigned cutoffYear, cutoffMonth, cutoffDay;
            cutoff.getDate(cutoffYear, cutoffMonth, cutoffDay);
            filter->setOlderThanDate(cutoffYear, cutoffMonth, cutoffDay);
            filter->setVisibleOnly(); // Only hide visible messages
            unsigned hiddenCount = hideLogSysInfoMsg(filter);
            
            return hiddenCount;
        }
        catch (IException *e)
        {
            EXCLOG(e, "Error hiding old global messages");
            e->Release();
            return 0;
        }
    }

    unsigned deleteOldMessages(unsigned cutoffDays)
    {
        try
        {
            // Calculate cutoff date
            CDateTime cutoff;
            cutoff.setNow();
            cutoff.adjustTime(-cutoffDays * 24 * 60); // Messages older than cutoff period
            
            // Delete messages older than the retention period cutoff
            unsigned year, month, day;
            cutoff.getDate(year, month, day);
            
            unsigned deletedCount = deleteOlderThanLogSysInfoMsg(false, false, year, month, day, nullptr);
            
            return deletedCount;
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
                
            try {
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
} *sashaGlobalMessageServer = nullptr;

ISashaServer *createSashaGlobalMessageServer()
{
    assertex(!sashaGlobalMessageServer);
    sashaGlobalMessageServer = new CSashaGlobalMessageServer();
    return sashaGlobalMessageServer;
}