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

#define DEFAULT_GLOBAL_MSG_RETENTION_DAYS 30
#define DEFAULT_GLOBAL_MSG_ARCHIVE_PATH "/var/lib/HPCCSystems/globalmessages"

static CriticalSection globalMsgCrit;

class CSashaGlobalMessageServer : public ISashaServer, public Thread
{
    std::atomic<bool> stopped{true};
    Semaphore stopSem;
    Mutex runMutex;
    
    // Configuration settings
    unsigned intervalHours = 24; // Default 24 hours
    unsigned retentionPeriodDays = DEFAULT_GLOBAL_MSG_RETENTION_DAYS; // Messages older than this are deleted
    StringBuffer archivePath;
    bool enableArchiving = false;
    bool enableAutoDelete = false;
    bool enableHideOldMessages = false;
    unsigned hideAfterDays = 7; // Hide messages after 7 days by default
    unsigned archiveAfterDays = 14; // Archive messages after 14 days by default
    
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
        intervalHours = compConfig->getPropInt("@interval", 24);
        if (intervalHours == 0)
            return; // Disabled if interval is 0

        // Load retention settings - messages older than retentionPeriodDays are deleted
        retentionPeriodDays = compConfig->getPropInt("@retentionDays", DEFAULT_GLOBAL_MSG_RETENTION_DAYS);
        hideAfterDays = compConfig->getPropInt("@hideAfterDays", hideAfterDays);
        archiveAfterDays = compConfig->getPropInt("@archiveAfterDays", archiveAfterDays);
        
        // Load archival settings
        const char *configPath = compConfig->queryProp("@archivePath");
        archivePath.set(configPath ? configPath : DEFAULT_GLOBAL_MSG_ARCHIVE_PATH);
        
        enableArchiving = compConfig->getPropBool("@enableArchiving", false);
        enableAutoDelete = compConfig->getPropBool("@enableAutoDelete", false);
        enableHideOldMessages = compConfig->getPropBool("@enableHideOldMessages", true);

        PROGLOG("Global Message Server Configuration:");
        PROGLOG("  Interval: %u hours", intervalHours);
        PROGLOG("  Retention Period: %u days (messages older than this are deleted)", retentionPeriodDays);
        PROGLOG("  Archive Path: %s", archivePath.str());
        PROGLOG("  Archiving: %s", enableArchiving ? "enabled" : "disabled");
        PROGLOG("  Auto Delete: %s", enableAutoDelete ? "enabled" : "disabled");
        PROGLOG("  Auto Hide: %s", enableHideOldMessages ? "enabled" : "disabled");
        PROGLOG("  Hide After: %u days", hideAfterDays);
        PROGLOG("  Archive After: %u days", archiveAfterDays);
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
        
        try {
            CriticalBlock crit(globalMsgCrit);
            
            // Get current timestamp for age calculations
            CDateTime now;
            now.setNow();
            
            // Calculate cutoff dates
            CDateTime deleteCutoff;
            deleteCutoff.setNow();
            deleteCutoff.adjustTime(-retentionPeriodDays * 24 * 60); // Messages older than retention period
            
            CDateTime hideCutoff;
            hideCutoff.setNow();  
            hideCutoff.adjustTime(-hideAfterDays * 24 * 60); // Messages older than hide period

            CDateTime archiveCutoff;
            archiveCutoff.setNow();
            archiveCutoff.adjustTime(-archiveAfterDays * 24 * 60); // Messages older than archive period

            // Step 2: Archive old messages if archiving is enabled
            if (enableArchiving) {
                archivedCount = archiveOldMessages(archiveCutoff);
            }

            // Delete messages outside retention period if auto-delete is enabled
            if (enableAutoDelete) {
                deletedCount = deleteOldMessages(deleteCutoff);
            }

            // Hide old messages if auto-hide is enabled
            if (enableHideOldMessages) {
                hiddenCount = hideOldMessages(hideCutoff);
            }

            if (hiddenCount || archivedCount || deletedCount)
                PROGLOG("Global Message maintenance: hidden %u, archived %u, deleted %u messages", hiddenCount, archivedCount, deletedCount);
        }
        catch (IException *e) {
            EXCLOG(e, "Error during Global Message maintenance");
            e->Release();
        }
        catch (...) {
            ERRLOG("Unknown error during Global Message maintenance");
        }
    }

    unsigned archiveOldMessages(CDateTime &cutoff)
    {
        try {
            // Create archive filename with timestamp
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

            // 1) Create a message filter
            Owned<ISysInfoLoggerMsgFilter> filter = createSysInfoLoggerMsgFilter();
            
            // 2) Set the date range for messages older than cutoff
            unsigned cutoffYear, cutoffMonth, cutoffDay;
            cutoff.getDate(cutoffYear, cutoffMonth, cutoffDay);
            filter->setOlderThanDate(cutoffYear, cutoffMonth, cutoffDay);
            
            // 3) Create message iterator
            Owned<ISysInfoLoggerMsgIterator> iter = createSysInfoLoggerMsgIterator(filter);
            
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
            
            // 4a) Read messages one by one and 4b) write to file
            ForEach(*iter)
            {
                const ISysInfoLoggerMsg &msg = iter->query();
                
                // Escape commas and quotes in message content for CSV format
                StringBuffer escapedMsg;
                const char *msgText = msg.queryMsg();
                if (msgText && *msgText)
                {
                    escapedMsg.append('"');
                    for (const char *p = msgText; *p; p++)
                    {
                        if (*p == '"')
                            escapedMsg.append("\"\""); // Escape quotes by doubling them
                        else
                            escapedMsg.append(*p);
                    }
                    escapedMsg.append('"');
                }
                
                // Escape source field too
                StringBuffer escapedSource;
                const char *sourceText = msg.querySource();
                if (sourceText && *sourceText)
                {
                    escapedSource.append('"').append(sourceText).append('"');
                }
                
                StringBuffer line;
                line.appendf("%" I64F "u,%" I64F "u,%s,%s,%u,%s,%s\n",
                            msg.queryLogMsgId(),
                            msg.queryTimeStamp(),
                            escapedSource.length() ? escapedSource.str() : "",
                            LogMsgClassToFixString(msg.queryClass()),
                            msg.queryLogMsgCode(),
                            msg.queryIsHidden() ? "true" : "false",
                            escapedMsg.length() ? escapedMsg.str() : "");
                
                fileIO->write(writeOffset, line.length(), line.str());
                writeOffset += line.length();
                archivedCount++;
            }
            
            // Write footer
            StringBuffer footer;
            footer.appendf("\n# Archive complete: %u messages archived\n", archivedCount);
            fileIO->write(writeOffset, footer.length(), footer.str());
            
            // Close the file
            fileIO.clear();
            file.clear();
            
            if (archivedCount > 0)
            {
                PROGLOG("Archived %u global messages to: %s", archivedCount, archiveFile.str());
                
                // 5) Delete the archived messages
                unsigned deletedCount = deleteOlderThanLogSysInfoMsg(true, true, cutoffYear, cutoffMonth, cutoffDay, nullptr);
                PROGLOG("Deleted %u archived messages from storage", deletedCount);
            }
            
            return archivedCount;
        }
        catch (IException *e) {
            EXCLOG(e, "Error archiving global messages");
            e->Release();
            return 0;
        }
    }

    unsigned hideOldMessages(CDateTime &cutoff)
    {
        try {
            Owned<ISysInfoLoggerMsgFilter> filter = createSysInfoLoggerMsgFilter();
            
            unsigned cutoffYear, cutoffMonth, cutoffDay;
            cutoff.getDate(cutoffYear, cutoffMonth, cutoffDay);
            filter->setOlderThanDate(cutoffYear, cutoffMonth, cutoffDay);
            filter->setVisibleOnly(); // Only hide visible messages
            unsigned hiddenCount = hideLogSysInfoMsg(filter);
            
            return hiddenCount;
        }
        catch (IException *e) {
            EXCLOG(e, "Error hiding old global messages");
            e->Release();
            return 0;
        }
    }

    unsigned deleteOldMessages(CDateTime &cutoff)
    {
        try {
            // Delete messages older than the retention period cutoff
            unsigned year, month, day;
            cutoff.getDate(year, month, day);
            
            unsigned deletedCount = deleteOlderThanLogSysInfoMsg(true, false, year, month, day, nullptr);
            
            return deletedCount;
        }
        catch (IException *e) {
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
        
        while (!stopped) {
            stopSem.wait(1000 * 60); // Wait 1 minute
            if (stopped)
                break;
            if (!schedule.ready())
                continue;
                
            try {
                runGlobalMessageMaintenance();
            }
            catch (IException *e) {
                EXCLOG(e, "Global Message Server maintenance error");
                e->Release();
            }
        }
        
        PROGLOG("Sasha Global Message Server stopped");
        return 0;
    }

private:
    // Helper methods remain the same
    
} *sashaGlobalMessageServer = nullptr;

ISashaServer *createSashaGlobalMessageServer()
{
    assertex(isContainerized());
    assertex(!sashaGlobalMessageServer); // initialization problem
    sashaGlobalMessageServer = new CSashaGlobalMessageServer();
    return sashaGlobalMessageServer;
}