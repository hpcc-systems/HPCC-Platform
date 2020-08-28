/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#ifndef _LOGFAILSAFE_HPP__
#define _LOGFAILSAFE_HPP__

#include "jlib.hpp"
#include "jstring.hpp"
#include "jutil.hpp" // for StringArray
#include "loggingcommon.hpp"
#include "LogSerializer.hpp"

const unsigned long DEFAULT_SAFE_ROLLOVER_REQ_THRESHOLD = 500000L;
const char* const PropSafeRolloverThreshold = "SafeRolloverThreshold";
const char* const PropFailSafeLogsDir = "FailSafeLogsDir";
const char* const DefaultFailSafeLogsDir = "./FailSafeLogs";
const char* const PropDecoupledLogging = "DecoupledLogging";

interface ILogFailSafe : IInterface
{
    virtual void Add(const char*, IPropertyTree* scriptValues, const char *strContents, CLogRequestInFile* reqInFile)=0;//
    virtual void Add(const char*, IPropertyTree* scriptValues, IInterface& pIn, CLogRequestInFile* reqInFile)=0;
    virtual StringBuffer& GenerateGUID(StringBuffer& GUID,const char* seed="") = 0;
    virtual void AddACK(const char* GUID)=0;
    virtual void RollCurrentLog()=0;
    virtual void RollOldLogs()=0;
    virtual bool FindOldLogs() = 0;
    virtual void LoadOldLogs(StringArray& oldLogData) = 0;
    virtual void SplitLogRecord(const char* requestStr,StringBuffer& GUID, StringBuffer& Cache)=0;
    virtual void SafeRollover() = 0;

    virtual void RolloverAllLogs()=0;//
    virtual bool PopPendingLogRecord(StringBuffer& GUID, StringBuffer& cache) = 0;//
    virtual bool canRollCurrentLog() = 0;
};

extern LOGGINGCOMMON_API ILogFailSafe* createFailSafeLogger(IPropertyTree* cfg, const char* logType="");
extern LOGGINGCOMMON_API ILogFailSafe* createFailSafeLogger(IPropertyTree* cfg, const char* pszService, const char* logType="");

class CLogFailSafe : implements ILogFailSafe, public CInterface
{
    CLogSerializer m_Added;
    CLogSerializer m_Cleared;
    static CLogFailSafe* m_Instance;
    StringBuffer m_LogType;
    StringArray m_UnsentLogs;
    StringBuffer m_logsdir;
    StringBuffer m_LogService;//
    StringArray oldLogs;

    CriticalSection m_critSec;//
    GuidMap m_PendingLogs;//
    unsigned long safeRolloverReqThreshold = DEFAULT_SAFE_ROLLOVER_REQ_THRESHOLD;
    unsigned long safeRolloverSizeThreshold = 0;
    bool decoupledLogging = false;

    void readCfg(IPropertyTree* cfg);
    void readSafeRolloverThresholdCfg(StringBuffer& safeRolloverThreshold);
    void createNew(const char* logType);
    void loadFailed(const char* logType);
    StringBuffer& getReceiveFileName(const char* sendFileName, StringBuffer& recieveName);
    StringBuffer& ExtractFileName(const char* fileName,StringBuffer& FileName);

    void loadPendingLogReqsFromExistingLogFiles();//
    void generateNewFileNames(StringBuffer& sendingFile, StringBuffer& receivingFile);//
public:
    IMPLEMENT_IINTERFACE;
    CLogFailSafe();
    CLogFailSafe(IPropertyTree* cfg, const char* logType);
    CLogFailSafe(IPropertyTree* cfg, const char* pszService, const char* logType);

    virtual ~CLogFailSafe();
    StringBuffer& GenerateGUID(StringBuffer& GUID,const char* seed="");
    virtual void Add(const char*, IPropertyTree* scriptValues, const char *strContents, CLogRequestInFile* reqInFile);//
    virtual void Add(const char*, IPropertyTree* scriptValues, IInterface& pIn, CLogRequestInFile* reqInFile);
    virtual void AddACK(const char* GUID);
    virtual void RollCurrentLog();
    virtual void RollOldLogs();
    virtual bool FindOldLogs();
    virtual void LoadOldLogs(StringArray& oldLogData);
    virtual void SplitLogRecord(const char* requestStr,StringBuffer& GUID, StringBuffer& Cache);

    virtual void SafeRollover();

    virtual void RolloverAllLogs();//
    virtual bool PopPendingLogRecord(StringBuffer& GUID, StringBuffer& cache);//
    virtual bool canRollCurrentLog() { return m_Added.getItemCount() == m_Cleared.getItemCount(); };
};

#endif // !_LOGFAILSAFE_HPP__
