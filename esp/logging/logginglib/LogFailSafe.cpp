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

#pragma warning(disable:4786)

/* Review notes: this file is modified based on:
 * HPCC-Platform/esp/clients/LoggingClient/LogFailSafe.cpp

   3 methods are added to ILogFailSafe: Add(), RolloverAllLogs(), PopPendingLogRecord().
   1 extern function is added: createFailSafeLogger()
   3 member variables are added to CLogFailSafe: m_LogService, m_critSec, m_PendingLogs
   5 methods and one constructor are added to CLogFailSafe: Add(), RolloverAllLogs(), PopPendingLogRecord(),
       loadPendingLogReqsFromExistingLogFiles(), generateNewFileNames()
   Minor changes are made in the existing methods.
 */
#include "LogFailSafe.hpp"
#include "jmisc.hpp"
#include "soapbind.hpp"

#define RECEIVING "_acked_"
#define SENDING   "_sending_"

const char* const RolloverExt=".old";
const unsigned long SAFE_ROLLOVER_THRESHOLD = 500000L;
const unsigned int TRACE_PENDING_LOGS_MIN = 10;
const unsigned int TRACE_PENDING_LOGS_MAX = 50;

extern LOGGINGCOMMON_API ILogFailSafe* createFailSafeLogger(const char* logType, const char* logsdir)
{
    return new CLogFailSafe(logType, logsdir);
}

extern LOGGINGCOMMON_API ILogFailSafe* createFailSafeLogger(const char* pszService, const char* logType, const char* logsdir)
{
    return new CLogFailSafe(pszService, logType, logsdir && *logsdir ? logsdir : "./FailSafeLogs");
}

CLogFailSafe::CLogFailSafe()
{
}

CLogFailSafe::CLogFailSafe(const char* logType, const char* logsdir) : m_LogType(logType), m_logsdir(logsdir)
{
    loadFailed(logType);
    createNew(logType);
}

CLogFailSafe::CLogFailSafe(const char* pszService, const char* logType, const char* logsdir)
    : m_LogService(pszService), m_LogType(logType), m_logsdir(logsdir)
{
    loadPendingLogReqsFromExistingLogFiles();

    StringBuffer send, receive;
    generateNewFileNames(send, receive);

    m_Added.Open(m_logsdir.str(), send.str(), NULL);
    m_Cleared.Open(m_logsdir.str(), receive.str(), NULL);
}

CLogFailSafe::~CLogFailSafe()
{
    ESPLOG(LogMax, "CLogFailSafe::~CLogFailSafe()");
    m_Added.Close();
    m_Cleared.Close();
}

bool CLogFailSafe::FindOldLogs()
{
    if(m_UnsentLogs.ordinality())
        return true;
    return false;
}

void CLogFailSafe::LoadOldLogs(StringArray& oldLogData)
{
    ForEachItemIn(aidx, m_UnsentLogs)
    {
        oldLogData.append(m_UnsentLogs.item(aidx));
    }
}

void CLogFailSafe::loadPendingLogReqsFromExistingLogFiles()
{
    VStringBuffer fileName("%s%s%s*.log", m_LogService.str(), m_LogType.str(), SENDING);
    Owned<IDirectoryIterator> di = createDirectoryIterator(m_logsdir.str(), fileName.str());
    ForEach (*di)
    {
        IFile &file = di->query();

        StringBuffer ackedName;
        GuidSet ackedSet;
        getReceiveFileName(file.queryFilename(),ackedName);
        CLogSerializer ackedLog(ackedName.str());
        ackedLog.loadAckedLogs(ackedSet);

        CLogSerializer sendLog(file.queryFilename());
        unsigned long total_missed = 0;
        {//scope needed for critical block below
            CriticalBlock b(m_critSec); //since we plan to use m_PendingLogs
            sendLog.loadSendLogs(ackedSet, m_PendingLogs, total_missed);
        }

        if (total_missed == 0)
        {
            ackedLog.Rollover(RolloverExt);
            sendLog.Rollover(RolloverExt);
        }
    }
}

void CLogFailSafe::generateNewFileNames(StringBuffer& sendingFile, StringBuffer& receivingFile)
{
    StringBuffer GUID;
    GenerateGUID(GUID);

    StringBuffer tmp;
    tmp.append(m_LogService).append(m_LogType);

    sendingFile.append(tmp).append(SENDING).append(GUID).append(".log");
    receivingFile.append(tmp).append(RECEIVING).append(GUID).append(".log");
}

bool CLogFailSafe::PopPendingLogRecord(StringBuffer& GUID, StringBuffer& cache)
{
    CriticalBlock b(m_critSec);

    GuidMap::iterator it = m_PendingLogs.begin();
    if (it == m_PendingLogs.end())
        return false;

    GUID.clear().append( (*it).first.c_str() );
    cache.clear().append( (*it).second.c_str() );
    m_PendingLogs.erase(it);

    unsigned int nPendingLogs = m_PendingLogs.size();
    if (nPendingLogs && (nPendingLogs < TRACE_PENDING_LOGS_MIN || (nPendingLogs % TRACE_PENDING_LOGS_MAX == 0)))
        ESPLOG(LogNormal, "%u logs pending", nPendingLogs);

    return true;
}

void CLogFailSafe::createNew(const char* logType)
{
    StringBuffer UniqueID;
    GenerateGUID(UniqueID);
    UniqueID.append(".log");

    StringBuffer send(logType),recieve(logType);

    send.append("_sending");
    recieve.append("_recieving");

    m_Added.Open(m_logsdir.str(),UniqueID,send.str());
    m_Cleared.Open(m_logsdir.str(),UniqueID,recieve.str());
}

void CLogFailSafe::loadFailed(const char* logType)
{
    StringBuffer fileName;
    fileName.appendf("%s_sending*.log",logType);

    Owned<IDirectoryIterator> di = createDirectoryIterator(m_logsdir.str(), fileName.str());
    ForEach (*di)
    {
        IFile &file = di->query();

        StringBuffer recieveName;
        GuidMap recieve_map;
        getReceiveFileName(file.queryFilename(),recieveName);

        CRecieveLogSerializer recieveLog(recieveName.str());
        recieveLog.LoadDataMap(recieve_map);

        CSendLogSerializer sendLog(file.queryFilename());
        sendLog.LoadDataMap(recieve_map,m_UnsentLogs);
    }
}

StringBuffer& CLogFailSafe::getReceiveFileName(const char* sendFileName, StringBuffer& receiveName)
{
    if (sendFileName)
    {
        receiveName.append(sendFileName);
        receiveName.replaceString(SENDING, RECEIVING);
    }
    return receiveName;
}

StringBuffer& CLogFailSafe::GenerateGUID(StringBuffer& GUID, const char* seed)
{
    GUID.appendf("%u",getRandom());
    while (GUID.length() < 10)
        GUID.insert(0,'0');

    addFileTimestamp(GUID);
    if(seed!=NULL && *seed!='\0')
        GUID.appendf(".%s",seed);
    return GUID;
}

void CLogFailSafe::SplitLogRecord(const char* requestStr,StringBuffer& GUID, StringBuffer& Cache)
{
    SplitRecord(requestStr,GUID,Cache);
}

void CLogFailSafe::Add(const char* GUID,IInterface& pIn)
{
    CSoapRequestBinding* reqObj = dynamic_cast<CSoapRequestBinding*>(&pIn);
    if (reqObj == 0)
        throw MakeStringException(-1, "Unable to cast interface to SoapBindind");

    StringBuffer dataStr;
    reqObj->serializeContent(NULL,dataStr,NULL);
    Add(GUID, dataStr);
}

void CLogFailSafe::Add(const char* GUID, const StringBuffer& strContents)
{
    VStringBuffer dataStr("<cache>%s</cache>", strContents.str());

    unsigned long item_count = m_Added.getItemCount();
    if (item_count > SAFE_ROLLOVER_THRESHOLD)
        SafeRollover();

    m_Added.Append(GUID, dataStr.str());
}

void CLogFailSafe::AddACK(const char* GUID)
{
    m_Cleared.Append(GUID, "");

    CriticalBlock b(m_critSec);
    GuidMap::iterator it = m_PendingLogs.find(GUID);
    if (it != m_PendingLogs.end())
        m_PendingLogs.erase(it);
}

void CLogFailSafe::RollCurrentLog()
{
    m_Added.Rollover(RolloverExt);
    m_Cleared.Rollover(RolloverExt);
}

void CLogFailSafe::SafeRollover()
{
    StringBuffer send, receive;
    generateNewFileNames(send, receive);

    // Rolling over m_Added first is desirable here beccause requests being written to the new tank file before
    // m_Cleared finishes rolling all haven't been sent yet (because the sending thread is here busy rolling).
    m_Added.SafeRollover  (m_logsdir.str(), send.str(), NULL,   RolloverExt);
    m_Cleared.SafeRollover(m_logsdir.str(), receive.str(), NULL, RolloverExt);
}

void CLogFailSafe::RollOldLogs()
{
    StringBuffer filesToFind;
    filesToFind.appendf("%s*.log",m_LogType.str());

    Owned<IDirectoryIterator> di = createDirectoryIterator(m_logsdir.str(), filesToFind.str());
    ForEach (*di)
    {
        IFile &file = di->query();

        StringBuffer fileName;
        ExtractFileName(file.queryFilename(),fileName);
        if (fileName.length() && strcmp(fileName.str(),m_Added.queryFileName()) != 0 &&  strcmp(fileName.str(),m_Cleared.queryFileName()) != 0 )
        {
            fileName.replaceString(".log", RolloverExt);
            file.rename(fileName.str());
        }
    }
}

//Rename existing .log files (except for current added/cleared log files) to .old files
void CLogFailSafe::RolloverAllLogs()
{
    VStringBuffer filesToFind("%s%s*.log", m_LogService.str(), m_LogType.str());
    Owned<IDirectoryIterator> di = createDirectoryIterator(m_logsdir.str(), filesToFind.str());
    ForEach (*di)
    {
        IFile &file = di->query();

        StringBuffer fileName;
        CLogSerializer::extractFileName(file.queryFilename(), fileName);
        if (fileName.length() && !streq(fileName.str(), m_Added.queryFileName()) &&
            !streq(fileName.str(), m_Cleared.queryFileName()))
        {
            fileName.replaceString(".log", RolloverExt);
            file.rename(fileName.str());
        }
    }
}

StringBuffer& CLogFailSafe::ExtractFileName(const char* fileName,StringBuffer& FileName)
{
    StringBuffer tmp(fileName);
    for(int i = tmp.length() - 1; i >=0; i--)
    {
        if(tmp.charAt(i) == '\\' || tmp.charAt(i) == '/')
            break;
        FileName.insert(0,tmp.charAt(i));
    }
    return FileName;
}
