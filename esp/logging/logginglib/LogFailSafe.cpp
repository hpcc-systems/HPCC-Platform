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

const unsigned int TRACE_PENDING_LOGS_MIN = 10;
const unsigned int TRACE_PENDING_LOGS_MAX = 50;

extern LOGGINGCOMMON_API ILogFailSafe* createFailSafeLogger(IPropertyTree* cfg, const char* logType)
{
    return new CLogFailSafe(cfg, logType);
}

extern LOGGINGCOMMON_API ILogFailSafe* createFailSafeLogger(IPropertyTree* cfg, const char* pszService, const char* logType)
{
    return new CLogFailSafe(cfg, pszService, logType);
}

CLogFailSafe::CLogFailSafe()
{
}

CLogFailSafe::CLogFailSafe(IPropertyTree* cfg, const char* logType) : m_LogType(logType)
{
    readCfg(cfg);
    loadFailed(logType);
    createNew(logType);
}

CLogFailSafe::CLogFailSafe(IPropertyTree* cfg, const char* pszService, const char* logType)
    : m_LogService(pszService), m_LogType(logType)
{
    readCfg(cfg);
    if (!decoupledLogging)
        loadPendingLogReqsFromExistingLogFiles();

    StringBuffer send, receive;
    generateNewFileNames(send, receive);

    m_Added.Open(m_logsdir.str(), send.str(), NULL);
    if (!decoupledLogging)
        m_Cleared.Open(m_logsdir.str(), receive.str(), NULL);
}

CLogFailSafe::~CLogFailSafe()
{
    ESPLOG(LogMax, "CLogFailSafe::~CLogFailSafe()");
    m_Added.Close();
    if (!decoupledLogging)
        m_Cleared.Close();
}

void CLogFailSafe::readCfg(IPropertyTree* cfg)
{
    StringBuffer safeRolloverThreshold(cfg->queryProp(PropSafeRolloverThreshold));
    if (!safeRolloverThreshold.isEmpty())
        readSafeRolloverThresholdCfg(safeRolloverThreshold);

    const char* logsDir = cfg->queryProp(PropFailSafeLogsDir);
    if (!isEmptyString(logsDir))
        m_logsdir.set(logsDir);
    else
        m_logsdir.set(DefaultFailSafeLogsDir);
    decoupledLogging = cfg->getPropBool(PropDecoupledLogging, false);
}

void CLogFailSafe::readSafeRolloverThresholdCfg(StringBuffer& safeRolloverThreshold)
{
    safeRolloverThreshold.trim();

    const char* ptrStart = safeRolloverThreshold.str();
    const char* ptrAfterDigit = ptrStart;
    while (*ptrAfterDigit && isdigit(*ptrAfterDigit))
        ptrAfterDigit++;

    if (!*ptrAfterDigit)
    {
        safeRolloverReqThreshold = atol(safeRolloverThreshold.str());
        return;
    }

    const char* ptr = ptrAfterDigit;
    while (*ptr && (ptr[0] == ' '))
        ptr++;
    char c = ptr[0];
    safeRolloverThreshold.setLength(ptrAfterDigit - ptrStart);
    safeRolloverSizeThreshold = atol(safeRolloverThreshold.str());
    switch (c)
    {
    case 'k':
    case 'K':
        safeRolloverSizeThreshold *= 1000;
        break;
    case 'm':
    case 'M':
        safeRolloverSizeThreshold *= 1000000;
        break;
    case 'g':
    case 'G':
        safeRolloverSizeThreshold *= 1000000000;
        break;
    case 't':
    case 'T':
        safeRolloverSizeThreshold *= 1000000000000;
        break;
    default:
        break;
    }
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
    //Read acked LogReqs from all of ack files.
    //Filter out those acked LogReqs from all of sending files.
    //Non-acked LogReqs are added into m_PendingLogs.
    //The LogReqs in the m_PendingLogs will be added to queue and
    //new tank files through the enqueue() in the checkPendingLogs().
    //After that, the oldLogs will be renamed to .old file.
    GuidSet ackedSet;
    VStringBuffer fileName("%s%s%s*%s", m_LogService.str(), m_LogType.str(), RECEIVING, logFileExt);
    Owned<IDirectoryIterator> it = createDirectoryIterator(m_logsdir.str(), fileName.str());
    ForEach (*it)
    {
        IFile &file = it->query();
        CLogSerializer ackedLog(file.queryFilename());
        ackedLog.loadAckedLogs(ackedSet);
        oldLogs.append(file.queryFilename());
    }

    fileName.setf("%s%s%s*%s", m_LogService.str(), m_LogType.str(), SENDING, logFileExt);
    Owned<IDirectoryIterator> di = createDirectoryIterator(m_logsdir.str(), fileName.str());
    ForEach (*di)
    {
        IFile &file = di->query();
        oldLogs.append(file.queryFilename()); //add to the files for rollover
        CLogSerializer sendLog(file.queryFilename());
        unsigned long total_missed = 0;
        sendLog.loadSendLogs(ackedSet, m_PendingLogs, total_missed);
    }
}

void CLogFailSafe::generateNewFileNames(StringBuffer& sendingFile, StringBuffer& receivingFile)
{
    StringBuffer GUID;
    GenerateGUID(GUID);

    StringBuffer tmp;
    tmp.append(m_LogService).append(m_LogType);
    sendingFile.append(tmp).append(SENDING).append(GUID).append(logFileExt);
    if (!decoupledLogging)
        receivingFile.append(tmp).append(RECEIVING).append(GUID).append(logFileExt);
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
    UniqueID.append(logFileExt);

    StringBuffer send(logType),recieve(logType);

    send.append("_sending");
    recieve.append("_recieving");

    m_Added.Open(m_logsdir.str(),UniqueID,send.str());
    m_Cleared.Open(m_logsdir.str(),UniqueID,recieve.str());
}

void CLogFailSafe::loadFailed(const char* logType)
{
    StringBuffer fileName;
    fileName.appendf("%s_sending*%s", logType, logFileExt);

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

void CLogFailSafe::Add(const char* GUID, IPropertyTree* scriptValues, IInterface& pIn, CLogRequestInFile* reqInFile)
{
    CSoapRequestBinding* reqObj = dynamic_cast<CSoapRequestBinding*>(&pIn);
    if (reqObj == 0)
        throw MakeStringException(-1, "Unable to cast interface to SoapBindind");

    StringBuffer dataStr;
    reqObj->serializeContent(NULL,dataStr,NULL);
    Add(GUID, scriptValues, dataStr, reqInFile);
}

void CLogFailSafe::Add(const char* GUID, IPropertyTree* scriptValues, const char *strContents, CLogRequestInFile* reqInFile)
{
    VStringBuffer dataStr("<cache>%s</cache>", strContents);

    if (safeRolloverSizeThreshold <= 0)
    {
        unsigned long item_count = m_Added.getItemCount();
        if (item_count > safeRolloverReqThreshold)
            SafeRollover();
    }
    else
    {
        unsigned long fileSize = m_Added.getFileSize();
        if (fileSize > safeRolloverSizeThreshold)
            SafeRollover();
    }
    m_Added.Append(GUID, scriptValues, dataStr, reqInFile);
}

void CLogFailSafe::AddACK(const char* GUID)
{
    m_Cleared.Append(GUID, nullptr, "", nullptr);

    CriticalBlock b(m_critSec);
    GuidMap::iterator it = m_PendingLogs.find(GUID);
    if (it != m_PendingLogs.end())
        m_PendingLogs.erase(it);
}

void CLogFailSafe::RollCurrentLog()
{
    m_Added.Rollover(rolloverFileExt);
    m_Cleared.Rollover(rolloverFileExt);
}

void CLogFailSafe::SafeRollover()
{
    StringBuffer send, receive;
    generateNewFileNames(send, receive);

    // Rolling over m_Added first is desirable here beccause requests being written to the new tank file before
    // m_Cleared finishes rolling all haven't been sent yet (because the sending thread is here busy rolling).
    m_Added.SafeRollover  (m_logsdir.str(), send.str(), nullptr, rolloverFileExt);
    m_Cleared.SafeRollover(m_logsdir.str(), receive.str(), nullptr, rolloverFileExt);
}

//Only rollover the files inside the oldLogs.
//This is called only when a log agent is starting.
void CLogFailSafe::RollOldLogs()
{
    ForEachItemIn(i, oldLogs)
    {
        StringBuffer fileName(oldLogs.item(i));
        Owned<IFile> file = createIFile(fileName);
        fileName.replaceString(logFileExt, rolloverFileExt);
        file->rename(fileName.str());
    }
    oldLogs.kill();
}

//Rename existing .log files (except for current added/cleared log files) to .old files
void CLogFailSafe::RolloverAllLogs()
{
    VStringBuffer filesToFind("%s%s*%s", m_LogService.str(), m_LogType.str(), logFileExt);
    Owned<IDirectoryIterator> di = createDirectoryIterator(m_logsdir.str(), filesToFind.str());
    ForEach (*di)
    {
        IFile &file = di->query();

        StringBuffer fileName;
        CLogSerializer::extractFileName(file.queryFilename(), fileName);
        if (fileName.length() && !streq(fileName.str(), m_Added.queryFileName()) &&
            !streq(fileName.str(), m_Cleared.queryFileName()))
        {
            fileName.replaceString(logFileExt, rolloverFileExt);
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
