/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */


// LogFailSafe.cpp: implementation of the CLogFailSafe class.
//
//////////////////////////////////////////////////////////////////////
#pragma warning(disable:4786)

#include "LogFailSafe.hpp"
#include "jmisc.hpp"
#include "soapbind.hpp"

const char* const RolloverExt="old";

ILogFailSafe * createFailsafelogger(const char* logType, const char* logsdir)
{
    return new CLogFailSafe(logType, logsdir);
}


//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////
CLogFailSafe::CLogFailSafe()
{
    
}

CLogFailSafe::CLogFailSafe(const char* logType, const char* logsdir) : m_LogType(logType), m_logsdir(logsdir)
{
    loadFailed(logType);
    createNew(logType);
}
    
CLogFailSafe::~CLogFailSafe()
{
    DBGLOG("CLogFailSafe::~CLogFailSafe()");
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
    DBGLOG("Searching for files of type %s",fileName.str());

    Owned<IDirectoryIterator> di = createDirectoryIterator(m_logsdir.str(), fileName.str());
    ForEach (*di)
    {

        IFile &file = di->query();
        
        StringBuffer recieveName;
        
        
        GuidMap recieve_map; 
        
        getRecieveFileName(file.queryFilename(),recieveName);
        
        DBGLOG("Loading %s", recieveName.str());
        CRecieveLogSerializer recieveLog(recieveName.str());
        recieveLog.LoadDataMap(recieve_map);
            
        DBGLOG("Checking %s", file.queryFilename());
        CSendLogSerializer sendLog(file.queryFilename());
        sendLog.LoadDataMap(recieve_map,m_UnsentLogs);
    }
}

StringBuffer& CLogFailSafe::getRecieveFileName(const char* sendFileName, StringBuffer& recieveName)
{
    DBGLOG("enter getRecieveFileName");
    if(!sendFileName)
        return recieveName;
    recieveName.append(sendFileName);
    recieveName.replaceString("sending","recieving");
    DBGLOG("leave getRecieveFileName");
    return recieveName;
}

StringBuffer& CLogFailSafe::GenerateGUID(StringBuffer& GUID, const char* seed)
{
    GUID.appendf("%u",getRandom());
    while (GUID.length() < 10)
    {
        GUID.insert(0,'0');
    }
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
        throw MakeStringException(-1, "Unable to cast interface to SoapBindind");;      
    StringBuffer dataStr;
    reqObj->serializeContent(NULL,dataStr,NULL);
    dataStr.insert(0,"<cache>");
    dataStr.append("</cache>");
    m_Added.Append(GUID,dataStr.str());
}

void CLogFailSafe::AddACK(const char* GUID)
{
    m_Cleared.Append(GUID,"");
}

void CLogFailSafe::RollCurrentLog()
{
    m_Added.Rollover(RolloverExt);
    m_Cleared.Rollover(RolloverExt);
}

void CLogFailSafe::SafeRollover()
{
    StringBuffer UniqueID;
    GenerateGUID(UniqueID);
    UniqueID.append(".log");

    StringBuffer send(m_LogType),recieve(m_LogType);

    send.append("_sending");
    recieve.append("_recieving");
    
    // Rolling over m_Added first is desirable here beccause requests being written to the new tank file before
    // m_Cleared finishes rolling all haven't been sent yet (because the sending thread is here busy rolling).
    m_Added.SafeRollover(m_logsdir.str(),UniqueID,send.str(), RolloverExt);
    m_Cleared.SafeRollover(m_logsdir.str(),UniqueID,recieve.str(), RolloverExt);
}

void CLogFailSafe::RollOldLogs()
{
    StringBuffer filesToFind;
    filesToFind.appendf("%s*.log",m_LogType.str());

    DBGLOG("Rolling files of type %s",filesToFind.str());
    Owned<IDirectoryIterator> di = createDirectoryIterator(m_logsdir.str(), filesToFind.str());

    ForEach (*di)
    {
        IFile &file = di->query();
        StringBuffer fileName;

        ExtractFileName(file.queryFilename(),fileName);

        DBGLOG("File Name:%s",fileName.str());
        
        //const char* fileNameStr = strstr(file.queryFilename(),"\\")+1;

        if (fileName.length() && strcmp(fileName.str(),m_Added.queryFileName()) != 0 &&  strcmp(fileName.str(),m_Cleared.queryFileName()) != 0 )
        {

            DBGLOG("Rolling file:%s",fileName.str());
            fileName.replaceString(".log",".old");
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
