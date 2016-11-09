/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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


// LogFailSafe.h: interface for the CLogFailSafe class.
//
//////////////////////////////////////////////////////////////////////
#pragma warning (disable : 4786)
#ifndef _LOGFAILSAFE_HPP__
#define _LOGFAILSAFE_HPP__

#ifdef LOGGINGCLIENT_EXPORTS
    #define WSLOGFAILSAFE_API DECL_EXPORT
#else
    #define WSLOGFAILSAFE_API DECL_IMPORT
#endif


#include "jlib.hpp"
#include "jstring.hpp"
#include "jutil.hpp" // for StringArray
#include "LogSerializer.hpp"


interface ILogFailSafe : IInterface
{
    virtual void Add(const char*,IInterface& pIn)=0;
    virtual StringBuffer& GenerateGUID(StringBuffer& GUID,const char* seed="") = 0;
    virtual void AddACK(const char* GUID)=0;
    virtual void RollCurrentLog()=0;
    virtual void RollOldLogs()=0;
    virtual bool FindOldLogs() = 0;
    virtual void LoadOldLogs(StringArray& oldLogData) = 0;
    virtual void SplitLogRecord(const char* requestStr,StringBuffer& GUID, StringBuffer& Cache)=0;
    virtual void SafeRollover() = 0;
};

extern "C" WSLOGFAILSAFE_API ILogFailSafe * createFailsafelogger(const char* logType="", const char* logsdir="./logs");

//MORE: This should probably be in the cpp file
class CLogFailSafe : implements ILogFailSafe, public CInterface
{
    CLogSerializer m_Added;
    CLogSerializer m_Cleared;
    static CLogFailSafe* m_Instance;
    StringBuffer m_LogType;
    StringArray m_UnsentLogs;
    StringBuffer m_logsdir;

private:
    void createNew(const char* logType);
    void loadFailed(const char* logType);
    StringBuffer& getRecieveFileName(const char* sendFileName, StringBuffer& recieveName);
    StringBuffer& ExtractFileName(const char* fileName,StringBuffer& FileName);
public:
    IMPLEMENT_IINTERFACE;
    CLogFailSafe();
    CLogFailSafe(const char* logType, const char* logsdir);
    
    virtual ~CLogFailSafe();
    StringBuffer& GenerateGUID(StringBuffer& GUID,const char* seed="");
    virtual void Add(const char*,IInterface& pIn);
    virtual void AddACK(const char* GUID);
    virtual void RollCurrentLog();
    virtual void RollOldLogs();
    virtual bool FindOldLogs();
    virtual void LoadOldLogs(StringArray& oldLogData);
    virtual void SplitLogRecord(const char* requestStr,StringBuffer& GUID, StringBuffer& Cache);

    virtual void SafeRollover();
    
};

#endif // !_LOGFAILSAFE_HPP__
