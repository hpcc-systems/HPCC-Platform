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


// LogFailSafe.h: interface for the CLogFailSafe class.
//
//////////////////////////////////////////////////////////////////////
#pragma warning (disable : 4786)
#ifndef _LOGFAILSAFE_HPP__
#define _LOGFAILSAFE_HPP__

#ifdef WIN32
    #ifdef LOGGINGCLIENT_EXPORTS
        #define WSLOGFAILSAFE_API __declspec(dllexport)
    #else
        #define WSLOGFAILSAFE_API __declspec(dllimport)
    #endif
#else
    #define WSLOGFAILSAFE_API
#endif


#include "jiface.hpp"
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

class CLogFailSafe : public CInterface,  implements ILogFailSafe
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
