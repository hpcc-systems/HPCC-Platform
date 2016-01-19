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

#ifndef _LOGSERIALIZER_HPP__
#define _LOGSERIALIZER_HPP__

#include "jlib.hpp"
#include "jstring.hpp"
#include "jutil.hpp" // for StringArray
#include "jmutex.hpp"
#include <map>
#include <string>
#include <set> //for GuidSet

typedef std::set<std::string> GuidSet;//
typedef std::map<std::string, std::string> GuidMap;

class CLogSerializer : public CInterface
{
    __int64 m_bytesWritten;
    unsigned long m_ItemCount;//
    CriticalSection crit;
protected:
    IFile* m_file;
    IFileIO* m_fileio;
    StringBuffer m_FileName;
    StringBuffer m_FilePath;
protected:
    __int64 WroteBytes();
    void Init();
    StringBuffer& GetRolloverFileName(StringBuffer& oldFile, StringBuffer& newfile, const char* newExtension);
public:
    bool EnsureDirectory(StringBuffer& Dir);
public:
    IMPLEMENT_IINTERFACE;
    CLogSerializer();
    CLogSerializer(const char* fileName);
    virtual ~CLogSerializer();
    void Open(const char* Directory,const char* NewFileName,const char* Prefix);
    void Close();
    void Remove();
    void Rollover(const char* ClosedPrefix);
    void Append(const char* GUID, const char* Data);
    void Remove(const char* GUID);
    virtual void SplitRecord(StringBuffer& FullStr, StringBuffer& GUID, StringBuffer& Cache){}
    static void splitLogRecord(MemoryBuffer& rawdata,StringBuffer& GUID, StringBuffer& data);//
    const char* queryFileName(){return m_FileName.str();};
    const char* queryFilePath(){return m_FilePath.str();};

    void SafeRollover(const char* Directory,const char* NewFileName,const char* Prefix, const char* ClosedPrefix);

    unsigned long getItemCount() const { return m_ItemCount; }//
    static StringBuffer& extractFileName(const char* fullName, StringBuffer& fileName);//
    virtual void loadSendLogs(GuidSet& ACKSet, GuidMap& MissedLogs, unsigned long& total_missed);//
    virtual void loadAckedLogs(GuidSet& ReceiveMap);//
};

class CSendLogSerializer : public CLogSerializer
{
public:
    IMPLEMENT_IINTERFACE;
    CSendLogSerializer();
    CSendLogSerializer(const char* fileName) : CLogSerializer(fileName) {};

    virtual void LoadDataMap(GuidMap& ACKMap,StringArray& MissedLogs);
    void LoadMap(MemoryBuffer& rawdata,StringBuffer& GUID, StringBuffer& data);
};

void SplitRecord(const char* FullStr, StringBuffer& GUID, StringBuffer& Cache);

class CRecieveLogSerializer : public CLogSerializer
{
public:
    IMPLEMENT_IINTERFACE;
    CRecieveLogSerializer();
    CRecieveLogSerializer(const char* fileName) : CLogSerializer(fileName){}
    virtual void LoadDataMap(GuidMap& GUIDmap);
    void LoadMap(MemoryBuffer& data,GuidMap& GUIDmap, bool printTrace = false);
};

#endif // !LOGSERIALIZER
