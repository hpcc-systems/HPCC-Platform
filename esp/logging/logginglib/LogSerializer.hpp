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

const char* const logFileExt = ".log";
const char* const rolloverFileExt = ".old";
const char* const logRequestScriptValues = "ScriptValues";

class CLogRequestInFile : public CSimpleInterface
{
    StringAttr fileName;
    offset_t pos;
    unsigned size = 0;
    StringAttr GUID;
    StringAttr option;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLogRequestInFile() { };

    const char* getFileName() { return fileName.get(); };
    void setFileName(const char* _fileName) { fileName.set(_fileName); };
    const char* getGUID() { return GUID.get(); };
    void setGUID(const char* _GUID) { GUID.set(_GUID); };
    const char* getOption() { return option.get(); };
    void setOption(const char* _option) { option.set(_option); };
    const offset_t getPos() { return pos; };
    void setPos(offset_t _pos) { pos = _pos; };
    const unsigned getSize() { return size; };
    void setSize(unsigned _size) { size = _size; };
};

class CLogSerializer : public CInterface
{
    __int64 m_bytesWritten;
    unsigned long m_ItemCount;//
    unsigned long fileSize;//
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
    CLogSerializer();
    CLogSerializer(const char* fileName);
    virtual ~CLogSerializer();
    void Open(const char* Directory,const char* NewFileName,const char* Prefix);
    void Close();
    void Remove();
    void Rollover(const char* ClosedPrefix);
    void Append(const char* GUID, IPropertyTree* scriptValues, const char* Data, CLogRequestInFile* reqInFile);
    void Remove(const char* GUID);
    virtual void SplitRecord(StringBuffer& FullStr, StringBuffer& GUID, StringBuffer& Cache){}
    static void splitLogRecord(MemoryBuffer& rawdata,StringBuffer& GUID, StringBuffer& data);//
    const char* queryFileName(){return m_FileName.str();};
    const char* queryFilePath(){return m_FilePath.str();};

    void SafeRollover(const char* Directory,const char* NewFileName,const char* Prefix, const char* ClosedPrefix);

    unsigned long getItemCount() const { return m_ItemCount; }//
    unsigned long getFileSize() const { return fileSize; }//
    static StringBuffer& extractFileName(const char* fullName, StringBuffer& fileName);//
    bool readLogRequest(CLogRequestInFile* file, StringBuffer& logRequest);
    virtual void loadSendLogs(GuidSet& ACKSet, GuidMap& MissedLogs, unsigned long& total_missed);//
    virtual void loadAckedLogs(GuidSet& ReceiveMap);//
    virtual bool readALogLine(IFileIO* fileIO, offset_t& readPos, MemoryBuffer& data);
};

class CSendLogSerializer : public CLogSerializer
{
public:
    CSendLogSerializer();
    CSendLogSerializer(const char* fileName) : CLogSerializer(fileName) {};

    virtual void LoadDataMap(GuidMap& ACKMap,StringArray& MissedLogs);
    void LoadMap(MemoryBuffer& rawdata,StringBuffer& GUID, StringBuffer& data);
};

void SplitRecord(const char* FullStr, StringBuffer& GUID, StringBuffer& Cache);

class CRecieveLogSerializer : public CLogSerializer
{
public:
    CRecieveLogSerializer();
    CRecieveLogSerializer(const char* fileName) : CLogSerializer(fileName){}
    virtual void LoadDataMap(GuidMap& GUIDmap);
    void LoadMap(MemoryBuffer& data,GuidMap& GUIDmap, bool printTrace = false);
};

#endif // !LOGSERIALIZER
