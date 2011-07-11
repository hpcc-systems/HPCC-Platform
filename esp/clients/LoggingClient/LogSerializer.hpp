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

// LogSerializer.h: interface for the CLogSerializer class.
//
//////////////////////////////////////////////////////////////////////

#ifndef _LOGSERIALIZER_HPP__
#define _LOGSERIALIZER_HPP__

#include "jiface.hpp"
#include "jstring.hpp"
#include "jutil.hpp" // for StringArray
#include "jmutex.hpp"
#include   <map>
#include   <string>


typedef std::map<std::string, std::string> GuidMap;



class CLogSerializer : public CInterface  
{
    __int64 m_bytesWritten;
    long m_ItemCount;
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
    const char* queryFileName(){return m_FileName.str();};
    const char* queryFilePath(){return m_FilePath.str();};

    void SafeRollover(const char* Directory,const char* NewFileName,const char* Prefix, const char* ClosedPrefix);
};

///////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
class CSendLogSerializer : public CLogSerializer  
{
public:
    IMPLEMENT_IINTERFACE;
    CSendLogSerializer();
    CSendLogSerializer(const char* fileName) : CLogSerializer(fileName)
    {
    }
    virtual void LoadDataMap(GuidMap& ACKMap,StringArray& MissedLogs);
    void LoadMap(MemoryBuffer& rawdata,StringBuffer& GUID, StringBuffer& data);
};

void SplitRecord(const char* FullStr, StringBuffer& GUID, StringBuffer& Cache);

///////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
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
