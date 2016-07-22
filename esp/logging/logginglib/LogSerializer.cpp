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

#pragma warning(disable : 4786)
#include "LogSerializer.hpp"
#include "jexcept.hpp"
#include "jfile.hpp"
#include "jlog.hpp"

/* Review notes: this file is modified based on:
 * HPCC-Platform/esp/clients/LoggingClient/LogSerializer.cpp

   5 methods are added to CLogSerializer: splitLogRecord(), getItemCount(), extractFileName(),
       loadSendLogs(), loadAckedLogs.
   Minor changes are made in the existing methods.
 */

#define TRACE_INTERVAL 100

CLogSerializer::CLogSerializer()
{
    Init();
}

CLogSerializer::CLogSerializer(const char* fileName)
{
    m_FilePath.append(fileName);
    Init();
    extractFileName(m_FilePath, m_FileName);//
}

StringBuffer& CLogSerializer::extractFileName(const char* fullName,StringBuffer& fileName)
{
    StringBuffer tmp(fullName);
    for(unsigned i = tmp.length(); i-- > 0; )
    {
        if(tmp.charAt(i) == '\\' || tmp.charAt(i) == '/')
            break;
        fileName.insert(0, tmp.charAt(i));
    }
    return fileName;
}

void CLogSerializer::Init()
{
    m_bytesWritten  = 0;
    m_ItemCount     = 0;
    m_fileio        = 0;
    m_file          = 0;
}

CLogSerializer::~CLogSerializer()
{
    DBGLOG("CLogSerializer::~CLogSerializer()");
    Close();
}

void CLogSerializer::Append(const char* GUID, const char* Data)
{
    StringBuffer toWrite,size;

    toWrite.appendf("%s\t%s\r\n",GUID,Data);
    size.appendf("%d",toWrite.length());
    while (size.length() < 8)
        size.insert(0,'0');

    size.append("\t");
    toWrite.insert(0,size.str());

    //optimize
    CriticalBlock b(crit);
    m_ItemCount++;
    m_bytesWritten += m_fileio->write(m_bytesWritten, toWrite.length(), toWrite.str());
}

void CLogSerializer::Remove(const char* GUID)
{
}

void CLogSerializer::Open(const char*Directory,const char* NewFileName,const char* Prefix)
{
    m_FilePath.clear();
    m_FilePath.append(Directory);
    if (!EnsureDirectory(m_FilePath))
        throw MakeStringException(-1,"Unable to create directory at %s.",m_FilePath.str());

    m_FilePath.append("/");

    m_FileName.clear();
    if (Prefix && *Prefix)
        m_FileName.append(Prefix).append("_");
    m_FileName.append(NewFileName);
    m_FilePath.append(m_FileName);

    m_file = createIFile(m_FilePath.str());
    m_fileio  =  m_file->open(IFOcreate);
    if (m_fileio == 0)
        throw MakeStringException(-1, "Unable to open logging file %s",m_FilePath.str());
    else
        DBGLOG("Tank file %s successfully created", m_FilePath.str());
}

bool CLogSerializer::EnsureDirectory(StringBuffer& Dir)
{
    try
    {
        Owned<IFile> pDirectory = createIFile(Dir.str());
        if(pDirectory->exists() == true)
            return true;
        return pDirectory->createDirectory();
    }
    catch(IException *ex)
    {
        ex->Release();
    }
    return false;
}

void CLogSerializer::Close()
{
    if(m_fileio)
    {
        m_fileio->Release();
        m_fileio = 0;
    }

    if(m_file)
    {
        m_file->Release();
        m_file = 0;
    }

    m_bytesWritten  = 0;//
    m_ItemCount     = 0;//
}

void CLogSerializer::Rollover(const char* ClosedPrefix)
{
    Close();
    Owned<IFile> file = createIFile(m_FilePath.str());
    if(file.get() && file->exists() == true)
    {
        StringBuffer newFileName;
        GetRolloverFileName(m_FileName,newFileName,ClosedPrefix);
        file->rename(newFileName.str());
    }
}

void CLogSerializer::SafeRollover(const char*Directory,const char* NewFileName,const char* Prefix, const char* ClosedPrefix)
{
    CriticalBlock b(crit);
    Rollover(ClosedPrefix);
    Init();
    Open(Directory, NewFileName, Prefix);
}

void CLogSerializer::splitLogRecord(MemoryBuffer& rawdata, StringBuffer& GUID, StringBuffer& line)//
{
    //send log buffer should be in the form of 2635473460.05_01_12_16_13_57\t<cache>...</cache>
    //parse it into GUID and line (as <cache>...</cache>)

    //receive log buffer should be in the form of 2515777767.12_11_03_08_25_29\t
    //we want to extract the GUID only

    const char* begin = rawdata.toByteArray(); //no string termination character \0
    int len = rawdata.length();
    if (begin && len>0)
    {
        const char* p = begin;
        const char* end = begin + len;

        while (*p && *p != '\t' && p < end)
            p++;

        GUID.append(p-begin, begin);

        if (++p < end)
            line.append(end-p, p);
    }
}

void CLogSerializer::loadSendLogs(GuidSet& ackSet, GuidMap& missedLogs, unsigned long& total_missed)//
{
    try
    {
        Close(); //release old file io, if any
        m_file = createIFile(m_FilePath.str());
        m_fileio = m_file->open(IFOread);
        if (m_fileio == 0)
            throw MakeStringException(-1, "Unable to open logging file %s",m_FilePath.str());

        offset_t finger = 0;
        total_missed = 0;
        while(true)
        {
            char dataSize[9];
            memset(dataSize, 0, 9);
            size32_t bytesRead = m_fileio->read(finger,8,dataSize);
            if(bytesRead==0)
                break;

            MemoryBuffer data;
            int dataLen = atoi(dataSize);
            finger+=9;
            bytesRead = m_fileio->read(finger,dataLen,data.reserveTruncate(dataLen));
            if(bytesRead==0)
                break;

            StringBuffer GUID,lostlogStr;
            splitLogRecord(data,GUID,lostlogStr);

            if (ackSet.find(GUID.str())==ackSet.end() && missedLogs.find(GUID.str()) == missedLogs.end())
            {
                if(total_missed % TRACE_INTERVAL == 0)
                    DBGLOG("Miss #%lu GUID: <%s>", total_missed, GUID.str());
                missedLogs[GUID.str()] = lostlogStr.str();
                total_missed++;
            }
            finger+=dataLen;
        }
    }
    catch(IException* ex)
    {
        StringBuffer errorStr;
        ex->errorMessage(errorStr);
        ERRLOG("Exception caught within CSendLogSerializer::LoadDataMap: %s",errorStr.str());
        ex->Release();
    }
    catch(...)
    {
        DBGLOG("Unknown Exception thrown in CSendLogSerializer::LoadDataMap");
    }
    Close();
}

void CLogSerializer::loadAckedLogs(GuidSet& ackedLogs)//
{
    try
    {
        Close(); //release old file io, if any
        m_file = createIFile(m_FilePath.str());
        m_fileio = m_file->open(IFOread);
        if (m_fileio == 0)
            throw MakeStringException(-1, "Unable to open logging file %s",m_FilePath.str());

        offset_t finger = 0;
        m_ItemCount = 0;
        while(true)
        {
            char dataSize[9];
            memset(dataSize, 0, 9);
            size32_t bytesRead = m_fileio->read(finger,8,dataSize);
            if(bytesRead==0)
                break;

            MemoryBuffer data;
            int dataLen = atoi(dataSize);
            finger+=9;
            bytesRead = m_fileio->read(finger,dataLen,data.reserveTruncate(dataLen));
            if(bytesRead==0)
                break;

            StringBuffer GUID, line;
            splitLogRecord(data, GUID, line);
            ackedLogs.insert(GUID.str());
            m_ItemCount++;

            finger+=dataLen;
        }
        DBGLOG("Total acks loaded %lu", m_ItemCount);
    }
    catch(IException* ex)
    {
        StringBuffer errorStr;
        ex->errorMessage(errorStr);
        ERRLOG("Exception caught within CLogSerializer::loadAckedLogs: %s",errorStr.str());
        ex->Release();
    }
    catch(...)
    {
        DBGLOG("Unknown Exception thrown in CLogSerializer::loadAckedLogs");
    }
    Close();
}

StringBuffer& CLogSerializer::GetRolloverFileName(StringBuffer& oldFile, StringBuffer& newfile, const char* newExtension)
{
    newfile.append(oldFile);
    newfile.replaceString(".log",newExtension);
    return newfile;
}

void CLogSerializer::Remove()
{
    Close();
    Owned<IFile> file = createIFile(m_FilePath.str());
    if(file.get() && file->exists() == true)
        file->remove();
}

__int64 CLogSerializer::WroteBytes()
{
    CriticalBlock b(crit);
    return m_bytesWritten;
}

void CSendLogSerializer::LoadDataMap(GuidMap& ACKMap,StringArray& MissedLogs)
{
    try
    {
        m_file = createIFile(m_FilePath.str());
        m_fileio = m_file->open(IFOread);
        if (m_fileio == 0)
            throw MakeStringException(-1, "Unable to open logging file %s",m_FilePath.str());
        else
            DBGLOG("File %s successfully opened", m_FilePath.str());

        long finger,bytesRead;
        finger = bytesRead = 0;

        bool bOk = true;
        MemoryBuffer dataSize,data;
        StringBuffer GUID,lostlogStr;
        int dataLen;
        unsigned int total = 0;
        unsigned int total_missed = 0;
        while(bOk)
        {
            bytesRead = m_fileio->read(finger,8,dataSize.reserveTruncate(8));
            if(bytesRead==0)
                break;

            finger+=9;
            dataLen = atoi(dataSize.toByteArray());

            bytesRead = m_fileio->read(finger,dataLen,data.reserveTruncate(dataLen));
            if(bytesRead==0)
                break;
            LoadMap(data,GUID,lostlogStr);
            if(total % TRACE_INTERVAL == 0)
            {
                DBGLOG("Checking log #%u", total);
                DBGLOG("{%s}", GUID.str());
            }
            total++;
            int i = MissedLogs.find(lostlogStr.str());
            if (!(*(ACKMap[GUID.str()].c_str())) && MissedLogs.find(lostlogStr.str()) == -1)
            {
                if(total_missed % TRACE_INTERVAL == 0)
                {
                    DBGLOG("Miss #%u", total_missed);
                    DBGLOG("<%s>", GUID.str());
                }
                MissedLogs.append(lostlogStr.str());
                total_missed++;
            }
            finger+=dataLen;
            data.clear();
            dataSize.clear();
            GUID.clear();
            lostlogStr.clear();
        }
        DBGLOG("Total logs checked %u, total missed %u", total, total_missed);
    }
    catch(IException* ex)
    {
        StringBuffer errorStr;
        ex->errorMessage(errorStr);
        ERRLOG("Exception caught within CSendLogSerializer::LoadDataMap: %s",errorStr.str());
        ex->Release();
    }
    catch(...)
    {
        DBGLOG("Unknown Exception thrown in CSendLogSerializer::LoadDataMap");
    }
    Close();
}

void CSendLogSerializer::LoadMap(MemoryBuffer& rawdata,StringBuffer& GUID, StringBuffer& line)
{
    line.append(rawdata.length() -1, rawdata.toByteArray());
    const char* strLine = line.str();
    while(*strLine && *strLine != '\t' && *strLine != '\0')
    {
        GUID.append(*strLine);
        strLine++;
    }
}

void SplitRecord(const char*  strLine, StringBuffer& GUID, StringBuffer& Cache)
{
    if(strLine==NULL || *strLine=='\0')
        return;

    while(*strLine && *strLine != '\t' && *strLine != '\0')
    {
        GUID.append(*strLine);
        strLine++;
    }
    strLine++;
    Cache.appendf("%s",strLine);
}

void CRecieveLogSerializer::LoadMap(MemoryBuffer& rawdata,GuidMap& GUIDmap, bool printTrace)
{
    //buffer chould be in the form of 000000030\t2515777767.12_11_03_08_25_29\r
    //we want to extract the GUID only....
    StringBuffer line,GUID;
    line.append(rawdata.length() -1, rawdata.toByteArray());
    const char* strLine = line.str();
    while(*strLine && *strLine != '\t' && *strLine != '\0')
    {
        GUID.append(*strLine);
        strLine++;
    }
    if(printTrace)
        DBGLOG("[%s]", GUID.str());
    GUIDmap[GUID.str()] = "1";
}

void CRecieveLogSerializer::LoadDataMap(GuidMap& GUIDmap)
{
    try
    {
        m_file = createIFile(m_FilePath.str());
        m_fileio = m_file->open(IFOread);
        if (m_fileio == 0)
            throw MakeStringException(-1, "Unable to open logging file %s",m_FilePath.str());
        else
            DBGLOG("File %s successfully opened", m_FilePath.str());

        long finger,bytesRead,dataLen;
        finger = bytesRead = dataLen = 0;
        MemoryBuffer filecontents,dataSize,data;
        bool bOk = true;
        unsigned int total = 0;
        while(bOk)
        {
            bytesRead = m_fileio->read(finger,8,dataSize.reserveTruncate(8));
            if(bytesRead==0)
                break;

            finger+=9;
            dataLen = atoi(dataSize.toByteArray());

            bytesRead = m_fileio->read(finger,dataLen,data.reserveTruncate(dataLen));
            if(bytesRead==0)
                break;
            bool printTrace = false;
            if(total % TRACE_INTERVAL == 0)
            {
                DBGLOG("Loading ack #%u", total);
                printTrace = true;
            }
            LoadMap(data,GUIDmap,printTrace);
            total++;

            finger+=dataLen;
            data.clear();
            dataSize.clear();
        }
        DBGLOG("Total acks loaded %u", total);
    }
    catch(IException* ex)
    {
        StringBuffer errorStr;
        ex->errorMessage(errorStr);
        ERRLOG("Exception caught within CRecieveLogSerializer::LoadDataMap: %s",errorStr.str());
        ex->Release();
    }
    catch(...)
    {
        DBGLOG("Unknown Exception thrown in CRecieveLogSerializer::LoadDataMap");
    }
    Close();
}
