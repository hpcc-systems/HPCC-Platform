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

// LogSerializer.cpp: implementation of the CLogSerializer class.
//
//////////////////////////////////////////////////////////////////////
#pragma warning(disable : 4786)
#include "LogSerializer.hpp"
#include "jexcept.hpp"
#include "jfile.hpp"
#include "jlog.hpp"

#define TRACE_INTERVAL 100

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

CLogSerializer::CLogSerializer()
{
    Init();
}

CLogSerializer::CLogSerializer(const char* fileName)  
{
    m_FilePath.append(fileName);
    Init();
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
    //DBGLOG("Appening GUID %s with data %s",GUID,Data);
    CriticalBlock b(crit);
    m_ItemCount++;

    StringBuffer toWrite,size;
    
    toWrite.appendf("%s\t%s\r\n",GUID,Data);
    size.appendf("%d",toWrite.length());
    while (size.length() < 8)
        size.insert(0,'0');

    size.append("\t");
    toWrite.insert(0,size.str());
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
    m_FileName.append(Prefix);
    m_FileName.append("_");
    m_FileName.append(NewFileName);

    m_FilePath.append(m_FileName);
    
    DBGLOG("Creating tank file %s", m_FilePath.str());
    m_file = createIFile(m_FilePath.str());
    m_fileio  =  m_file->open(IFOcreate);
    if (m_fileio == 0)
        throw MakeStringException(-1, "Unable to open logging file %s",m_FilePath.str());
    else
        DBGLOG("Tank file %s successfully created", m_FilePath.str());

}

bool CLogSerializer::EnsureDirectory(StringBuffer& Dir)
{
    try{
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

}

void CLogSerializer::Rollover(const char* ClosedPrefix)
{
    Close();
    Owned<IFile> file = createIFile(m_FilePath.str()); 
    if(file.get() && file->exists() == true)
    {
        StringBuffer newFileName;
        GetRolloverFileName(m_FileName,newFileName,ClosedPrefix);
        DBGLOG("Rolling over %s to %s", m_FilePath.str(), newFileName.str());
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

StringBuffer& CLogSerializer::GetRolloverFileName(StringBuffer& oldFile, StringBuffer& newfile, const char* newExtension)
{
    newfile.append(oldFile);
    newfile.replaceString("log",newExtension);
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
    DBGLOG("Loading missed logs, if any");
    try{
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
    catch(...){
        DBGLOG("Unknown Exception thrown in CSendLogSerializer::LoadDataMap");
    }
    Close();
}

void CSendLogSerializer::LoadMap(MemoryBuffer& rawdata,StringBuffer& GUID, StringBuffer& line)
{
    //StringBuffer line;
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
    DBGLOG("Loading ACKMap");
    try{
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
    catch(...){
        DBGLOG("Unknown Exception thrown in CRecieveLogSerializer::LoadDataMap");
    }
    Close();
}


