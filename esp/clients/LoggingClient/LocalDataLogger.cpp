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



// LocalDataLogger.cpp: implementation of the CLocalDataLogger class.
//
//////////////////////////////////////////////////////////////////////

#include "LocalDataLogger.hpp"
#include "jutil.hpp"
#include "jfile.ipp"

#define RETRIES 20


//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

CLocalDataLogger::CLocalDataLogger()
{

}

CLocalDataLogger::CLocalDataLogger(IPropertyTree *cfg, const char *process, const char *service, const char* UrlRoot)
{
    StringBuffer _directoryCache,xpath;
    
    xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/localCache/", process, service);
    IPropertyTree* cacheprop = cfg->queryBranch(xpath.str());
    if(cacheprop==0)
    {
        WARNLOG(-1,"No local cache defined for %s service.",service);
        return;
    }
    Init(cacheprop->queryProp("@cache"),cacheprop->queryProp("@fileExtension"),UrlRoot,cacheprop->getPropInt("@timerPeriod"),cacheprop->getPropInt("@cacheTimeout"));




}

CLocalDataLogger::CLocalDataLogger(const char* logDirectory,const char* ext,unsigned TimerPeriod,unsigned CacheTimeoutPeriod) 
{
    Init(logDirectory,ext,"",TimerPeriod,CacheTimeoutPeriod);
}

void CLocalDataLogger::Init(const char* logDirectory,const char* ext,const char* UrlRoot, unsigned TimerPeriod,unsigned CacheTimeoutPeriod)
{
    if(!logDirectory || !ext)
        throw MakeStringException(-1,"Invalid parameters passed to cLocalDataLogger::Init");
    StringBuffer pathtodir;
    pathtodir.appendf("%s",logDirectory);

    Owned<IFile> pDirectory = createIFile(pathtodir.str());
    if(pDirectory->exists() == false)
        pDirectory->createDirectory();

    m_UrlRoot.appendf("%s",UrlRoot);

    m_logDirectory.append(logDirectory);
    StringBuffer dirpath;
    dirpath.appendf("%s",logDirectory);
    m_CleanupThread.setown(new CFileCleanupThread(dirpath,ext,TimerPeriod,CacheTimeoutPeriod));
    m_CleanupThread->start();
}

CLocalDataLogger::~CLocalDataLogger()
{
    if (m_CleanupThread.get())
        m_CleanupThread->finish();
}

StringBuffer& CLocalDataLogger::generateUniqueName(StringBuffer& returnName) 
{
    CriticalBlock b(crit);
    Owned<IJlibDateTime> _timeNow =  createDateTimeNow();
    SCMStringBuffer _dateString;
    _timeNow->getDateString(_dateString);
    returnName.appendf("%u_%s",getRandom(),_dateString.str());
    return returnName;
}

StringBuffer& CLocalDataLogger::getFilePath(const char* fileName,StringBuffer& returnPath)
{
    returnPath.appendf("%s/%s.HTML",m_logDirectory.str(),fileName);
    return returnPath;
}

StringBuffer& CLocalDataLogger::writeData(const StringBuffer& dataToCache,const StringBuffer& tokenName,StringBuffer& returnPath)
{
    DBGLOG("CLocalDataLogger::writeData");
    StringBuffer tmpFile;
    getFilePath(tokenName.str(),tmpFile);
    Owned<IFile> file = createIFile(tmpFile.str());
    for (int i = 0; i < RETRIES; ++i)
    {
        try{
            Owned<IFileIO> io = file->open(IFOwrite);
            size32_t filesize = dataToCache.length();
            void* filedata = (void*)dataToCache.toCharArray();
            io->write(0,filesize ,filedata );
            if(m_UrlRoot.length()==0)
                returnPath.appendf("/Cache?Name=%s.HTML",tokenName.str());
            else
                returnPath.appendf("/%s/Cache?Name=%s.HTML",m_UrlRoot.str(),tokenName.str());
            break;
        }
        catch(IOSException *ose)
        {
            //The web site could be serving up the page as we try to update it...
            ose->Release();
            Sleep(10);
        }
        catch(...)
        {
            DBGLOG("Unknown exception thrown while reading local data logger file");
        }
    }
    
    return returnPath;
}

MemoryBuffer& CLocalDataLogger::readData(MemoryBuffer& dataCached,const char* cachedName)
{
    DBGLOG("CLocalDataLogger::readData");
    StringBuffer fileName;
    if (strstr(cachedName,"files_") != 0)
    {
        const char* filestr = strstr(cachedName,"files_") + 7;
        fileName.append(filestr);
    }
    else
        fileName.append(cachedName);

    
    StringBuffer filepath;
    //need to keep it consistant with the files_ method of obtaining files.....
    filepath.appendf("%s/%s",m_logDirectory.str(),fileName.str());
    try{
        DBGLOG("about to create file reference");
        Owned<IFile> file = createIFile(filepath);
        if (file)
        {
            for (int i = 0; i < RETRIES; ++i)
            {
                DBGLOG("Trying to open for the %d time",i);
                try
                {
                    DBGLOG("Trying to open %s",filepath.str());
                    Owned<IFileIO> io = file->open(IFOread);
                    if (io)
                    {
                        DBGLOG("Managed to open");
                        size32_t filesize = io->size();
                        io->read(0, filesize, dataCached.reserveTruncate(filesize));
                        DBGLOG("Managed to read");
                        return dataCached;
                    }
                }
                catch(IOSException *ose)
                {
                    //The web site could be serving up the page as we try to update it...
                    ose->Release();
                    Sleep(10);
                }
                catch(...)
                {
                    DBGLOG("Unknown exception thrown while reading local data logger file");
                }
            }
        }
    }
    catch(...)
    {
        DBGLOG("Unknown exception thrown while reading local data logger file2");
    }
    return dataCached;
}

