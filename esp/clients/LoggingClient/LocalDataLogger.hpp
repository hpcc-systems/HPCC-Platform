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

// LocalDataLogger.h: interface for the CLocalDataLogger class.
//
//////////////////////////////////////////////////////////////////////

#ifndef _LocalDataLogger_HPP__
#define _LocalDataLogger_HPP__

#ifdef LOGGINGCLIENT_EXPORTS
    #define WSLOGThread_API DECL_EXPORT
#else
    #define WSLOGThread_API DECL_IMPORT
#endif

#include "jiface.hpp"
#include "jstring.hpp"
#include "jthread.hpp"
#include "jlog.hpp"
#include "jfile.hpp"

class CFileCleanupThread : public Thread
{
private:
    StringBuffer Directory;
    StringBuffer Ext;
    unsigned m_TimerPeriod;
    long m_CacheTimeoutPeriod;
    Semaphore* m_sem;
    bool m_bRun;

    void CleanFiles()
    {
        //DBGLOG("directory len %d and ext len: %d",Directory.length(),Ext.length());
        
        if (Directory.length() == 0 || Ext.length() == 0)
            return;

        CDateTime currentTime;
        currentTime.setNow();

        int fileCounter = 0;
//      DBGLOG("Directory:%s for files of ext:%s",Directory.str(), Ext.str());
        Owned<IDirectoryIterator> di = createDirectoryIterator(Directory.str(), Ext.str());
        ForEach (*di)
        {
            IFile &file = di->query();
        
            CDateTime createTime, modifiedTime,accessedTime;
            file.getTime( &createTime,  &modifiedTime, &accessedTime);

            StringBuffer accessedTimeStr,currentTimeStr;
            accessedTime.getString(accessedTimeStr);
        
            accessedTime.adjustTime(+m_CacheTimeoutPeriod);
            accessedTimeStr.clear();

            accessedTime.getString(accessedTimeStr);
            
            currentTime.getString(currentTimeStr);

            if (accessedTime.compare(currentTime) < 0)
            {
                const char* fileName = file.queryFilename();
                DBGLOG("Trying to remove:%s",fileName);
                if (file.exists() == true)
                {
                    fileCounter++;
                    bool bDeleteOk = file.remove();
                    if (!bDeleteOk)
                        WARNLOG("ERROR Removing old cache file %s",fileName);
                }
            }
        }
    }
public:
    CFileCleanupThread()
    {
        m_TimerPeriod = 1;
        m_CacheTimeoutPeriod = 1;
        m_bRun = true;
    }

    //CFileCleanupThread(StringBuffer& DirectoryPath , const char* fileExt, unsigned TimerPeriodInNanoSec, unsigned CacheTimeoutPeriodInSec = 36000)
    CFileCleanupThread(StringBuffer& DirectoryPath , const char* fileExt, unsigned TimerPeriodInMinutes = 1, unsigned CacheTimeoutPeriodInMinutes = 1)
    {
        m_sem = new Semaphore();
        m_TimerPeriod = TimerPeriodInMinutes * 60 * 1000;
        m_CacheTimeoutPeriod = CacheTimeoutPeriodInMinutes;
        Ext.appendf("*.%s",fileExt);
        Directory.append(DirectoryPath.str());
    }

    ~CFileCleanupThread()
    {
       
        if (m_sem != 0)
            delete m_sem;
    }

    virtual int run()
    {
        DBGLOG("Started Local Directory Tidyup for %s every %d",Directory.str(),m_TimerPeriod);
        Link();
        while(m_bRun)
        {
            m_sem->wait(m_TimerPeriod);
            CleanFiles();
        }
        Release();
        return 0;
    }
    virtual void finish()
    {
        m_bRun = false;
        m_sem->signal();
        this->join();
    }
};

class WSLOGThread_API CLocalDataLogger : public CInterface  
{
private:
        StringBuffer m_logDirectory;
        Owned<CFileCleanupThread> m_CleanupThread;
        CriticalSection crit;
        StringBuffer m_UrlRoot;
public:
    CLocalDataLogger();
    CLocalDataLogger(const char* logDirectory,const char* ext="",unsigned TimerPeriod=1000,unsigned CacheTimeoutPeriod=3600);
    CLocalDataLogger(IPropertyTree *cfg, const char *process, const char *service, const char* UrlRoot);
    virtual ~CLocalDataLogger();
    void Init(const char* logDirectory,const char* ext="", const char* UrlRoot="", const unsigned TimerPeriod=1000,const unsigned CacheTimeoutPeriod=3600);
    StringBuffer &getFilePath(const char* fileName,StringBuffer& returnPath);
    StringBuffer& generateUniqueName(StringBuffer& returnName);
    StringBuffer& writeData(const StringBuffer& dataToCache,const StringBuffer& tokenName,StringBuffer& returnPath);
    MemoryBuffer& readData(MemoryBuffer& dataCached,const char* cachePath);
};

#endif // !defined(_LocalDataLogger_HPP__)
