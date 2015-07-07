/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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


#include "ConfigFileUtils.hpp"
#include "jfile.hpp"
#include "jmutex.hpp"

CConfigFileUtils* CConfigFileUtils::getInstance()
{
    static Owned<CConfigFileUtils> s_configFileSingleton;
    static CSingletonLock slock;

    if (slock.lock() == true)
    {
      if (s_configFileSingleton.get() == NULL)
        s_configFileSingleton.setown(new CConfigFileUtils(DEFAULT_CONFIGURATION_PATH));

      slock.unlock();
    }
    return s_configFileSingleton.get();
}

CConfigFileUtils::CConfigFileUtils(const char* pDir, const char* pMask) : m_pDirPath(pDir), m_pMask(pMask)
{
}

CConfigFileUtils::~CConfigFileUtils()
{
    closeConfigurationFile();
}

enum CConfigFileUtils::CF_ERROR_CODES CConfigFileUtils::createNewConfigurationFile(const char* pConfigFileName)
{
    notify(IConfigFileUtilsObserver::CF_FILE_CREATE_EVENT);
    return CF_NO_ERROR;
}

enum CConfigFileUtils::CF_ERROR_CODES CConfigFileUtils::openConfigurationFile(const char* pConfigFileName)
{
    if (m_pFileIO.get() != NULL)
        return CF_FILE_ALREADY_OPEN;
    else if (m_pFileIO.get() == NULL)
        return CF_FILE_NOT_OPEN;
    else
    {
        m_pFile.setown(createIFile(pConfigFileName));
        m_pFileIO.setown(m_pFile->open(IFOcreaterw));

        notify(IConfigFileUtilsObserver::CF_FILE_OPEN_EVENT);
        return CF_NO_ERROR;
    }
}

enum CConfigFileUtils::CF_ERROR_CODES CConfigFileUtils::closeConfigurationFile()
{
    if (m_pFileIO.get() != NULL)
    {
        m_pFileIO->close();
        m_pFileIO.clear();
        m_pFile.clear();
        notify(IConfigFileUtilsObserver::CF_FILE_CLOSE_EVENT);

        return CF_NO_ERROR;
    }
    else
        return CF_FILE_NOT_OPEN;
}

enum CConfigFileUtils::CF_ERROR_CODES CConfigFileUtils::writeToOpenConfigurationFile(const char* pBuffer, unsigned int length)
{
    if (length == 0)
        return CF_WRITE_BUFFER_EMPTY;
    else if (m_pFile.get() == NULL)
        return CF_FILE_NOT_OPEN;
    else
    {
        m_pFileIO->write(0, length, pBuffer);
        notify(IConfigFileUtilsObserver::CF_FILE_WRITE_EVENT);
        return CF_NO_ERROR;
    }
}

enum CConfigFileUtils::CF_ERROR_CODES CConfigFileUtils::populateConfigFileArray()
{
    m_configFileArray.kill();

    if (m_pDir.get() == NULL)
    {
        m_pDir.set(createIFile(m_pDirPath));

        if ( (m_pDir.get() == NULL) || (m_pDir->exists() != true) || (m_pDir->isDirectory() == false) )
            return CF_DIRECTORY_ACCESS_ERROR;
    }

    Owned<IDirectoryIterator> dirIter = m_pDir->directoryFiles(m_pMask);

    if(dirIter.get() != NULL)
    {
        IFile &file = dirIter->query();
        StringBuffer fname(file.queryFilename());
        getFileNameOnly(fname, false);
        m_configFileArray.append(fname);

        return CF_NO_ERROR;
    }
    else
        return CF_DIRECTORY_ACCESS_ERROR;
}

enum CConfigFileUtils::CF_ERROR_CODES CConfigFileUtils::writeConfigurationToFile(const char *pFilePath, const char* pBuffer, unsigned int length)
{
    assert(pBuffer != NULL);
    assert(length != 0);
    assert(pBuffer != NULL && *pBuffer != 0);

    Owned<IFile>   pFile;
    Owned<IFileIO> pFileIO;
    IFileIO *pFIO = NULL;

    pFile.setown(createIFile(pFilePath));

    try
    {
        pFIO = pFile->open(IFOcreaterw);
    }
    catch(IErrnoException *pException)
    {
        pException->Release();
    }


    if (pFIO == NULL)
        return CF_FILE_PERMISSIONS;

    pFileIO.setown(pFIO);
    pFileIO->write(0, length, pBuffer);
    notify(IConfigFileUtilsObserver::CF_FILE_WRITE_NO_CHECK);

    return CF_NO_ERROR;
}
