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

#ifndef _CONFIG_FILE_HPP_
#define _CONFIG_FILE_HPP_

#include "jiface.hpp"
#include "jmutex.hpp"
#include "jutil.hpp"
#include "ConfigFileUtilsObservable.hpp"

static const char* DEFAULT_CONFIGURATION_PATH("/etc/HPCCSystems/source/");
static const char* DEFAULT_CONFIGURATION_MASK("*.xml");

class CConfigFileUtils : public CConfigFileUtilsObservable
{
public:

    enum CF_ERROR_CODES { CF_NO_ERROR = 0,
                          CF_FILE_EXISTS,
                          CF_FILE_PERMISSIONS,
                          CF_FILE_NOT_OPEN,
                          CF_FILE_ALREADY_OPEN,
                          CF_FILE_DOES_NOT_EXIST,
                          CF_WRITE_BUFFER_EMPTY,
                          CF_DIRECTORY_ACCESS_ERROR,
                          CF_OTHER = 99 };

    static CConfigFileUtils* getInstance();
    virtual ~CConfigFileUtils();
    const StringArray& getAvailableConfigurationFiles() const;
    enum CF_ERROR_CODES writeConfigurationToFile(const char *pFilePath, const char* pBuffer, unsigned int length);
    enum CF_ERROR_CODES createNewConfigurationFile(const char* pConfigFileName);
    enum CF_ERROR_CODES openConfigurationFile(const char* pConfigFileName);
    enum CF_ERROR_CODES closeConfigurationFile();
    enum CF_ERROR_CODES writeToOpenConfigurationFile(const char* pBuffer, unsigned int length);

protected:

    enum CF_ERROR_CODES populateConfigFileArray();

private:

    CConfigFileUtils(const char* pDir = DEFAULT_CONFIGURATION_PATH, const char* pMask = DEFAULT_CONFIGURATION_MASK);

    StringArray     m_configFileArray;
    const char*     m_pDirPath;
    const char*     m_pMask;
    Owned<IFile>    m_pDir;
    Owned<IFile>    m_pFile;
    Owned<IFileIO>  m_pFileIO;
};

#endif // _CONFIG_FILE_HPP_
