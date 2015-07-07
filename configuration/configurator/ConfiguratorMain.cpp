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

#include "ConfiguratorMain.hpp"
#include "EnvironmentConfiguration.hpp"
#include "WizardBase.hpp"
#include "ConfigSchemaHelper.hpp"
#include "SchemaCommon.hpp"
#include "ExceptionStrings.hpp"
#include <iostream>
#include "jfile.hpp"

#ifdef CONFIGURATOR_LIB
    #include "../configurator_ui/ConfiguratorUI.hpp"
    #include "../configurator_app/MainWindowInterface.h"
#endif // CONFIGURATOR_LIB

#define BUFF_SIZE 1024

const char *pDefaultDocExt =  ".mod.xml";
const char *pDefaultQMLExt =  ".qml";

void usage()
{
    std::cout << "configurator -use <xsd files>  -b <base dir path>" << std::endl;
    std::cout << "Example Usage: ./configurator -use dali.xsd -b /opt/HPCCSystems/componentfiles/configxml -d -t /tmp " << std::endl;
    std::cout << "-d -doc                           : generate docs" << std::endl;
    std::cout << "-b -base <base dir path>          : base directory path to use with -use option and for xs:include references in xsd files" << std::endl;
    std::cout << "-e -extension <file extension>    : write docs or qml to files with appended extension (default " <<  pDefaultDocExt << ")" << std::endl;
    std::cout << "-t -target <target directory>     : directory to which to docs will be written. If not specified, then output will go to std::out" << std::endl;
    std::cout << "-u -use <schema xsd>              : use specified xsd schema instead of buildset file" << std::endl;
    std::cout << "-h -help                          : prints out this usage" << std::endl;

    std::cout << std::endl;

    std::cout << std::endl << "** Experimental **" << std::endl;
    std::cout <<"Example Usage: ./configurator -use esp.xsd -b /opt/HPCCSystems/componentfiles/configxml/  -qml -t ./ -e qml -s1 ./esp.xsd.qml -env /etc/HPCCSystems/source/environment.xml" << std::endl;
    std::cout << "-f -file <build set file>         : buildset file name (required if base directory is specfied" << std::endl;
    std::cout << "-p -path <base dir path>          : base directory path (required if buildset file name is specified)" << std::endl;
    std::cout << "-x -xsd  <xsd file name>          : xsd file name (can be more than one) - For use with buildset file" << std::endl;
    std::cout << "-l -list                          : list available xsd files" << std::endl;
    std::cout << "-m -xml                           : generate XML configuration file" << std::endl;
    std::cout << "-q -qml                           : prints QML" << std::endl;
    std::cout << "-c -env -config <path to env xml file> : load environment config xml file (e.g. environment.xml) " << std::endl;
#ifdef CONFIGURATOR_LIB
        std::cout << "-s1 -server1 <qml file>           : run server using qml file" << std::endl;
        std::cout << "-s2 -server2                      : run QT application mode" << std::endl;
#endif // CONFIGURATOR_LIB
    std::cout << "-dump                             : dump out xsd internal structure and values" << std::endl;
}

#ifndef CONFIGURATOR_LIB
    int main(int argc, char *argv[])
#else
    int ConfiguratorMain(int argc, char* argv[])
#endif // CONFIGURATOR_LIB
{
    InitModuleObjects();

    int idx = 1;

    CConfigSchemaHelper *pSchemaHelper = NULL;

    char pBuildSetFile[BUFF_SIZE];
    char pBuildSetFileDir[BUFF_SIZE];
    char pTargetDocDir[BUFF_SIZE];
    char pTargetDocExt[BUFF_SIZE];
    char pOverrideSchema[BUFF_SIZE];
    char pBasePath[BUFF_SIZE];
    char pEnvXMLPath[BUFF_SIZE];
    char pQMLFile[BUFF_SIZE];

    memset(pBuildSetFile, 0, sizeof(pBuildSetFile));
    memset(pBuildSetFileDir, 0, sizeof(pBuildSetFileDir));
    memset(pTargetDocDir, 0, sizeof(pTargetDocDir));
    memset(pOverrideSchema, 0, sizeof(pOverrideSchema));
    memset(pBasePath, 0, sizeof(pBasePath));
    memset(pEnvXMLPath, 0, sizeof(pEnvXMLPath));
    memset(pQMLFile,0, sizeof(pQMLFile));

    strncpy(pTargetDocExt, pDefaultDocExt, sizeof(pTargetDocExt));

    bool bListXSDs      = false;
    bool bGenDocs       = false;
    bool bGenQML        = false;
    bool bDump          = false;
    bool bLoadEnvXML    = false;
    bool bQMLServer     = false;

    StringArray arrXSDs;

    if (argc == 1)
    {
        usage();
        return 0;
    }

    while(idx < argc)
    {
        if (stricmp(argv[idx], "-help") == 0 || stricmp(argv[idx], "-h") == 0)
        {
            usage();
            return 0;
        }
        if (stricmp(argv[idx], "-dump") == 0)
            bDump = true;
        if (stricmp(argv[idx], "-config") == 0 || stricmp(argv[idx], "-c") == 0 || stricmp(argv[idx], "-env") == 0)
        {
            idx++;
            bLoadEnvXML = true;

            if (argv[idx] == NULL)
            {
                std::cout << "Missing env xml file parameter!" << std::endl;
                return 0;
            }
            strncpy(pEnvXMLPath, argv[idx], BUFF_SIZE);
        }
        else if (stricmp(argv[idx], "-file") == 0 || stricmp(argv[idx], "-f") == 0)
        {
            idx++;

            assert(argv[idx]);
            if (argv[idx] == NULL)
            {
                std::cout << "Missing file parameter!" << std::endl;
                return 0;
            }
            strncpy(pBuildSetFile, argv[idx], BUFF_SIZE);
        }
        else if (stricmp(argv[idx], "-path") == 0 || stricmp(argv[idx], "-p") == 0)
        {
            idx++;

            assert(argv[idx]);
            if (argv[idx] == NULL)
            {
                std::cout << "Missing path parameter!" << std::endl;
                return 0;
            }
            strncpy(pBuildSetFileDir, argv[idx], BUFF_SIZE);
        }
        else if (stricmp(argv[idx], "-base") == 0 || stricmp(argv[idx], "-b") == 0)
        {
            idx++;

            assert(argv[idx]);
            if (argv[idx] == NULL)
            {
                std::cout << "Missing base dir parameter!" << std::endl;
                return 0;
            }
            strncpy(pBasePath, argv[idx], BUFF_SIZE);
        }
        else if (stricmp(argv[idx], "-xsd") == 0 || stricmp(argv[idx], "-x") == 0)
        {
            idx++;

            assert(argv[idx]);
            if (argv[idx] == NULL)
            {
                std::cout << "Missing XSD file!" << std::endl;
                return 0;
            }
            arrXSDs.append(argv[idx]);
        }
#ifdef CONFIGURATOR_LIB
        else if (stricmp(argv[idx], "-s1") == 0 || stricmp(argv[idx], "-server1") == 0)
        {
            idx++;

            if (argv[idx] == NULL)
            {
                std::cout << "Missing qml file!" << std::endl;
                return 0;
            }
            strncpy(pQMLFile, argv[idx], BUFF_SIZE);
            bQMLServer = true;
        }
#endif // #ifdef CONFIGURATOR_LIB
        else if (stricmp(argv[idx], "-list") == 0 || stricmp(argv[idx], "-l") == 0)
            bListXSDs = true;
        else if (stricmp(argv[idx], "-doc") == 0 || stricmp(argv[idx], "-d") == 0)
            bGenDocs = true;
        else if (stricmp(argv[idx], "-target") == 0 || stricmp(argv[idx], "-t") == 0)
        {
            idx++;

            assert(argv[idx]);
            if (argv[idx] == NULL)
            {
                std::cout << "Missing target!" << std::endl;
                return 0;
            }
            strcpy(pTargetDocDir,argv[idx]);
        }
        else if (stricmp(argv[idx], "-extension") == 0 || stricmp(argv[idx], "-e") == 0)
        {
            idx++;

            assert(argv[idx]);
            if (argv[idx] == NULL)
            {
                std::cout << "Missing extension!" << std::endl;
                return 0;
            }
            if (argv[idx][0] != '.')
            {
                strcat(pTargetDocExt, ".");
                strcpy(&(pTargetDocExt[1]),argv[idx]);
            }
            else if (*pTargetDocExt == 0)
                strcat(pTargetDocExt, "");
            else
                strcpy(pTargetDocExt,argv[idx]);
        }
        else if (stricmp(argv[idx], "-use") == 0 || stricmp(argv[idx], "-u") == 0)
        {
            idx++;

            assert(argv[idx]);
            if (argv[idx] == NULL)
            {
                std::cout << "Missing schema xsd!" << std::endl;
                return 0;
            }
            else
            {
                strcpy(pOverrideSchema, argv[idx]);
                arrXSDs.append(argv[idx]);
            }
        }
        else if (stricmp(argv[idx], "-qml") == 0 || stricmp(argv[idx], "-q") == 0)
            bGenQML = true;
        idx++;
    }

    if ((pBuildSetFile[0] != 0) ^ (pBuildSetFileDir[0] != 0))
    {
        puts("-file and -path need to be both set or neither one!");
        return 0;
    }
    if (bGenDocs == true && arrXSDs.length() == 0)
    {
        puts("No XSDs specified for doc generation!");
        return 0;
    }
    if (pBuildSetFile[0] == 0 && pOverrideSchema[0] == 0)
    {
        pSchemaHelper = CConfigSchemaHelper::getInstance();
    }
    else if (pBuildSetFile[0] == 0)
    {
        pSchemaHelper = CConfigSchemaHelper::getInstance(pBasePath);
    }
    else
    {
        pSchemaHelper = CConfigSchemaHelper::getInstance(pBuildSetFile, pBuildSetFileDir);
    }

    assert(pSchemaHelper);
    if (pOverrideSchema[0] != 0)
        CBuildSetManager::getInstance()->setBuildSetArray(arrXSDs);

    try
    {
        pSchemaHelper->populateSchema();
    }
    CATCH_EXCEPTION_AND_EXIT

    if (bListXSDs == true)
    {
        StringArray arrXSDs;
        CBuildSetManager::getInstance()->getBuildSetComponents(arrXSDs);

        if (arrXSDs.length() > 0)
            std::cout << "XSD files (" << arrXSDs.length() << ")" << std::endl;

        for (int idx = 0; idx < arrXSDs.length(); idx++)
            std::cout << "(" << idx+1 << ") " << arrXSDs.item(idx) << std::endl;
    }

    for (int idx =  0; bGenDocs == true && idx < arrXSDs.length(); idx++)
    {
        if (pTargetDocDir[0] == 0)
            std::cout << pSchemaHelper->printDocumentation(arrXSDs.item(idx));
        else
        {
            Owned<IFile>   pFile;
            Owned<IFileIO> pFileIO;
            StringBuffer strTargetPath;
            const char *pXSDFile = strrchr(arrXSDs.item(idx), '/') == NULL ? arrXSDs.item(idx) : strrchr(arrXSDs.item(idx),'/');

            strTargetPath.append(pTargetDocDir).append("/").append(pXSDFile).append(pTargetDocExt);
            pFile.setown(createIFile(strTargetPath.str()));
            pFileIO.setown(pFile->open(IFOcreaterw));

            const char *pDoc = pSchemaHelper->printDocumentation(arrXSDs.item(idx));

            if (pDoc == NULL)
                continue;

            pFileIO->write(0, strlen(pDoc), pDoc);
        }
    }

    for (int idx =  0; bGenQML == true && idx < arrXSDs.length(); idx++)
    {
        if (pTargetDocDir[0] == 0)
        {
            char *pOutput = NULL;

            pSchemaHelper->printQML(arrXSDs.item(idx), &pOutput);
            std::cout << pOutput;

            free(pOutput);
        }
        else
        {
            Owned<IFile>   pFile;
            Owned<IFileIO> pFileIO;
            StringBuffer strTargetPath;

            const char *pXSDFile = strrchr(arrXSDs.item(idx), '/') == NULL ? arrXSDs.item(idx) : strrchr(arrXSDs.item(idx),'/');

            strTargetPath.append(pTargetDocDir).append("/").append(pXSDFile).append(pDefaultQMLExt);
            pFile.setown(createIFile(strTargetPath.str()));
            pFileIO.setown(pFile->open(IFOcreaterw));

            char *pQML = NULL;
            pSchemaHelper->printQML(arrXSDs.item(idx), &pQML);

            if (pQML == NULL)
            {
                free(pQML);
                continue;
            }
            free(pQML);
            pFileIO->write(0, strlen(pQML), pQML);
        }
    }

    for (int idx =  0; (bDump == true || bLoadEnvXML == true) && idx < arrXSDs.length(); idx++)
    {
        if (bLoadEnvXML == true)
            pSchemaHelper->loadEnvFromConfig(pEnvXMLPath);
        if (bDump == true)
            pSchemaHelper->printDump(arrXSDs.item(idx));
    }

    if (bQMLServer == true)
    {
#ifdef CONFIGURATOR_LIB
        StartQMLUI(pQMLFile);
#endif // CONFIGURATOR_LIB
    }
    return 0;
}
