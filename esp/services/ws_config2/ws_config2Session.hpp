/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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


#ifndef _CONFIG2SERVICE_SESSION_HPP_
#define _CONFIG2SERVICE_SESSION_HPP_


#include "EnvironmentMgr.hpp"
#include "build-config.h"
#include <iterator>
#include <algorithm>
#include <map>


#define SESSION_KEY_LENGTH 10

struct ConfigMgrSession {

    // NOTE: all paths have NO trailing slash
    std::string     username;            // owner of the session
    std::string     schemaPath;          // path to schema files
    std::string     sourcePath;          // path to environment files
    std::string     activePath;          // path to active environment file
    EnvironmentType configType;          // configuration type (XML, JSON, etc.)
    std::string     masterConfigFile;    // master configuration file for the session
    std::string     lockKey;             // key for write operations when session is locled
    std::string     lastMsg;             // last error message
    std::string     curEnvironmentFile;  // name of currentl loaded envronment file
    bool            locked;              // true if locked
    bool            modified;            // true if session has modified loaded environment
    bool            externallyModified;  // true if another session modified the environment
    EnvironmentMgr  *m_pEnvMgr;          // ptr to active environment manager for session


    ConfigMgrSession() : locked(false), modified(false), m_pEnvMgr(nullptr), configType(UNDEFINED) { }
    ~ConfigMgrSession()
    {
        if (m_pEnvMgr != nullptr)
        {
            delete m_pEnvMgr;
            m_pEnvMgr = nullptr;
        }
    }


    bool initializeSession(std::map<std::string, std::string> &cfgParms)
    {
        bool rc = true;
        m_pEnvMgr = getEnvironmentMgrInstance(configType);
        if (m_pEnvMgr)
        {
            if (!m_pEnvMgr->loadSchema(schemaPath, masterConfigFile, cfgParms))
            {
                rc = false;
                lastMsg = "Unable to load configuration schema, error = " + m_pEnvMgr->getLastSchemaMessage();
            }
        }
        else
        {
            rc = false;
            lastMsg = "Unrecognized configuration type";
        }
        return rc;
    }


    void getEnvironmentFullyQualifiedPath(const std::string &envFile, std::string &fullPath)
    {
        fullPath = sourcePath + envFile;
    }


    bool loadEnvironment(const std::string &envFile)
    {
        std::string fullPath;
        getEnvironmentFullyQualifiedPath(envFile, fullPath);
        bool rc = true;

        closeEnvironment();

        if (!m_pEnvMgr->loadEnvironment(fullPath))
        {
            rc = false;
            lastMsg = "Unable to load environment file, error = " + m_pEnvMgr->getLastEnvironmentMessage();
        }
        else
        {
            curEnvironmentFile = envFile;
        }

        return rc;
    }


    void closeEnvironment()
    {
        m_pEnvMgr->discardEnvironment();
        curEnvironmentFile = "";
        locked = modified = externallyModified = false;
        lockKey = "";
    }


    bool saveEnvironment(const std::string &saveAsFilename)
    {
        bool rc = false;
        std::string saveFile = (saveAsFilename != "") ? saveAsFilename : curEnvironmentFile;
        if (m_pEnvMgr->saveEnvironment(saveFile))
        {
            modified = false;
            curEnvironmentFile = saveFile;
            rc = true;
        }
        else
        {
            rc = false;
            lastMsg = "Unable to save enivronment file, error = " + m_pEnvMgr->getLastEnvironmentMessage();
        }
        return rc;
    }


    std::string getEnvironmentFileExtension() const
    {
        std::string ext = ".unk";
        if (configType == XML)
        {
            ext = ".xml";
        }
        return ext;
    }


    bool lock()
    {
        bool rc = true;
        if (!locked)
        {
            lockKey = generateRandomString(SESSION_KEY_LENGTH);
            locked = true;
        }
        else
        {
            rc = false;
        }
        return rc;
    }


    bool unlock(const std::string &key)
    {
        bool rc = true;
        if (locked)
        {
            locked = !(lockKey == key);
            rc = !locked;
        }
        return rc;
    }


    bool doesKeyFit(const std::string &key)
    {
        // must be locked and the key must match
        return locked && (lockKey == key);
    }


    const std::string &getLastMsg()
    {
        return lastMsg;
    }


    std::string generateRandomString(size_t length)
    {
        const char* charmap = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        const size_t charmapLength = strlen(charmap);
        auto generator = [&](){ return charmap[rand()%charmapLength]; };
        std::string result;
        result.reserve(length);
        std::generate_n(std::back_inserter(result), length, generator);
        return result;
    }

};


#endif
