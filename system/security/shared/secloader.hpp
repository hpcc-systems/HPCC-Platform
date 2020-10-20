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

#ifndef _SECLOADER_HPP__
#define _SECLOADER_HPP__
#include "seclib.hpp"

typedef IAuthMap* (*createDefaultAuthMap_t_)(IPropertyTree* config);
typedef ISecManager* (*newSecManager_t_)(const char *serviceName, IPropertyTree &config);
typedef ISecManager* (*newPluggableSecManager_t_)(const char *serviceName, IPropertyTree &secMgrCfg, IPropertyTree &authCfg);

class SecLoader
{
public:
    ///
    /// Method:  loadPluggableSecManager
    ///
    /// Using the given configuration property trees, this method loads the specified
    /// Security Manager DLL/SO implemented in the specified library file and calls
    /// its instance factory to create and return an ISecManager security manager instance
    /// for the given ESP service
    ///
    /// @param  bindingName     Binding name ie 'WsTopology_smc_myesp'
    /// @param  bindingCfg      'Binding' IPropertyTree associated with ESPService
    /// @param  secMgrCfg       'SecurityManager' IPropertyTree from component config file
    ///
    /// @return an ISecManager Security Manager instance
    ///
    template <class SECMGR>
    static SECMGR* loadPluggableSecManager(const char * bindingName, IPropertyTree* bindingCfg, IPropertyTree* secMgrCfg)
    {
        const char * lsm = "Load Security Manager :";

        StringBuffer libName, instFactory;
        secMgrCfg->getProp("@LibName", libName);
        if (libName.isEmpty())
        {
            // @libName is commonly used
            secMgrCfg->getProp("@libName", libName);
            if (libName.isEmpty())
                throw MakeStringException(-1, "%s library name not specified for %s", lsm, bindingName);
        }
        //TODO Search for LibName in plugins folder, or in specified location

        instFactory.set(secMgrCfg->queryProp("@InstanceFactoryName"));
        if (instFactory.isEmpty())
        {
            // @instanceFactoryName is commonly used
            instFactory.set(secMgrCfg->queryProp("@instanceFactoryName"));
            if (instFactory.isEmpty())
                instFactory.set("createInstance");
        }

        //Load the DLL/SO
        HINSTANCE pluggableSecLib = LoadSharedObject(libName.str(), true, false);
        if(pluggableSecLib == NULL)
            throw MakeStringException(-1, "%s can't load library %s for %s", lsm, libName.str(), bindingName);

        //Retrieve address of exported SECMGR instance factory
        newPluggableSecManager_t_ xproc = NULL;
        xproc = (newPluggableSecManager_t_)GetSharedProcedure(pluggableSecLib, instFactory.str());
        if (xproc == NULL)
            throw MakeStringException(-1, "%s cannot locate procedure %s of '%s'", lsm, instFactory.str(), libName.str());

        //Call SECMGR instance factory and return the new instance
        DBGLOG("Calling '%s' in pluggable security manager '%s'", instFactory.str(), libName.str());
        SECMGR* pPSM = dynamic_cast<SECMGR*>(xproc(bindingName, *secMgrCfg, *bindingCfg));
        if (pPSM == nullptr)
            throw MakeStringException(-1, "%s Security Manager %s failed to instantiate in call to %s", lsm, libName.str(), instFactory.str());
        return pPSM;
    }

    static ISecManager* loadSecManager(const char* model_name, const char* servicename, IPropertyTree* cfg)
    {
        if (!model_name || !*model_name)
            throw MakeStringExceptionDirect(-1, "Security model not specified");

        StringBuffer realName;

        if(stricmp(model_name, "LdapSecurity") == 0)
        {
            realName.append(SharedObjectPrefix).append(LDAPSECLIB).append(SharedObjectExtension);
            HINSTANCE ldapseclib = LoadSharedObject(realName.str(), true, false);
            if(ldapseclib == NULL)
                throw MakeStringException(-1, "can't load library %s", realName.str());

            newSecManager_t_ xproc = NULL;
            xproc = (newSecManager_t_)GetSharedProcedure(ldapseclib, "newLdapSecManager");

            if (xproc)
                return xproc(servicename, *cfg);
            else
                throw MakeStringException(-1, "procedure newLdapSecManager of %s can't be loaded", realName.str());
        }
        else
            throw MakeStringException(-1, "Security model %s not supported", model_name);
    }

    static IAuthMap* loadTheDefaultAuthMap(IPropertyTree* cfg)
    {
        HINSTANCE seclib = LoadSharedObject(LDAPSECLIB, true, false);       // ,false,true may actually be more helpful.
        if(seclib == NULL)
            throw MakeStringException(-1, "can't load library %s", LDAPSECLIB);

        createDefaultAuthMap_t_ xproc = NULL;
        xproc = (createDefaultAuthMap_t_)GetSharedProcedure(seclib, "newDefaultAuthMap");

        if (xproc)
            return xproc(cfg);
        else
            throw MakeStringException(-1, "procedure newDefaultAuthMap of %s can't be loaded", LDAPSECLIB);
    }
};

#endif
