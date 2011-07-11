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

#ifndef _SECLOADER_HPP__
#define _SECLOADER_HPP__
#include "seclib.hpp"

#ifdef _WIN32
#define LDAPSECLIB "LdapSecurity.dll"
#else
#define LDAPSECLIB "libLdapSecurity.so"
#endif

typedef IAuthMap* (*createDefaultAuthMap_t_)(IPropertyTree* config);
typedef ISecManager* (*newSecManager_t_)(const char *serviceName, IPropertyTree &config);

class SecLoader
{
public:
    static ISecManager* loadSecManager(const char* model_name, const char* servicename, IPropertyTree* cfg)
    {
        if(model_name && stricmp(model_name, "LdapSecurity") == 0)
        {
            HINSTANCE ldapseclib = LoadSharedObject(LDAPSECLIB, true, false);
            if(ldapseclib == NULL)
                throw MakeStringException(-1, "can't load library %s", LDAPSECLIB);
            
            newSecManager_t_ xproc = NULL;
            xproc = (newSecManager_t_)GetSharedProcedure(ldapseclib, "newLdapSecManager");

            if (xproc)
                return xproc(servicename, *cfg);
            else
                throw MakeStringException(-1, "procedure newLdapSecManager of %s can't be loaded", LDAPSECLIB);
        }
        else if(model_name && stricmp(model_name, "Local") == 0)
        {
            HINSTANCE ldapseclib = LoadSharedObject(LDAPSECLIB, true, false);
            if(ldapseclib == NULL)
                throw MakeStringException(-1, "can't load library %s", LDAPSECLIB);
            
            newSecManager_t_ xproc = NULL;
            xproc = (newSecManager_t_)GetSharedProcedure(ldapseclib, "newLocalSecManager");

            if (xproc)
                return xproc(servicename, *cfg);
            else
                throw MakeStringException(-1, "procedure newLocalSecManager of %s can't be loaded", LDAPSECLIB);
        }
        else if(model_name && stricmp(model_name, "Default") == 0)
        {
            HINSTANCE ldapseclib = LoadSharedObject(LDAPSECLIB, true, false);
            if(ldapseclib == NULL)
                throw MakeStringException(-1, "can't load library %s", LDAPSECLIB);
            
            newSecManager_t_ xproc = NULL;
            xproc = (newSecManager_t_)GetSharedProcedure(ldapseclib, "newDefaultSecManager");

            if (xproc)
                return xproc(servicename, *cfg);
            else
                throw MakeStringException(-1, "procedure newDefaultSecManager of %s can't be loaded", LDAPSECLIB);
        }
        else
            throw MakeStringException(-1, "Security model %s not supported", model_name?model_name:"UNKNOWN");
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
