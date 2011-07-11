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

#ifndef __ESPPLUGIN_IPP__
#define __ESPPLUGIN_IPP__

#include "esp.hpp"

class CEspPlugin : public SharedObject, public CInterface,
   implements IEspPlugin
{
private:
   StringBuffer m_plugin;

private:
   CEspPlugin(CEspPlugin &);

public:
   IMPLEMENT_IINTERFACE;

   CEspPlugin(const char *plugin, bool fload=true) : 
      m_plugin(plugin)
   {
      if (fload)
         load();
   }

   ~CEspPlugin()
   {
   }

//IEspPlugin
   bool isLoaded(){return SharedObject::loaded();}

   bool load()
    {
        SharedObject::load(m_plugin.str(), true);       // I'm not really sure what this should be - if global (as default) there will be clashes between multiple dloads
        if (!loaded())
        {
            ERRLOG("ESP Failed to load shared object (%s)", m_plugin.str());    
            return false;
        }
        return true;
    }
   void unload()
    {
        SharedObject::unload();
    }

    void * getProcAddress(const char *name)
    {
    #ifdef _WIN32
        return (isLoaded()) ? ((void *) ::GetProcAddress((HMODULE) getInstanceHandle(), name)) : NULL;
    #else
        #include "dlfcn.h"
        return (isLoaded()) ? ((void *) dlsym(getInstanceHandle(), const_cast<char *>(name))) : NULL;
    #endif
    }

    const char* getName()
    {
        return m_plugin.str();
    }
};


inline IEspPlugin *loadPlugin(const char *name)
{
   CEspPlugin *plugin = new CEspPlugin(name);
   if (plugin->isLoaded())
      return plugin;

   plugin->Release();
   return NULL;
}

#endif //__ESPPLUGIN_IPP__
