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
            OERRLOG("Failed to load shared object (%s)", m_plugin.str());
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
