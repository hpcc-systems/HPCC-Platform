/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#ifndef _PluginLoader_HPP_
#define _PluginLoader_HPP_

#include "jlog.hpp"
#include "jmutex.hpp"
#include "jptree.hpp"
#include <map>
#include <string>

/**
 * TPluginLoader encapsulates the logic used to obtain an entry-point function pointer from a
 * shared library. The logic involved is not complex, but it is repetitive.
 *
 * The shared library name may be configured dynamically or known in advance. Given a
 * configuration property tree and an XPath, the name will be extracted from the property tree.
 * Given only a default name, the default will be used. Given all three, a configured value will
 * be used if one exists and the default will be used otherwise.
 *
 * The entry-point function name may be configured dynamically or known in advance. Given a
 * configuration property tree and an XPath, the name will be extracted from the property tree.
 * Given only a default name, the default will be used. Given all three, a configured value will
 * be used if one exists and the default will be used otherwise.
 *
 * The template parameter is an entry point function signature. This suggests a limitation of
 * one kind of plugin per instance.
 */
template <typename entry_point_t>
class TPluginLoader
{
public:
    template <typename instance_t>
    using Creator = std::function<instance_t*(entry_point_t entryPoint)>;

    TPluginLoader(const char* libraryDefault, const char* entryPointDefault, const char* libraryXPath, const char* entryPointXPath)
        : m_libraryXPath(libraryXPath)
        , m_libraryDefault(libraryDefault)
        , m_entryPointXPath(entryPointXPath)
        , m_entryPointDefault(entryPointDefault)
    {
        StringBuffer missing;
        if (m_libraryXPath.isEmpty() && m_libraryDefault.isEmpty())
        {
            if (!missing.isEmpty())
                missing.append(" and ");
            missing.append("library name");
        }
        if (m_entryPointXPath.isEmpty() && m_entryPointDefault.isEmpty())
        {
            if (!missing.isEmpty())
                missing.append(" and ");
            missing.append("entry-point function name");
        }
        if (!missing.isEmpty())
            ERRLOG("bad plugin loading configuration - missing %s", missing.str());
    }

    template <typename instance_t>
    instance_t* create(const IPTree& configuration, Creator<instance_t> creator)
    {
        instance_t* instance = nullptr;
        if (!creator)
        {
            ERRLOG("plugin creation failed - invalid creation hook function");
            return nullptr;
        }
        else
        {
            try
            {
                entry_point_t entryPoint = lookup(configuration);
                if (entryPoint)
                    instance = creator(entryPoint);
            }
            catch (IException* e)
            {
                StringBuffer msg;
                ERRLOG("plugin creation exception: %s", e->errorMessage(msg).str());
            }
            catch (...)
            {
                ERRLOG("plugin creation exception");
            }
        }
        return instance;
    }

    entry_point_t lookup(const IPTree& configuration)
    {
        const char* libraryName = nullptr;
        const char* entryPointName = nullptr;
        if (!m_libraryXPath.isEmpty())
            libraryName = configuration.queryProp(m_libraryXPath);
        if (!m_entryPointXPath.isEmpty())
            entryPointName = configuration.queryProp(m_entryPointXPath);
        return lookup(libraryName, entryPointName);
    }

    entry_point_t lookup(const char* libraryName, const char* entryPointName)
    {
        bool canFind = true;
        if (isEmptyString(libraryName))
        {
            if (m_libraryDefault.isEmpty())
            {
                ERRLOG("plugin loader name lookup failed - no library name and no default value");
                canFind = false;
            }
            else
                libraryName = m_libraryDefault;
        }
        if (isEmptyString(entryPointName))
        {
            if (m_entryPointDefault.isEmpty())
            {
                ERRLOG("plugin loader name lookup failed - no entry point name and no default value");
                canFind = false;
            }
            else
                entryPointName = m_entryPointDefault;
        }
        return (canFind ? find(libraryName, entryPointName) : nullptr);
    }

private:
    using EntryPointMap = std::map<std::string, entry_point_t>;
    struct Plugin
    {
        HINSTANCE library = nullptr;
        EntryPointMap entryPoints;
    };
    using PluginMap = std::map<std::string, Plugin>;
    PluginMap       m_plugins;
    CriticalSection m_lock;
    StringBuffer    m_libraryXPath;
    StringBuffer    m_libraryDefault;
    StringBuffer    m_entryPointXPath;
    StringBuffer    m_entryPointDefault;

    entry_point_t find(const char* libraryName, const char* entryPointName)
    {
        VStringBuffer realName("%s%s%s", SharedObjectPrefix, libraryName, SharedObjectExtension);
        CriticalBlock block(m_lock);
        Plugin& plugin = m_plugins[libraryName];
        if (!plugin.library)
        {
            plugin.library = LoadSharedObject(realName, true, false);
            if (!plugin.library)
            {
                ERRLOG("plugin loader lookup failed - cannot load library '%s'", realName.str());
                return nullptr;
            }
        }
        entry_point_t& entryPoint = plugin.entryPoints[entryPointName];
        if (!entryPoint)
        {
            entryPoint = (entry_point_t)GetSharedProcedure(plugin.library, entryPointName);
            if (!entryPoint)
            {
                ERRLOG("plugin loader lookup failed - library '%s' does not export '%s'", realName.str(), entryPointName);
                return nullptr;
            }
        }
        return entryPoint;
    }
};

#endif // _PluginLoader_HPP_
