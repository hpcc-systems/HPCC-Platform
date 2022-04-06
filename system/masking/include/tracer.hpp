/*##############################################################################

    Copyright (C) 2022 HPCC Systems®.

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

#pragma once

#include "tracer.h"
#include <map>

/**
 * @brief Abstraction responsible for preventing trace messages from being produced by
 *        `CModularTracer`.
 */
interface IModularTraceMsgFilter : extends IInterface
{
    virtual bool rejects(const LogMsgCategory& category) const = 0;
};

/**
 * @brief Abstraction responsible for creating trace messages for `CModularTracer`.
 *
 * This extends `IModularTraceMsgFilter`. Installed as a sink, it may reject messages without
 * impacting other sinks. Installed as a filter, rejecting a message affects all sinks. A Category
 * rejected when used as a sink should also be rejected when used as a filter.
 *
 * Some potential sinks include:
 *
 * - Output using standard jlog functions.
 * - Output using `stdout`, intended for use with command line tools.
 * - Collection of messages to be returned as part of a response, or saved for later debugging.
 */
interface IModularTraceMsgSink : extends IModularTraceMsgFilter
{
    virtual void valog(const LogMsgCategory& category, const char* format, va_list arguments) = 0;
};

/**
 * @brief Implementation of `IModularTraceMsgSink` directing message output to the jlog log
 *        manager.
 */
class CStandardTraceMsgSink : public CInterfaceOf<IModularTraceMsgSink>
{
public:
    virtual void valog(const LogMsgCategory& category, const char* format, va_list arguments) override
    {
        VALOG(category, format, arguments);
    }
    virtual bool rejects(const LogMsgCategory& category) const
    {
        return REJECTLOG(category);
    }
};

/**
 * @brief Concrete implementation of `CBaseTracer` that permits helper objects to change the
 *        default behavior.
 *
 * Zero or more instances of `IModularTraceMsgFilter` may be installed. If any instance rejects a
 * category, the corresponding log operation reports inactive and no output is produced.
 *
 * Zero or more instances of `IModularTraceMsgSink` may be installed. An instance of
 * `CStandardTraceMsgSink` is installed by default; users may either retain, remove, or replace it.
 * If no instances are installed, all operations report inactive and no output is produced. Message
 * content is passed to all installed sinks for independent processing; a message rejected by one
 * sink may be acceptable to another.
 */
class CModularTracer : public CBaseTracer
{
public:
    static constexpr const char* DefaultHelperName = "";
public:
    virtual bool logIsActive(const LogMsgCategory& category) const override
    {
        ReadLockBlock block(sinksLock);
        switch (sinks.size())
        {
        case 0:
            return false;
        case 1:
            return (checkFilters(category) && !sinks.begin()->second->rejects(category));
        default:
            if (checkFilters(category))
            {
                for (const Sinks::value_type& node : sinks)
                {
                    if (!node.second->rejects(category))
                        return true;
                }
            }
            return false;
        }
    }
protected:
    virtual void valog(const LogMsgCategory& category, const char* format, va_list arguments) const override
    {
        ReadLockBlock block(sinksLock);
        switch (sinks.size())
        {
        case 0:
            break;
        case 1:
            if (checkFilters(category))
                sinks.begin()->second->valog(category, format, arguments);
            break;
        default:
            if (checkFilters(category))
            {
                for (const Sinks::value_type& node : sinks)
                {
                    va_list copiedArguments;
                    va_copy(copiedArguments, arguments);
                    node.second->valog(category, format, copiedArguments);
                    va_end(copiedArguments);
                }
            }
            break;
        }
    }
    virtual bool checkFilters(const LogMsgCategory& category) const
    {
        ReadLockBlock block(filtersLock);
        switch (filters.size())
        {
        case 0:
            return true;
        case 1:
            return !filters.begin()->second->rejects(category);
        default:
            for (const Filters::value_type& node : filters)
            {
                if (node.second->rejects(category))
                    return false;
            }
            return true;
        }
    }
private:
    using Filters = std::map<std::string, Linked<IModularTraceMsgFilter> >;
    Filters filters;
    mutable ReadWriteLock filtersLock;
    using Sinks = std::map<std::string, Linked<IModularTraceMsgSink> >;
    Sinks sinks;
    mutable ReadWriteLock sinksLock;
public:
    CModularTracer()
    {
        setSink(DefaultHelperName, new CStandardTraceMsgSink());
    }
    inline bool setSink(IModularTraceMsgSink* sink)
    {
        return setSink(DefaultHelperName, sink);
    }
    bool setSink(const char* name, IModularTraceMsgSink* sink)
    {
        if (!name)
            return false;
        WriteLockBlock block(sinksLock);
        if (!sink)
            return (sinks.erase(name) > 0);
        sinks[name].set(sink);
        return true;
    }
    inline IModularTraceMsgSink* getSink() const
    {
        return getSink(DefaultHelperName);
    }
    IModularTraceMsgSink* getSink(const char* name) const
    {
        if (name)
        {
            ReadLockBlock block(sinksLock);
            Sinks::const_iterator it = sinks.find(name);
            if (it != sinks.end())
                return it->second.getLink();
        }
        return nullptr;
    }
    inline bool setFilter(IModularTraceMsgFilter* filter)
    {
        return setFilter(DefaultHelperName, filter);
    }
    bool setFilter(const char* name, IModularTraceMsgFilter* filter)
    {
        if (!name)
            return false;
        WriteLockBlock block(filtersLock);
        if (!filter)
            return (filters.erase(name) > 0);
        filters[name].set(filter);
        return true;
    }
    inline IModularTraceMsgFilter* getFilter() const
    {
        return getFilter(DefaultHelperName);
    }
    IModularTraceMsgFilter* getFilter(const char* name) const
    {
        if (name)
        {
            ReadLockBlock block(filtersLock);
            Filters::const_iterator it = filters.find(name);
            if (it != filters.end())
                return it->second.getLink();
        }
        return nullptr;
    }
};

/**
 * @brief Implementation of `IModularTraceMsgSink` directing message output directly to `stdout`.
 *
 * Jlog functions append newlines at the end of each message. This behavior tends to be assumed.
 * This class appends newlines by default, but this may be changed.
 *
 * Trace output generated by individual unit tests, as opposed to the test framework, frequently
 * insert newlines at the start and end of messages. This ensures test franework output is never
 * on the same line as test output. This class dot not prepend newlines by default, but this may
 * be changed.
 */
class CConsoleTraceMsgSink : public CInterfaceOf<IModularTraceMsgSink>
{
public:
    virtual void valog(const LogMsgCategory& category, const char* format, va_list arguments) override
    {
        StringBuffer msg;
        if (prependEOLN)
            msg.append('\n');
        msg.valist_appendf(format, arguments);
        if (appendEOLN && msg.charAt(msg.length() - 1) != '\n')
            msg.append('\n');
        printf("%s", msg.str());
    }
    virtual bool rejects(const LogMsgCategory& category) const
    {
        return false;
    }
private:
    bool appendEOLN = true;
    bool prependEOLN = false;
public:
    CConsoleTraceMsgSink()
    {
    }
    CConsoleTraceMsgSink(bool _prependEOLN)
        : prependEOLN(_prependEOLN)
    {
    }
    CConsoleTraceMsgSink(bool _prependEOLN, bool _appendEOLN)
        : appendEOLN(_appendEOLN)
        , prependEOLN(_prependEOLN)
    {
    }
};
