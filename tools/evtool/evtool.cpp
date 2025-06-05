/*##############################################################################

    Copyright (C) 2024 HPCC Systems®.

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

#include "evtool.hpp"
#include "eventfilter.h"
#include "jevent.hpp"
#include "jfile.hpp"
#include "jstring.hpp"
#include <iostream>

int CEvToolCommand::dispatch(int argc, const char* argv[], int pos)
{
    for (int idx = pos + 1; idx < argc; ++idx)
    {
        if (!accept(argv[idx]))
        {
            StringBuffer err;
            err << "invalid argument: " << argv[idx] << "\n\n";
            consoleErr().put(err.length(), err.str());
            usage(argc, argv, pos, consoleErr());
            return 1;
        }
        if (isHelp)
        {
            usage(argc, argv, pos, consoleOut());
            return 0;
        }
    }
    if (!isGoodRequest())
    {
        StringBuffer err;
        err << "incomplete request\n\n";
        consoleErr().put(err.length(), err.str());
        usage(argc, argv, pos, consoleErr());
        return 1;
    }
    return doRequest();
}

void CEvToolCommand::usage(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out)
{
    usageSyntax(argc, argv, pos, out);
    usageSynopsis(out);
    usageOptions(out);
    usageFilters(out);
    usageParameters(out);
    usageDetails(out);
}

bool CEvToolCommand::accept(const char* arg)
{
    if (!arg)
        return false;
    switch (arg[0])
    {
    case '\0':
        return false;
    case '-':
        switch (arg[1])
        {
        case '\0':
            return false;
        case '-':
            return arg[2] && acceptVerboseOption(arg + 2);
        default:
            for (size_t idx = 1; arg[idx]; ++idx)
            {
                if (!acceptTerseOption(arg[idx]))
                    return false;
            }
            break;
        }
        break;
    default:
        return acceptParameter(arg);
    }
    return true;
}

bool CEvToolCommand::acceptTerseOption(char opt)
{
    if (('?' == opt) || ('h' == opt))
    {
        isHelp = true;
        return true;
    }
    return false;
}

bool CEvToolCommand::acceptVerboseOption(const char* opt)
{
    if (streq(opt, "help"))
    {
        isHelp = true;
        return true;
    }
    else
    {
        const char* valueDelim = strchr(opt, '=');
        if (!valueDelim || !valueDelim[1])
            return false;
        StringAttr key(opt, valueDelim - opt);
        return acceptKVOption(key, valueDelim + 1);
    }
    return false;
}

bool CEvToolCommand::acceptKVOption(const char* key, const char* value)
{
    return false;
}

bool CEvToolCommand::acceptParameter(const char* arg)
{
    return false;
}

void CEvToolCommand::usageSyntax(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out)
{
    StringBuffer prefix("usage: ");
    const char* delim = strrchr(argv[0], PATHSEPCHAR);
    if (delim)
        prefix << delim + 1;
    else
        prefix << argv[0];
    prefix << " ";
    for (int idx = 1; idx <= pos; ++idx)
        prefix << argv[idx] << " ";
    out.put(prefix.length(), prefix.str());
}

void CEvToolCommand::usageSynopsis(IBufferedSerialOutputStream& out)
{
}

void CEvToolCommand::usageOptions(IBufferedSerialOutputStream& out)
{
    constexpr const char* usageStr = R"!!!(
Options:
    -?, -h, --help            Show this help message and exit.
)!!!";
    size32_t usageStrLength = size32_t(strlen(usageStr));
    out.put(usageStrLength, usageStr);
}

void CEvToolCommand::usageFilters(IBufferedSerialOutputStream& out)
{
}

void CEvToolCommand::usageParameters(IBufferedSerialOutputStream& out)
{
}

void CEvToolCommand::usageDetails(IBufferedSerialOutputStream& out)
{
}

IPropertyTree* CEvToolCommand::loadConfiguration(const char* path) const
{
    Owned<IPropertyTree> tree;
    StringBuffer markup;
    markup.loadFile(path);
    if (markup.isEmpty())
        throw makeStringExceptionV(-1, "failed to load configuration '%s'", path);
    if (markup.charAt(0) == '<') // looks like XML
        tree.setown(createPTreeFromXMLString(markup));
    else if (markup.charAt(0) == '{') // looks like JSON
        tree.setown(createPTreeFromJSONString(markup));
    else // assume YAML
        tree.setown(createPTreeFromYAMLString(markup));
    if (!tree)
        throw makeStringExceptionV(-1, "invalid configuration '%s'", path);
    return tree.getClear();
}

namespace
{
    static Owned<IBufferedSerialOutputStream> out;
    static Owned<IBufferedSerialOutputStream> err;
}

IBufferedSerialOutputStream& consoleOut()
{
    if (!out)
    {
        Owned<IFile> file = createIFile("stdout:");
        Owned<IFileIO> fileIO = file->open(IFOwrite);
        Owned<ISerialOutputStream> baseStream = createSerialOutputStream(fileIO);
        out.setown(createBufferedOutputStream(baseStream, 0x10000, false));
    }
    return *out;
}

IBufferedSerialOutputStream& consoleErr()
{
    if (!err)
    {
        Owned<IFile> file = createIFile("stderr:");
        Owned<IFileIO> fileIO = file->open(IFOwrite);
        Owned<ISerialOutputStream> baseStream = createSerialOutputStream(fileIO);
        err.setown(createBufferedOutputStream(baseStream, 0x10000, false));
    }
    return *err;
}

void cleanupConsole()
{
    if (out)
    {
        out->flush();
        out.clear();
    }
    if (err)
    {
        err->flush();
        err.clear();
    }
}

int CEvtCommandGroup::dispatch(int argc, const char* argv[], int pos)
{
    if (pos + 1 >= argc)
    {
        usage(argc, argv, pos, consoleOut());
        return 0;
    }
    const char* subcmd = argv[pos + 1];
    if (!subcmd)
    {
        usage(argc, argv, pos, consoleErr());
        return 1;
    }
    CmdMap::iterator it = commands.find(subcmd);
    if (commands.end() == it)
    {
        StringBuffer err;
        err << "unknown command: " << subcmd << "\n\n";
        consoleErr().put(err.length(), err.str());
        usage(argc, argv, pos, consoleErr());
        return 1;
    }
    Owned<IEvToolCommand> cmd = it->second();
    return cmd->dispatch(argc, argv, pos + 1);
}

void CEvtCommandGroup::usage(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out)
{
    std::vector<const char*> args;
    for (int idx = 0; idx <= pos; ++idx)
        args.push_back(argv[idx]);
    args.push_back(nullptr);
    for (const CmdMap::value_type& c : commands)
    {
        Owned<IEvToolCommand> cmd = c.second();
        args.back() = c.first.c_str();
        cmd->usage(2, &args.at(0), pos + 1, out);
        out.put(1, "\n");
    }
}

CEvtCommandGroup::CEvtCommandGroup(CmdMap& _commands)
    : commands(_commands)
{
}

CEvtCommandGroup::CEvtCommandGroup(CmdMap&& _commands)
    : commands(std::move(_commands))
{
}
