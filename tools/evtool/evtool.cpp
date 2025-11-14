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

static constexpr const char* descriptionNotAvailable = "Description not available.\n";

int CEvtCommandBase::dispatch(int argc, const char* argv[], int pos)
{
    // Allow subclasses to override the entire dispatch logic if needed
    return doDispatch(argc, argv, pos);
}

int CEvtCommandBase::doDispatch(int argc, const char* argv[], int pos)
{
    for (int idx = pos + 1; idx < argc; ++idx)
    {
        if (acceptHelpOption(argv[idx]))
        {
            if (isHelpRequested())
            {
                usage(argc, argv, pos, consoleOut());
                return 0;
            }
        }
        else if (!acceptArgument(argv[idx]))
        {
            StringBuffer err;
            err << "invalid argument: " << argv[idx] << "\n\n";
            consoleErr().put(err.length(), err.str());
            usage(argc, argv, pos, consoleErr());
            return 1;
        }
    }

    if (isHelpRequested())
    {
        usage(argc, argv, pos, consoleOut());
        return 0;
    }

    if (!isValidRequest())
    {
        StringBuffer err;
        err << "incomplete request\n\n";
        consoleErr().put(err.length(), err.str());
        usage(argc, argv, pos, consoleErr());
        return 1;
    }
    return executeCommand();
}

void CEvtCommandBase::usage(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out)
{
    StringBuffer helpText("usage: ");
    usageSyntaxPrefix(helpText, argc, argv, pos);
    usageSyntax(helpText);
    usageDescription(helpText);
    out.put(helpText.length(), helpText.str());
}

bool CEvtCommandBase::acceptHelpOption(const char* arg)
{
    if (!arg)
        return false;

    // Check for -h, -?, --help
    if (streq(arg, "-h") || streq(arg, "-?") || streq(arg, "--help"))
    {
        isHelp = true;
        return true;
    }

    // Check for terse options containing h or ?
    if (arg[0] == '-' && arg[1] != '-' && arg[1] != '\0')
    {
        for (size_t i = 1; arg[i]; ++i)
        {
            if (arg[i] == 'h' || arg[i] == '?')
            {
                isHelp = true;
                return true;
            }
        }
    }

    return false;
}

void CEvtCommandBase::usageSyntax(StringBuffer& helpText)
{
    helpText.append('\n');
}

void CEvtCommandBase::usageDescription(StringBuffer& helpText)
{
    const char* description = getVerboseDescription();
    if (isEmptyString(description))
    {
        if (hasBriefDescription())
            description = getBriefDescription();
        else
            description = descriptionNotAvailable;
    }
    helpText << "\n" << description << "\n";
}

void CEvtCommandBase::usageOptions(IBufferedSerialOutputStream& out)
{
    constexpr const char* usageStr = R"!!!(Options:
    -?, -h, --help            Show this help message and exit.
)!!!";
    size32_t usageStrLength = size32_t(strlen(usageStr));
    out.put(usageStrLength, usageStr);
}

void CEvtCommandBase::usageParameters(IBufferedSerialOutputStream& out)
{
}

void CEvtCommandBase::usageDetails(IBufferedSerialOutputStream& out)
{
}

void CEvtCommandBase::usageSyntaxPrefix(StringBuffer& prefix, int argc, const char* argv[], int pos)
{
    const char* delim = strrchr(argv[0], PATHSEPCHAR);
    if (delim)
        prefix << delim + 1;
    else
        prefix << argv[0];
    for (int idx = 1; idx <= pos; ++idx)
        prefix << " " << argv[idx];
    prefix.append(' ');
}

void CEvToolCommand::usage(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out)
{
    CEvtCommandBase::usage(argc, argv, pos, out);
    usageOptions(out);
    usageFilters(out);
    usageParameters(out);
    usageDetails(out);
}

bool CEvToolCommand::acceptArgument(const char* arg)
{
    return accept(arg);
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
    // Help options are now handled by base class
    return false;
}

bool CEvToolCommand::acceptVerboseOption(const char* opt)
{
    // Help options are now handled by base class
    const char* valueDelim = strchr(opt, '=');
    if (!valueDelim || !valueDelim[1])
        return false;
    StringAttr key(opt, valueDelim - opt);
    return acceptKVOption(key, valueDelim + 1);
}

bool CEvToolCommand::acceptKVOption(const char* key, const char* value)
{
    return false;
}

bool CEvToolCommand::acceptParameter(const char* arg)
{
    return false;
}

void CEvToolCommand::usageOptions(IBufferedSerialOutputStream& out)
{
    // Call base class implementation for standard options
    CEvtCommandBase::usageOptions(out);
}

void CEvToolCommand::usageFilters(IBufferedSerialOutputStream& out)
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
        out.setown(createBufferedOutputStream(baseStream, 0x10000));
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
        err.setown(createBufferedOutputStream(baseStream, 0x10000));
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
    // Check for help first
    if (pos + 1 < argc)
    {
        const char* arg = argv[pos + 1];
        if (acceptHelpOption(arg))
        {
            usage(argc, argv, pos, consoleOut());
            return 0;
        }
    }

    // If no subcommand provided, show usage
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
    CEvtCommandBase::usage(argc, argv, pos, out);
    usageParameters(out);
}

bool CEvtCommandGroup::acceptArgument(const char* arg)
{
    // Not used in normal dispatch flow
    return false;
}

bool CEvtCommandGroup::isValidRequest()
{
    // Not used in normal dispatch flow
    return true;
}

int CEvtCommandGroup::executeCommand()
{
    // Not used in normal dispatch flow
    return 0;
}

void CEvtCommandGroup::usageSyntax(StringBuffer& helpText)
{
    helpText.append(R"!!!(<command> [command-parameters]
)!!!");
}
void CEvtCommandGroup::usageParameters(IBufferedSerialOutputStream& out)
{
    constexpr const char* usageStr =
R"!!!(Commands:
)!!!";
    size32_t usageStrLength = size32_t(strlen(usageStr));
    out.put(usageStrLength, usageStr);

    for (const CmdMap::value_type& entry : commands)
    {
        out.put(4, "    ");
        out.put(entry.first.length(), entry.first.c_str());
        out.put(2, ": ");
        Owned<IEvToolCommand> cmd = entry.second();
        if (cmd->hasBriefDescription())
        {
            const char* desc = cmd->getBriefDescription();
            out.put(strlen(desc), desc);
        }
        out.put(1, "\n");
    }

    constexpr const char* advice = R"!!!(
Request help for any command for more information.
)!!!";
    out.put(size32_t(strlen(advice)), advice);
}

bool CEvtCommandGroup::hasVerboseDescription() const
{
    return !verbose.isEmpty();
}

const char* CEvtCommandGroup::getVerboseDescription() const
{
    return verbose.get();
}

bool CEvtCommandGroup::hasBriefDescription() const
{
    return !brief.isEmpty();
}

const char* CEvtCommandGroup::getBriefDescription() const
{
    return brief.get();
}

CEvtCommandGroup::CEvtCommandGroup(CmdMap& _commands, const char* _verbose, const char* _brief)
    : commands(_commands)
    , verbose(_verbose)
    , brief(_brief)
{
    if (brief.isEmpty())
        brief.set(descriptionNotAvailable);
}

CEvtCommandGroup::CEvtCommandGroup(CmdMap&& _commands, const char* _verbose, const char* _brief)
    : commands(std::move(_commands))
    , verbose(_verbose)
    , brief(_brief)
{
    if (brief.isEmpty())
        brief.set(descriptionNotAvailable);
}
