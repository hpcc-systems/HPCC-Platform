/*##############################################################################

    Copyright (C) 2025 HPCC Systems®.

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

#include "toolcommand.hpp"
#include "jstring.hpp"
#include <iostream>

using namespace std;

// ----------------------------------------------------------------------------
// IToolCommand
// ----------------------------------------------------------------------------

std::ostream* IToolCommand::out = &cout;
std::ostream* IToolCommand::err = &cerr;

void IToolCommand::setOutputStream(std::ostream& output)
{
    out = &output;
}

void IToolCommand::setErrorStream(std::ostream& error)
{
    err = &error;
}

// ----------------------------------------------------------------------------
// CCommonToolCommand
// ----------------------------------------------------------------------------

void CCommonToolCommand::usagePrefix(ostream& target, int argc, const char* argv[], int pos)
{
    const char* app = strrchr(argv[0], PATHSEPCHAR);
    target << (app ? app + 1 : argv[0]) << ' ';
    for (int idx = 1; idx <= pos; ++idx)
        target << argv[idx] << " ";
}

bool CCommonToolCommand::doImplicitHelp(int argc, const char* argv[], int pos)
{
    if (pos + 1 < argc)
        return false;
    usage(*err, argc, argv, pos);
    return true;
}

bool CCommonToolCommand::doExplicitHelp(int argc, const char* argv[], int pos)
{
    for (int idx = pos; idx < argc; ++idx)
    {
        if (argv[idx] && (streq(argv[idx], "--help") || streq(argv[idx], "-h") || streq(argv[idx], "-?")))
        {
            usage(*out, argc, argv, pos);
            return true;
        }
    }
    return false;
}

// ----------------------------------------------------------------------------
// CAtomicToolCommand
// ----------------------------------------------------------------------------

int CAtomicToolCommand::dispatch(int argc, const char* argv[], int pos)
{
    if (pos < argc && !streq(argv[pos], queryName()))
    {
        *err << "internal error: expected '" << queryName() << "' but got '" << argv[pos] << "'" << endl << endl;
        return 1;
    }
    if (doExplicitHelp(argc, argv, pos))
        return 0;
    return doCommand(argc, argv, pos);
}

void CAtomicToolCommand::usage(ostream& target, int argc, const char* argv[], int pos)
{
    target << queryAbstract() << endl << endl;
    target << "Usage:" << endl;
    target << "    ";
    usagePrefix(target, argc, argv, pos);
    target << "[ options ]" << endl;
    target << endl;
    target << "Options:" << endl;
    target << "    -h, -?, --help : Display this help" << endl << endl;
}

int CAtomicToolCommand::doCommand(int argc, const char* argv[], int pos)
{
    usagePrefix(*out, argc, argv, pos);
    *out << ": command not implemented" << endl << endl;
    return 0;
}

// ----------------------------------------------------------------------------
// CToolCommandGroup
// ----------------------------------------------------------------------------

int CToolCommandGroup::dispatch(int argc, const char* argv[], int pos)
{
    if (pos < argc && !streq(argv[pos], queryName()))
    {
        *err << "internal error: expected '" << queryName() << "' but got '" << argv[pos] << "'" << endl << endl;
        return 1;
    }

    if (doImplicitHelp(argc, argv, pos))
        return 1;

    Group::iterator it = group.find(argv[pos + 1]);
    if (it != group.end())
        return it->second->dispatch(argc, argv, pos + 1);

    if (doExplicitHelp(argc, argv, pos))
        return 0;

    *err << "Unknown command '" << argv[pos + 1] << "'" << endl << endl;
    usage(*err, argc, argv, pos);
    return 1;
}

void CToolCommandGroup::usage(ostream& target, int argc, const char* argv[], int pos)
{
    target << queryAbstract() << endl;
    target << endl;
    target << "Usage:" << endl;
    target << "    ";
    usagePrefix(target, argc, argv, pos);
    target << "[ ( options | command ) ]" << endl;
    target << endl;
    target << "Options:" << endl;
    target << "    -h, -?, --help : Display this help" << endl;
    target << endl;
    target << "Commands:" << endl;
    for (auto& cmd : group)
    {
        target << "    " << cmd.first;
        size_t pad = maxNameLength - cmd.first.length();
        if (pad)
            target << string(pad, ' ');
        target << " : " << cmd.second->queryAbstract() << endl;
    }
    target << endl;
}
