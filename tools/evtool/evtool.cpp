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
#include "jstring.hpp"
#include <iostream>

int CEvToolCommand::dispatch(int argc, const char* argv[], int pos)
{
    for (int idx = pos + 1; idx < argc; ++idx)
    {
        if (!accept(argv[idx]))
        {
            std::cerr << "invalid argument: " << argv[idx] << std::endl << std::endl;
            usage(argc, argv, idx, std::cerr);
            return 1;
        }
        if (isHelp)
        {
            usage(argc, argv, idx, std::cout);
            return 0;
        }
    }
    if (!isGoodRequest())
    {
        std::cerr << "incomplete request" << std::endl << std::endl;
        usage(argc, argv, pos, std::cerr);
        return 1;
    }
    return doRequest();
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
    return false;
}

bool CEvToolCommand::acceptParameter(const char* arg)
{
    return false;
}

void CEvToolCommand::usagePrefix(int argc, const char* argv[], int pos, std::ostream& out)
{
    out << "usage: ";
    const char* delim = strrchr(argv[0], PATHSEPCHAR);
    if (delim)
        out << delim + 1;
    else
        out << argv[0];
    out << " ";
    for (int idx = 1; idx <= pos; ++idx)
        out << argv[idx] << " ";
}
