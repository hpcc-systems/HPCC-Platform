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

#pragma once

#include "jiface.hpp"
#include <ostream>

// Core interface for tool commands. `dispatch` processes individual requests. `usage` provides
// help text for the command.
interface IEvToolCommand : extends IInterface
{
    virtual int dispatch(int argc, const char* argv[], int pos) = 0;
    virtual void usage(int argc, const char* argv[], int pos, std::ostream& out) = 0;
};

// Command factory functions.
extern IEvToolCommand* createDumpCommand();
