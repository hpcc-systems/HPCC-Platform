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

#include "evtool.h"

// Abstract implementation of the command interface. `dispatch` is implemented with the help of
// several virtual and abstract methods. `usage` remains for subclasses to implement.
class CEvToolCommand : public CInterfaceOf<IEvToolCommand>
{
public: // new abstract methods
    virtual bool isGoodRequest() = 0;
    virtual int  doRequest() = 0;
public: // IEvToolCommand
    virtual int dispatch(int argc, const char* argv[], int pos) override;
protected:
    virtual bool accept(const char* arg);
    virtual bool acceptTerseOption(char opt);
    virtual bool acceptVerboseOption(const char* opt);
    virtual bool acceptParameter(const char* arg);
    void usagePrefix(int argc, const char* argv[], int pos, std::ostream& out);
protected:
    bool isHelp = false;
};