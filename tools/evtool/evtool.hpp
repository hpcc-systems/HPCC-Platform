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
#include "jstring.hpp"

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
    void usagePrefix(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out);
protected:
    bool isHelp = false;
};

// Concrete implementation of the command interface that manages a choice of multiple subcommands.
class CEvtCommandGroup : public CInterfaceOf<IEvToolCommand>
{
public: // IEvToolCommand
    virtual int dispatch(int argc, const char* argv[], int pos) override;
    virtual void usage(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out) override;
public:
    CEvtCommandGroup(CmdMap&& _commands);
protected:
    CmdMap commands;
};

// Abstract base class for event file operation implementations. Derived classes should be
// independent of the command line interface. The `IEvToolCommand` interface is used to connect
// operation implementations to the command line interface.
//
// A rule of thumb when defining an operation and a connecting command is that anything that
// would be needed for an ESP to reuse the operation belongs in the operation, while anything
// specific to converting command line arguments to operation parameters belongs in the command.
class CEventFileOp
{
public:
    virtual bool ready() const;
    virtual int doOp() = 0;
public:
    virtual ~CEventFileOp() = default;
    void setInputPath(const char* path);
protected:
    StringAttr inputPath;
};

// Extension of `CEventFileOp` that supports streaming output.
// There may never be an operation that is not streaming, but the distinction is made for now.
class CStreamingEventFileOp : public CEventFileOp
{
public: // CEventFileOp
    virtual bool ready() const override;
public:
    void setOutput(IBufferedSerialOutputStream& _out);
protected:
    Linked<IBufferedSerialOutputStream> out;
};
