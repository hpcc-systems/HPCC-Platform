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
#include "jstream.hpp"
#include "jstring.hpp"
#include <functional>
#include <map>
#include <string>

// Core interface for tool commands. `dispatch` processes individual requests. `usage` provides
// help text for the command.
interface IEvToolCommand : extends IInterface
{
public:
    virtual int dispatch(int argc, const char* argv[], int pos) = 0;
    virtual void usage(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out) = 0;
protected:
    friend class CEvtCommandGroup;
    virtual bool hasVerboseDescription() const = 0;
    virtual const char* getVerboseDescription() const = 0;
    virtual bool hasBriefDescription() const = 0;
    virtual const char* getBriefDescription() const = 0;
};

// Base class that standardizes help argument handling and usage output generation
class CEvtCommandBase : public CInterfaceOf<IEvToolCommand>
{
public: // IEvToolCommand
    virtual int dispatch(int argc, const char* argv[], int pos) override;
    virtual void usage(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out) override;
protected:
    // Help handling
    virtual bool acceptHelpOption(const char* arg);
    virtual bool isHelpRequested() const { return isHelp; }

    // Dispatch handling (can be overridden by subclasses like command groups)
    virtual int doDispatch(int argc, const char* argv[], int pos);

    // Usage generation components
    virtual void usageSyntax(StringBuffer& helpText);
    virtual void usageDescription(StringBuffer& helpText);
    virtual void usageOptions(IBufferedSerialOutputStream& out);
    virtual void usageParameters(IBufferedSerialOutputStream& out);
    virtual void usageDetails(IBufferedSerialOutputStream& out);

    // Abstract methods for different command types
    virtual bool acceptArgument(const char* arg) = 0;
    virtual bool isValidRequest() = 0;
    virtual int executeCommand() = 0;

    // Helper for standardized syntax prefix generation
    void usageSyntaxPrefix(StringBuffer& prefix, int argc, const char* argv[], int pos);

private:
    bool isHelp = false;
};

using CmdCreator = std::function<IEvToolCommand*()>;
using CmdMap = std::map<std::string, CmdCreator>;

// Concrete implementation of the command interface that manages a choice of multiple subcommands.
class CEvtCommandGroup : public CEvtCommandBase
{
public:
    CEvtCommandGroup(CmdMap& _commands, const char* _verbose, const char* _brief);
    CEvtCommandGroup(CmdMap&& _commands, const char* _verbose, const char* _brief);

    // Override dispatch to handle subcommands
    virtual int dispatch(int argc, const char* argv[], int pos) override;
    virtual void usage(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out) override;

protected:
    // CEvtCommandBase implementation (fallback methods, not used in normal dispatch)
    virtual bool acceptArgument(const char* arg) override;
    virtual bool isValidRequest() override;
    virtual int executeCommand() override;
    virtual void usageSyntax(StringBuffer& helpText) override;
    virtual void usageParameters(IBufferedSerialOutputStream& out) override;

    // IEvToolCommand description interface
    virtual bool hasVerboseDescription() const override;
    virtual const char* getVerboseDescription() const override;
    virtual bool hasBriefDescription() const override;
    virtual const char* getBriefDescription() const override;

protected:
    CmdMap commands;
    StringAttr verbose;
    StringAttr brief;
};

extern IBufferedSerialOutputStream& consoleOut();
extern IBufferedSerialOutputStream& consoleErr();
extern void cleanupConsole();

// Command factory functions.
extern IEvToolCommand* createDumpCommand();
extern IEvToolCommand* createSaveAsCommand();
extern IEvToolCommand* createSimCommand();
extern IEvToolCommand* createIndexCommand();
