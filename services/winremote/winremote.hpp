/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef WINREMOTE_INCL
#define WINREMOTE_INCL

#include "jiface.hpp"

#ifdef WINREMOTE_EXPORTS
    #define WINREMOTE_API DECL_EXPORT
#else
    #define WINREMOTE_API DECL_IMPORT
#endif

interface IRemoteOutput : extends IInterface
{
    virtual void write(const char * ip, const char * data) = 0;
};

extern "C" WINREMOTE_API IRemoteOutput* queryStdout();
extern "C" WINREMOTE_API IRemoteOutput* queryStderr();

interface IRemoteCommand : extends IInterface
{
    virtual void setCommand(const char * command) = 0;
    virtual void setPriority(int priority) = 0;
    virtual void setCopyFile(bool overwrite) = 0;
    virtual void setWorkDirectory(const char * dir) = 0;
    virtual void setSession(int session) = 0;
    virtual void setOutputStream(IRemoteOutput * _out) = 0;
    virtual void setErrorStream(IRemoteOutput * _err) = 0;
    virtual void setNetworkLogon(bool _set) = 0;
    virtual int getErrorStatus() const = 0;
    virtual int getPid() const = 0;
};

extern "C" WINREMOTE_API IRemoteCommand* createRemoteCommand();

interface IRemoteProcess : extends IInterface
{
    virtual IStringVal & getMachine(IStringVal & val) const = 0;
    virtual int getPid() const = 0;
    virtual int getParent() const = 0;
    virtual IStringVal & getApplicationName(IStringVal & val) const = 0;
    virtual IStringVal & getCommandLine(IStringVal & val) const = 0;
    virtual IStringVal & getCurrentDirectory(IStringVal & val) const = 0;
};

typedef IIteratorOf<IRemoteProcess> IRemoteProcessIterator;

interface IRemoteAgent : extends IInterface
{
    virtual void addMachine(const char * machine, const char * user, const char * pwd) = 0;
    virtual void startCommand(IRemoteCommand * cmd) = 0;
    virtual IRemoteProcessIterator * getProcessList(const char * path, IRemoteOutput * _err) = 0;
    virtual void stop() = 0;
    virtual void wait() = 0;
};

extern "C" WINREMOTE_API IRemoteAgent* createRemoteAgent();

#endif // WINREMOTE_INCL

