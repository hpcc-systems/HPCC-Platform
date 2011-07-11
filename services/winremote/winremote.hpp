/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef WINREMOTE_INCL
#define WINREMOTE_INCL

#include "jiface.hpp"

#ifdef WIN32
    #ifdef WINREMOTE_EXPORTS
        #define WINREMOTE_API __declspec(dllexport)
    #else
        #define WINREMOTE_API __declspec(dllimport)
    #endif
#else
    #define WINREMOTE_API
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

