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

#ifndef _MSG_H
#define _MSG_H

#include "platform.h"
#include "win32.hpp"
#include <tchar.h>
#include <Lmcons.h>
#include "service/service.h"

#define PSEXECPIPE   TEXT("PsExec")
#define PSEXECSVCEXE TEXT("psexecsvc.exe")


struct CmdMessage
{
    CmdMessage()
    {
        memset(this,0,sizeof(*this));
    }

    CmdMessage(const char* type,const char* user,const char* pwd)
    {
        strcpy(command,type);
        processId = ::GetCurrentProcessId();
        threadId = ::GetCurrentThreadId();
        DWORD sz=arraysize(machine);
        ::GetComputerNameA(machine,&sz);
        memset(username,0,sizeof(username));
        memset(password,0,sizeof(password));

        if(user && *user)
        {
            _snprintf(username,arraysize(username)-1,"%hs",user);
        }

        if(pwd && *pwd) 
        {
            _snprintf(password,arraysize(password)-1,"%hs",pwd);
        }
            
    }

    LPTSTR GetStdin(LPTSTR buf,size32_t size,LPCSTR remote=".") const
    {
        _sntprintf(buf,size,_T("\\\\%hs\\pipe\\%s.%hs.%d.%d.in"),remote,PSEXECPIPE,machine,processId,threadId);
        return buf;
    }

    LPTSTR GetStdout(LPTSTR buf,size32_t size,LPCSTR remote=".")  const
    {
        _sntprintf(buf,size,_T("\\\\%hs\\pipe\\%s.%hs.%d.%d.out"),remote,PSEXECPIPE,machine,processId,threadId);
        return buf;
    }

    LPTSTR GetStderr(LPTSTR buf,size32_t size,LPCSTR remote=".")  const
    {
        _sntprintf(buf,size,_T("\\\\%hs\\pipe\\%s.%hs.%d.%d.err"),remote,PSEXECPIPE,machine,processId,threadId);
        return buf;
    } 

    CHAR  command[16];
    DWORD processId;
    DWORD threadId;
    CHAR  machine[MAX_COMPUTERNAME_LENGTH+1];
    CHAR  username[UNLEN+1];
    CHAR  password[PWLEN+1];

};

struct ExecMessage
{
    enum Flags {NoWait=1};
    
    ExecMessage()
    {
        memset(this,0,sizeof(*this));
    }

    ExecMessage(LPCTSTR _command,LPCTSTR _workingDir=NULL,DWORD _flags=0,DWORD _priority=NORMAL_PRIORITY_CLASS,DWORD _session=0)
    {
        _snwprintf(command,arraysize(command),L"%hs",_command);
        if(_workingDir)
            _snwprintf(workingDir,arraysize(workingDir),L"%hs",_workingDir);
        else
            *workingDir=0;
        priority=_priority;
        flags=_flags;
        session=_session;
    }

    LPWSTR GetCommand()  const
    {
        return (LPWSTR)command;
    }

    LPCWSTR GetWorkingDir() const
    {
        return *workingDir ? workingDir : NULL;
    }

    DWORD GetPriority()  const
    {
        return priority;
    }

    DWORD GetSession()  const
    {
        return session;
    }

    bool IsNoWait() const
    {
        return (flags&NoWait);
    }

    WCHAR command[1024];
    WCHAR workingDir[_MAX_PATH];
    DWORD priority;
    DWORD flags;
    DWORD session;
};

struct ExecResult
{
    ExecResult()
    {
        memset(this,0,sizeof(*this));
    }

    DWORD err;
    DWORD pid;
};

struct ListMessage
{
    ListMessage()
    {
    }

    ListMessage(LPCTSTR _path)
    {
        if(_path)
            _snwprintf(path,arraysize(path),L"%hs",_path);
        else
            *path=0;
    }

    ListMessage(int pid)
    {
        _snwprintf(path,arraysize(path),L":%d",pid);
    }


    bool hasPid()
    {
        return *path==':';
    }

    LPCWSTR GetPath() const
    {
        return *path ? path : 0;
    }

    DWORD GetPid() const
    {
        return _wtoi(path+1);
    }

    WCHAR path[_MAX_PATH];
};

struct ListResult
{
    ListResult()
    {
        memset(this,0,sizeof(*this));
    }

    ListResult(DWORD _pid,DWORD _parent,DWORD _session,LPCWSTR _app,LPCWSTR _cmd,LPCWSTR _dir)
    {
        pid=_pid;
        parent=_parent;
        session=_session;
        wcsncpy(appName,_app,arraysize(appName));
        wcsncpy(commandLine,_cmd,arraysize(commandLine));
        wcsncpy(workingDir,_dir,arraysize(workingDir));
    }

    DWORD GetPid()
    {
        return pid;
    }

    DWORD GetParent()
    {
        return parent;
    }

    DWORD GetSession()
    {
        return session;
    }

    LPCWSTR GetAppName() const
    {
        return appName;
    }

    LPCWSTR GetCommandLine() const
    {
        return commandLine;
    }

    LPCWSTR GetWorkingDir() const
    {
        return workingDir;
    }

    DWORD pid;
    DWORD parent;
    DWORD session;
    WCHAR appName[_MAX_PATH];
    WCHAR commandLine[MAX_COMMAND];
    WCHAR workingDir[_MAX_PATH];
};

#endif
