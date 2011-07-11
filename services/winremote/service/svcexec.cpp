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

#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <process.h>
#include <tchar.h>
#include <Lmcons.h>
#include "service.h"
#include "win32.hpp"
#include "winprocess.hpp"
#include "svcmsg.hpp"

using namespace win32;
const int clienttimeout=10000;
const int proxytimeout=5000;
LONG requestCount=0;
Event stopEvent(true,false);

bool Complete(HANDLE evt,DWORD timeout=INFINITE)
{
    if(::GetLastError()==ERROR_IO_PENDING)
    {
        HANDLE events[] = {evt,stopEvent};
        DWORD wait = ::WaitForMultipleObjects(arraysize(events), events, FALSE, timeout);
        switch(wait)
        {
        case WAIT_OBJECT_0:
            return true;

        case WAIT_OBJECT_0+1:
            throw win32::Error(L"Service Stopped");

        case WAIT_TIMEOUT:
            throw win32::Error(L"Operation timed out");
        }
        throw win32::SystemError(L"Wait operation failed");
    }
    return false; 
}

void Read(File& file,void* buf,size32_t size)
{
    Overlapped ovl;
    DWORD count=file.Read(buf,size,&ovl);
    if(!count && Complete(ovl,clienttimeout))
    {
        count=file.GetResult(&ovl);
    }

    if(count!=size) 
        throw win32::Error(L"Read %d bytes instead of %d",count,size);
}

bool ExecProcess(NamedPipe& pipe, CmdMessage& cmd)
{
    ExecMessage msg;
    Read(pipe,&msg,sizeof(msg));

    ExecResult ret;
    ret.err=ERROR_SUCCESS;
    try
    {
        Token utoken;
        if(*cmd.username)
        { 
            CHAR *username, *domain;
            if(username=strchr(cmd.username,'\\'))
            {
                *username++=0;
                domain=cmd.username;
            }
            else
            {
                username=cmd.username;
                domain=NULL;
            }
             
            UserToken ut(username,domain,cmd.password,LOGON32_LOGON_INTERACTIVE);
            if(!ut)
                throw win32::SystemError(L"Failed to logon user");
            if(!ImpersonateLoggedOnUser(ut))
                throw win32::SystemError(L"Failed to impersonate user");
            utoken.set(ut.get());
        }
        else
        {
            if(!pipe.ImpersonateClient())
                throw win32::SystemError(L"Failed to impersonate client");
        }

        STARTUPINFO si={sizeof si, 0, NULL};
        NamedPipe hstdin, hstdout, hstderr;

        DWORD session=msg.GetSession();
        if(!msg.IsNoWait() && session==0)
        {
            PSECURITY_DESCRIPTOR sd=(PSECURITY_DESCRIPTOR) _alloca(SECURITY_DESCRIPTOR_MIN_LENGTH);

            if(!sd || 
                !::InitializeSecurityDescriptor(sd, SECURITY_DESCRIPTOR_REVISION) ||
                !::SetSecurityDescriptorDacl(sd, TRUE, NULL, FALSE))       
            {
                throw win32::SystemError(L"Create security descriptor failed");
            }

            SECURITY_ATTRIBUTES sa={sizeof sa, sd, TRUE};
            TCHAR buf[MAX_PATH];

            if(!hstdin.Open(cmd.GetStdin(buf,arraysize(buf)), PIPE_ACCESS_INBOUND, PIPE_TYPE_BYTE | PIPE_WAIT, PIPE_UNLIMITED_INSTANCES,0,0,-1,&sa))
                throw win32::SystemError(L"Create SDTIN pipe failed");

            if(!hstdout.Open(cmd.GetStdout(buf,arraysize(buf)), PIPE_ACCESS_OUTBOUND, PIPE_TYPE_BYTE | PIPE_WAIT, PIPE_UNLIMITED_INSTANCES,0,0,-1,&sa))
                throw win32::SystemError(L"Create SDTOUT pipe failed");

            if(!hstderr.Open(cmd.GetStderr(buf,arraysize(buf)), PIPE_ACCESS_OUTBOUND, PIPE_TYPE_BYTE | PIPE_WAIT, PIPE_UNLIMITED_INSTANCES,0,0,-1,&sa))
                throw win32::SystemError(L"Create STDERR pipe failed");

            ExecResult ret;
            ret.err=ERROR_SUCCESS;
            DWORD status=ERROR_SUCCESS;
            Overlapped ovl;
            if(!pipe.Transact(&ret,sizeof(ret),&status,sizeof(status),&ovl) && !Complete(ovl,clienttimeout))
                throw win32::SystemError(L"Could not communicate with the client");

            si.hStdOutput = hstdout;
            si.hStdInput  = hstdin;
            si.hStdError  = hstderr;
            si.dwFlags = STARTF_USESTDHANDLES;
        }

        TCHAR username[UNLEN+1];
        DWORD namesz=arraysize(username);
        ::GetUserName(username,&namesz);

        if(*cmd.username)
        { 
            ::RevertToSelf();
        }
        else
        {
            ThreadToken token(TOKEN_READ|TOKEN_DUPLICATE);
            if(!token)
                throw win32::SystemError(L"Get thread token failed");

            SECURITY_IMPERSONATION_LEVEL impersonationLevel=SecurityAnonymous;
            if(!token.GetInfo(TokenImpersonationLevel,&impersonationLevel,sizeof(impersonationLevel)))
                throw win32::SystemError(L"Get impersonation level failed");

            ::RevertToSelf();

            Token imp(token,MAXIMUM_ALLOWED,NULL,impersonationLevel,TokenPrimary);
            if(!imp)
                throw win32::SystemError(L"Create impersonation token failed");
    
            utoken.set(imp.get());
        }

        if(!utoken.SetInfo(TokenSessionId,&session,sizeof(session)))
            throw win32::SystemError(L"Set session");
        

        struct UserProfile
        {
            UserProfile(HANDLE _token,LPTSTR username): token(_token)
            {
                memset(&profile,0,sizeof(profile));
                profile.dwSize=sizeof(profile);
                profile.lpUserName=username; 

                if(!::LoadUserProfile(token,&profile))
                    throw win32::SystemError(L"Could not load user profile");
            }
        
            ~UserProfile()
            {
                ::UnloadUserProfile(token, profile.hProfile); 
            }

            HANDLE token;
            PROFILEINFO profile;

        } userprofile(utoken,username);

        struct EnvBlock
        {
            EnvBlock(HANDLE token,BOOL inherit): ptr(0)
            {
                if(!::CreateEnvironmentBlock(&ptr,token,inherit))
                    throw win32::SystemError(L"Could not create environment block");
            }

            ~EnvBlock()
            {
                ::DestroyEnvironmentBlock(ptr);
            }

            operator LPVOID() { return ptr;}

            LPVOID ptr;
        } env(utoken,FALSE);

        PROCESS_INFORMATION pi;

        if(msg.IsNoWait() || session==0)
        {
            DWORD flags=msg.GetPriority() | CREATE_UNICODE_ENVIRONMENT | (msg.IsNoWait() ? CREATE_NEW_CONSOLE : CREATE_NO_WINDOW);
            if(!::CreateProcessAsUser(utoken, NULL, msg.GetCommand(), NULL, NULL, TRUE, flags, env, msg.GetWorkingDir(), &si, &pi))
                throw win32::SystemError(L"Create process failed %s",msg.GetCommand());
            CloseHandle(pi.hThread);
            CloseHandle(pi.hProcess);
            ret.pid=pi.dwProcessId;
        }
        else
        {
            WCHAR buf[MAX_PATH*4]={0};
            DWORD len=::GetModuleFileName(NULL,buf,arraysize(buf));
            _snwprintf(buf+len,arraysize(buf)-len,L" -proxy %d ",::GetCurrentProcessId());
            len=wcslen(buf);
            cmd.GetStdin(buf+len,arraysize(buf)-1-len);
            wcscat(buf,L" ");
            len=wcslen(buf);
            cmd.GetStdout(buf+len,arraysize(buf)-1-len);
            wcscat(buf,L" ");
            len=wcslen(buf);
            cmd.GetStderr(buf+len,arraysize(buf)-1-len);

            if(!::CreateProcessAsUser(utoken, NULL, buf, NULL, NULL, TRUE, CREATE_SUSPENDED|CREATE_NO_WINDOW|CREATE_UNICODE_ENVIRONMENT, env, msg.GetWorkingDir(), &si, &pi))
                throw win32::SystemError(L"Create proxy process failed");

            Handle thread;
            thread.set(pi.hThread);

            struct Proc: public Handle
            {
                Proc(HANDLE h): Handle(h) {}
                ~Proc() { ::TerminateProcess(*this,0); }
            } proc(pi.hProcess);

            _snwprintf(buf,arraysize(buf),TEXT("\\\\.\\pipe\\") PSEXECPIPE TEXT(".%d.%d"),::GetCurrentProcessId(),pi.dwProcessId);

            NamedPipe proxy(buf,FILE_FLAG_OVERLAPPED|PIPE_ACCESS_DUPLEX, PIPE_TYPE_MESSAGE|PIPE_READMODE_MESSAGE|PIPE_WAIT,1,0,0,-1,NULL);
            if(!proxy)
                throw win32::SystemError(L"Create proxy pipe failed");

            ::ResumeThread(thread);
        
            Overlapped ovl;
            if(!proxy.Connect(&ovl) && !Complete(ovl,proxytimeout))
                throw win32::SystemError(L"Proxy did not connect");

            ovl.Reset();
            DWORD status=ERROR_SUCCESS;
            if(!pipe.Transact(&ret,sizeof(ret),&status,sizeof(status),&ovl) && !Complete(ovl,clienttimeout))
                throw win32::SystemError(L"Could not communicate with the client");

            ovl.Reset();
            if(!proxy.Transact(msg.GetCommand(),sizeof(msg.command),&ret,sizeof(ret),&ovl) && !Complete(ovl,proxytimeout))
                throw win32::SystemError(L"Could not communicate with the proxy");

            proc.Wait();
        }
    }
    catch(const win32::Error& e)
    {
        ret.err=e.GetCode();
        AddToMessageLog((TCHAR*)e.GetMessage());
    }

    Overlapped ovl;
    if(!pipe.Write(&ret,sizeof(ret),&ovl) && !Complete(ovl,clienttimeout))
        throw win32::SystemError(L"Could not send result to the client");
    return true;
}

bool ListProcesses(NamedPipe& pipe, CmdMessage& cmd)
{
    ListMessage msg;
    Read(pipe,&msg,sizeof(msg));

    if(!pipe.ImpersonateClient())
        throw win32::SystemError(L"Impersonate client failed");

    ThreadToken token(TOKEN_ADJUST_PRIVILEGES);
    if(!token.AdjustPrivilege(SE_DEBUG_NAME,true))
        throw win32::SystemError(L"Could not do SE_DEBUG_NAME");
    
    ProcessList procs;
    if(msg.hasPid())
        procs.add(msg.GetPid());
    else
        procs.add(msg.GetPath());

    for(ProcessList::iterator it=procs.begin();it!=procs.end();it++)
    {
        ProcessParams param(*it);
        ListResult res(param.GetPid(),param.GetParent(),param.GetSession(),param.GetApplicationName(),param.GetCommandLine(),param.GetCurrentDirectory());

        Overlapped ovl;
        if(!pipe.Write(&res,sizeof(res),&ovl) && !Complete(ovl,clienttimeout))
            throw win32::SystemError(L"Could not send result to the client");
    }
    return true;
}


void CommunicationPipeThreadProc(void* param)
{ 
    try
    {
        NamedPipe pipe;
        pipe.set((HANDLE)param);

        CmdMessage msg;
        Read(pipe,&msg,sizeof(msg));

        if(stricmp(msg.command,"EXEC")==0)
            ExecProcess(pipe,msg);
        else if(stricmp(msg.command,"LIST")==0)
            ListProcesses(pipe,msg);
        else 
            throw win32::Error(L"Unknown command %.*s",sizeof(msg.command),msg.command);
    }
    catch(const win32::Error& e)
    {
        AddToMessageLog((TCHAR*)e.GetMessage());
    }
    // If this was the last client, let's stop ourself
    if (::InterlockedDecrement(&requestCount)==0)
        stopEvent.Set();
}


extern "C" VOID ServiceStart (DWORD dwArgc, LPTSTR *lpszArgv)
{
    try
    {
        PSECURITY_DESCRIPTOR sd=(PSECURITY_DESCRIPTOR) _alloca(SECURITY_DESCRIPTOR_MIN_LENGTH);

        if(!sd || 
           !::InitializeSecurityDescriptor(sd, SECURITY_DESCRIPTOR_REVISION) ||
           !::SetSecurityDescriptorDacl(sd, TRUE, NULL, FALSE))       
        {
            throw win32::SystemError(L"Create security descriptor failed");
        }

        SECURITY_ATTRIBUTES sa={sizeof sa, sd, FALSE};

        if(!::ReportStatusToSCMgr(SERVICE_RUNNING, NO_ERROR, 3000))
            throw win32::SystemError(L"Report to SCM failed");

        ////////////////////////////////////////////////////////
        //
        // Service is now running, perform work until shutdown
        //
        for(;;)
        {
            // open our named pipe...
            //
            NamedPipe pipe(TEXT("\\\\.\\pipe\\")PSEXECPIPE,FILE_FLAG_OVERLAPPED|PIPE_ACCESS_DUPLEX, PIPE_TYPE_MESSAGE|PIPE_READMODE_MESSAGE|PIPE_WAIT,PIPE_UNLIMITED_INSTANCES,0,0,-1,&sa);
            if(pipe==INVALID_HANDLE_VALUE)
                throw win32::SystemError(L"Create named pipe failed");

            // wait for a connection...
            //
            Overlapped ovl;
            if(pipe.Connect(&ovl) || Complete(ovl))
            {
               ::InterlockedIncrement(&requestCount);
                _beginthread(CommunicationPipeThreadProc, 0, (void*)pipe.get());
            }
        }
    }
    catch(const win32::Error& e)
    {
        _tprintf(TEXT("StartService: %s"),e.GetMessage());
    }
}

extern "C" VOID ServiceStop()
{
   stopEvent.Set();
}

extern "C" VOID SelfDestruct()
{
   HMODULE  module = GetModuleHandle(0);
   TCHAR    buf[MAX_PATH];
   GetModuleFileName(module, buf, MAX_PATH);
   CloseHandle((HANDLE)4);
    __asm 
    {
      lea     eax, buf
      push    0
      push    0
      push    eax
      push    ExitProcess
      push    module
      push    DeleteFile
      push    UnmapViewOfFile
      ret
    }
}


extern "C" VOID ProxyService(int pid,const char* in,const char* out,const char* err)
{
    try
    {
        PSECURITY_DESCRIPTOR sd=(PSECURITY_DESCRIPTOR) _alloca(SECURITY_DESCRIPTOR_MIN_LENGTH);

        if(!sd || 
            !::InitializeSecurityDescriptor(sd, SECURITY_DESCRIPTOR_REVISION) ||
            !::SetSecurityDescriptorDacl(sd, TRUE, NULL, FALSE))       
        {
            throw win32::SystemError(L"Create security descriptor failed");
        }

        SECURITY_ATTRIBUTES sa={sizeof sa, sd, TRUE};

        NamedPipe hstdin, hstdout, hstderr;
            
        if(!hstdin.Open(in, PIPE_ACCESS_INBOUND, PIPE_TYPE_BYTE | PIPE_WAIT, PIPE_UNLIMITED_INSTANCES,0,0,-1,&sa))
            throw win32::SystemError(L"Create SDTIN pipe failed");
        if(!hstdout.Open(out, PIPE_ACCESS_OUTBOUND, PIPE_TYPE_BYTE | PIPE_WAIT, PIPE_UNLIMITED_INSTANCES,0,0,-1,&sa))
            throw win32::SystemError(L"Create SDTOUT pipe failed");
        if(!hstderr.Open(err, PIPE_ACCESS_OUTBOUND, PIPE_TYPE_BYTE | PIPE_WAIT, PIPE_UNLIMITED_INSTANCES,0,0,-1,&sa))
            throw win32::SystemError(L"Create STDERR pipe failed");

        TCHAR buf[1024];
        _snwprintf(buf,arraysize(buf),TEXT("\\\\.\\pipe\\") PSEXECPIPE TEXT(".%d.%d"),pid,::GetCurrentProcessId());

        File parent(buf,GENERIC_READ|GENERIC_WRITE,0,OPEN_EXISTING,FILE_FLAG_WRITE_THROUGH|SECURITY_DELEGATION);
        if(!parent || !parent.Read(buf,arraysize(buf)))
            throw win32::SystemError(L"Could not read from the pipe");

        ExecResult ret;
        ret.err=ERROR_SUCCESS;
        try
        {
            STARTUPINFO si = { sizeof si, 0, NULL };
            si.hStdOutput = hstdout;
            si.hStdInput  = hstdin;
            si.hStdError  = hstderr;
            si.dwFlags = STARTF_USESTDHANDLES;

            PROCESS_INFORMATION pi;
            if(!CreateProcess(NULL, buf, NULL, NULL, TRUE, CREATE_NO_WINDOW, NULL, NULL, &si, &pi))
                throw win32::SystemError(L"Create process failed %s",buf);
            CloseHandle(pi.hThread);
            CloseHandle(pi.hProcess);
            ret.pid=pi.dwProcessId;
        }
        catch(const win32::Error& e)
        {
            ret.err=e.GetCode();
        }

        if(!parent.Write(&ret,sizeof(ret)))
            throw win32::SystemError(L"Could not write to the pipe");

    }
    catch(...)
    {
        //MessageBox(0,e.GetMessage(),L"",MB_OK);
    }
}
