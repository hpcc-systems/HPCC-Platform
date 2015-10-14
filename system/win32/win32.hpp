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

#ifndef __JWIN32_HPP
#define __JWIN32_HPP

#define _CRT_SECURE_NO_WARNINGS
#define _WIN32_WINNT 0x0500
#include <windows.h>

#include <stdio.h>
#include <stdarg.h>
#include <malloc.h> 
#include <tchar.h> 


#ifndef arraysize
#define arraysize(T) (sizeof(T)/sizeof(*T))
#endif

namespace win32
{

class Error
{
public:
    Error(LPCTSTR msg,...): code(0) 
    {
        memset(buf,0,sizeof(buf));
        
        va_list marker;
        va_start(marker,msg); 
        _vsntprintf(buf,arraysize(buf)-1,msg,marker);
        va_end(marker); 
    }
    
    LPCTSTR GetMessage() const { return buf; }
    DWORD   GetCode() const { return code; }


    static void perror()
    {
        DWORD code=::GetLastError();
        LPVOID msg=0;
        if(::FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL, code, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPTSTR) &msg, 0, NULL) && msg)
        {
            _tprintf(TEXT("System error %d: %s\n"),code,(const char *)msg);
            ::LocalFree(msg);
        }
    }

protected:
    Error(): code(::GetLastError()) 
    {
        memset(buf,0,sizeof(buf));
    }

    DWORD code;
    TCHAR buf[256];
};

class SystemError: public Error
{
public:
    SystemError(LPCTSTR msg,...)
    {
        va_list marker;
        va_start(marker,msg); 
        _vsntprintf(buf,arraysize(buf)-1,msg,marker);
        va_end(marker); 

        if(code!=ERROR_SUCCESS)
        {
            LPVOID msg=0;
            if(::FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL, code, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPTSTR) &msg, 0, NULL) && msg)
            {
                int len=_tcslen(buf);
                if(len<arraysize(buf)-1)
                    _sntprintf(buf+len,arraysize(buf)-1-len,TEXT(" - (%d) %s"),code,(const char *)msg);
                ::LocalFree(msg);
            }
        }

    }
};

struct NullHandle
{
    operator HANDLE() { return NULL; }
};

struct InvalidHandle
{
    operator HANDLE() { return INVALID_HANDLE_VALUE; }
};

template<typename inv> class HandleBase
{
public:
    HandleBase(): handle(inv()) {}

    HandleBase(HANDLE h, DWORD access, bool inherit=false, DWORD options=0, HANDLE source=GetCurrentProcess()): handle(inv()) 
    {
        ::DuplicateHandle(source,h,::GetCurrentProcess(),&handle,access,inherit,options); 
    }

    ~HandleBase()     { if(isValid()) Close(); }

    operator HANDLE()  { return handle; }
    bool operator !()  { return !isValid(); }

    void set(HANDLE h)
    {   
        if(isValid())
            Close();
        handle=h;
    }

    HANDLE get()
    {
        HANDLE temp=handle;
        handle=inv();
        return temp;
    }

    void Close()
    {
        ::CloseHandle(handle);
        handle=inv();
    }

    DWORD Wait(int timeout=INFINITE,bool alertable=false)
    {
        return ::WaitForSingleObjectEx(*this,timeout,alertable?TRUE:FALSE);
    }

    bool isSignaled()
    {
        return Wait(0)==WAIT_OBJECT_0;
    }

    bool isValid()
    {
        return handle!=inv();
    }

protected:
    HANDLE handle;

    HandleBase(HANDLE h): handle(h) { }
};

typedef  HandleBase<NullHandle> Handle;


class Event: public Handle
{
public:
    Event(bool manual, bool state, LPSECURITY_ATTRIBUTES sa=NULL):
      Handle(::CreateEvent(sa, manual, state, NULL)) {}

    Event(const char* name, bool manual, bool state, LPSECURITY_ATTRIBUTES sa=NULL):
      Handle(::CreateEventA(sa, manual, state, name)) {}

    Event(const char* name, int access):
      Handle(::OpenEventA(access, FALSE, name)) {}

    Event(const wchar_t* name, bool manual, bool state, LPSECURITY_ATTRIBUTES sa=NULL):
      Handle(::CreateEventW(sa, manual, state, name)) {}

    Event(const wchar_t* name, int access):
      Handle(::OpenEventW(access, FALSE, name)) {}

    bool Set()
    {
        return ::SetEvent(*this)!=FALSE;
    }

    bool Reset()
    {
        return ::ResetEvent(*this)!=FALSE;
    }
};


struct Overlapped:public OVERLAPPED
{
    Overlapped(LPCTSTR name=NULL)
    {
        memset((OVERLAPPED*)this,0,sizeof(OVERLAPPED));
        hEvent=::CreateEvent(NULL,TRUE,FALSE,name);
    }

    ~Overlapped()
    {
        ::CloseHandle(hEvent);
    }

    operator HANDLE()
    {
        return hEvent; 
    }

    DWORD Wait(int timeout=INFINITE,bool alertable=false)
    {
        return ::WaitForSingleObjectEx(*this,timeout,alertable?TRUE:FALSE);
    }

    bool isSignaled()
    {
        return Wait(0)==WAIT_OBJECT_0;
    }

    void Reset()
    {
        HANDLE evt=hEvent;
        ::ResetEvent(evt);
        memset((OVERLAPPED*)this,0,sizeof(OVERLAPPED));
        hEvent=evt;
    }
};

class File: public HandleBase<InvalidHandle>
{
public:
    File() {}

    File(LPCTSTR name,DWORD access,DWORD share=0,DWORD create=OPEN_EXISTING,DWORD flags=0,LPSECURITY_ATTRIBUTES sa=NULL):
      HandleBase<InvalidHandle>(::CreateFile(name,access,share,sa,create,flags,NULL))
    {
    }

    operator bool()    { return isValid(); }

    bool Open(LPCSTR name,DWORD access,DWORD share=0,DWORD create=OPEN_EXISTING,DWORD flags=0,LPSECURITY_ATTRIBUTES sa=NULL)
    {
        set(::CreateFileA(name,access,share,sa,create,flags,NULL));
        return isValid();
    }

    bool Open(LPCWSTR name,DWORD access,DWORD share=0,DWORD create=OPEN_EXISTING,DWORD flags=0,LPSECURITY_ATTRIBUTES sa=NULL)
    {
        set(::CreateFileW(name,access,share,sa,create,flags,NULL));
        return isValid();
    }

    DWORD Read(LPVOID buffer,DWORD count,LPOVERLAPPED ovr=NULL)
    {
        DWORD temp=0;
        ::ReadFile(*this,buffer,count,&temp,ovr);
        return temp;
    }

    DWORD Write(LPCVOID buffer,DWORD count,LPOVERLAPPED ovr=NULL)
    {
        DWORD temp=0;
        ::WriteFile(*this,buffer,count,&temp,ovr);
        return temp;
    }

    DWORD GetResult(LPOVERLAPPED ovr, bool wait=false)
    {
        DWORD temp=0;
        ::GetOverlappedResult(*this,ovr,&temp,wait ? TRUE : FALSE);
        return temp;
    }

    DWORD Transact(LPVOID inBuffer,DWORD inBufferSize,LPVOID outBuffer,DWORD outBufferSize,LPOVERLAPPED lpOverlapped=NULL)
    {
        DWORD temp=0;
        ::TransactNamedPipe(*this,inBuffer,inBufferSize,outBuffer,outBufferSize,&temp,lpOverlapped);
        return temp;
    }

    BOOL SetPipeState(LPDWORD mode,LPDWORD maxCollectionCount=0,LPDWORD collectDataTimeout=0)
    {
        return ::SetNamedPipeHandleState(*this,mode,maxCollectionCount,collectDataTimeout);
    }

    BOOL SetPipeMode(DWORD mode)
    {
        return SetPipeState(&mode);
    }

    BOOL CancelIO()
    {
        return ::CancelIo(*this);
    }

protected:
    File(HANDLE h): HandleBase<InvalidHandle>(h) {}
};

class NamedPipe: public File
{
public:
    NamedPipe() {}

    NamedPipe(LPCTSTR name,DWORD openMode,DWORD pipeMode=PIPE_TYPE_BYTE|PIPE_READMODE_BYTE|PIPE_WAIT,DWORD maxInstances=PIPE_UNLIMITED_INSTANCES, DWORD outBufferSize=0,DWORD inBufferSize=0,DWORD defaultTimeOut=-1,LPSECURITY_ATTRIBUTES sa=NULL):
        File(::CreateNamedPipe(name,openMode,pipeMode,maxInstances,outBufferSize,inBufferSize,defaultTimeOut,sa))
    {
    }

    ~NamedPipe() {}

    bool Open(LPCSTR name,DWORD openMode,DWORD pipeMode=PIPE_TYPE_BYTE|PIPE_READMODE_BYTE|PIPE_WAIT,DWORD maxInstances=PIPE_UNLIMITED_INSTANCES, DWORD outBufferSize=0,DWORD inBufferSize=0,DWORD defaultTimeOut=-1,LPSECURITY_ATTRIBUTES sa=NULL)
    {
        set(::CreateNamedPipeA(name,openMode,pipeMode,maxInstances,outBufferSize,inBufferSize,defaultTimeOut,sa)); 
        return isValid();
    }

    bool Open(LPCWSTR name,DWORD openMode,DWORD pipeMode=PIPE_TYPE_BYTE|PIPE_READMODE_BYTE|PIPE_WAIT,DWORD maxInstances=PIPE_UNLIMITED_INSTANCES, DWORD outBufferSize=0,DWORD inBufferSize=0,DWORD defaultTimeOut=-1,LPSECURITY_ATTRIBUTES sa=NULL)
    {
        set(::CreateNamedPipeW(name,openMode,pipeMode,maxInstances,outBufferSize,inBufferSize,defaultTimeOut,sa)); 
        return isValid();
    }


    bool Connect(LPOVERLAPPED ovr=NULL)
    {
        return ::ConnectNamedPipe(*this,ovr)==TRUE || GetLastError()==ERROR_PIPE_CONNECTED;
    }

    bool Disconnect()
    {
        return ::DisconnectNamedPipe(*this)!=FALSE;
    }

    bool ImpersonateClient()
    {
        return ::ImpersonateNamedPipeClient(*this)!=FALSE;
    }

};

class Token: public Handle
{
public:
    Token() {}

    Token(HANDLE existingToken,DWORD access,LPSECURITY_ATTRIBUTES tokenAttributes,SECURITY_IMPERSONATION_LEVEL impersonationLevel,TOKEN_TYPE tokenType)
    {
        if(!::DuplicateTokenEx(existingToken,access,tokenAttributes,impersonationLevel,tokenType,&handle))
        {
        }
    }

    bool AdjustPrivilege(LPCTSTR privname,bool enable)
    {
        int count=1,
            size=sizeof(DWORD)+sizeof(LUID_AND_ATTRIBUTES)*count;

        TOKEN_PRIVILEGES* privs=(TOKEN_PRIVILEGES*)_alloca(size);
        privs->PrivilegeCount=count;
        if(!::LookupPrivilegeValue(NULL,privname,&privs->Privileges[0].Luid))
            return false;
        if(enable)
            privs->Privileges[0].Attributes|=SE_PRIVILEGE_ENABLED;
        else
            privs->Privileges[0].Attributes=0;
        return ::AdjustTokenPrivileges(*this,FALSE,privs,size,NULL,0)!=0 && ::GetLastError()==ERROR_SUCCESS;
    }

    DWORD GetInfo(TOKEN_INFORMATION_CLASS info,LPVOID buf,DWORD size)
    {
        DWORD sz=0;
        if(!::GetTokenInformation(*this,info,buf,size,&sz))
        {
            return 0;
        }
        return sz;
    }

    DWORD GetInfoSize(TOKEN_INFORMATION_CLASS info)
    {
        DWORD sz=0;
        ::GetTokenInformation(*this,info,0,0,&sz);
        return sz;
    }

    BOOL SetInfo(TOKEN_INFORMATION_CLASS info,LPVOID buf,DWORD size)
    {
        return ::SetTokenInformation(*this,info,buf,size);
    }


protected:
};

class ProcessToken: public Token
{
public:

    ProcessToken(int access)
    {
        ::OpenProcessToken(::GetCurrentProcess(),access,&handle);
    }

    ProcessToken(HANDLE process,int access)
    {
      ::OpenProcessToken(process,access,&handle);
    }

};

class ThreadToken: public Token
{
public:

    ThreadToken(int access,bool self=false)
    {
        ::OpenThreadToken(::GetCurrentThread(),access,self?TRUE:FALSE,&handle);
    }

    ThreadToken(HANDLE thread,int access,bool self=false)
    {
        ::OpenThreadToken(thread,access,self?TRUE:FALSE,&handle);
    }

};


class UserToken:public Token
{
    public:
        UserToken(LPSTR lpszUsername,LPSTR lpszDomain,LPSTR lpszPassword,DWORD dwLogonType,WORD dwLogonProvider=LOGON32_PROVIDER_DEFAULT)
        {
            if(!LogonUserA(lpszUsername,lpszDomain,lpszPassword,dwLogonType,dwLogonProvider,&handle))
                handle=NULL;
        }

        UserToken(LPWSTR lpszUsername,LPWSTR lpszDomain,LPWSTR lpszPassword,DWORD dwLogonType,WORD dwLogonProvider=LOGON32_PROVIDER_DEFAULT)
        {
            if(!LogonUserW(lpszUsername,lpszDomain,lpszPassword,dwLogonType,dwLogonProvider,&handle))
                handle=NULL;
        }
};

}

#endif
