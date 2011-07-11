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

#ifndef __WINPROCESS
#define __WINPROCESS

#include "win32.hpp"
#include "winternl.h"
#include "psapi.h"
#include <vector>
#include <list>
#define MAX_COMMAND 1024

// another kludge to build Win2K programs with NT 4 build environment
extern "C" WINBASEAPI BOOL WINAPI ProcessIdToSessionId(DWORD dwProcessId,DWORD *pSessionId);

namespace win32
{

typedef std::vector<wchar_t> wstring;

class Process: public Handle
{
public:
    Process() 
    {}

    Process(int _pid, int access): Handle(::OpenProcess(access, FALSE, _pid))
    {}

    Process(HANDLE h, int access): Handle(h,access)
    {}


//needs SDK
#ifdef _PSAPI_H_
    TCHAR* GetImageName(TCHAR* buf,size_t size)
    {
        DWORD res=0;
        TCHAR temp[MAX_PATH];
        if(::GetModuleFileNameEx(*this,NULL,temp,arraysize(temp)))
        {
            if(!::GetLongPathName(temp,buf,size))
            {
                _tcsncpy(buf,temp,size);
            }
            return buf;
        }
        return  0;
    }
#endif

    bool Terminate(int code=-1)
    {
        return ::TerminateProcess(*this,code)!=FALSE;
    }

};

class RemoteMemoryAsync
{
public:
    RemoteMemoryAsync(HANDLE proc,void* data,size_t size,DWORD access)
    {
        buf=::VirtualAllocEx(proc,NULL,size,MEM_COMMIT,access);
        if(buf)
            ::WriteProcessMemory(proc,buf,data,size,0);
    }

    operator void*() { return buf; }

protected:
    void* buf;
};

class RemoteMemory: public RemoteMemoryAsync
{
public:
    RemoteMemory(HANDLE _proc,void* data,size_t size,DWORD access):
      RemoteMemoryAsync(_proc,data,size,access), proc(_proc) {}

    ~RemoteMemory()
    {
        if(buf)
            ::VirtualFreeEx(proc,buf,0,MEM_RELEASE);
    }

protected:
    HANDLE proc;
};

class RemoteThread: public Handle
{
public:
    RemoteThread(HANDLE process, LPTHREAD_START_ROUTINE func, LPVOID param, DWORD flags, DWORD stack=0):
      Handle(::CreateRemoteThread(process,NULL,stack,func,param,flags,&id))
    {
    }
protected:
    DWORD id;
};


class ProcessPipe
{
public:
    ProcessPipe(const TCHAR* progname, const char * envp=NULL)
    {
        HANDLE pipeRead, pipeWrite;
        // Create a pipe to read output from the child program
        if(!::CreatePipe(&pipeRead, &pipeWrite, NULL, 0))
        {
            return;
        }
    
        read.set(pipeRead);
        Handle hstdout(pipeWrite,0,true,DUPLICATE_SAME_ACCESS|DUPLICATE_CLOSE_SOURCE);

        // Create a pipe to supply data to the child program
        if(!::CreatePipe(&pipeRead, &pipeWrite, NULL, 0))
        {
            return;
        }

        write.set(pipeWrite);
        Handle hstdin(pipeRead,0,true,DUPLICATE_SAME_ACCESS|DUPLICATE_CLOSE_SOURCE);


        // Prep the child program
        STARTUPINFO StartupInfo;
        memset(&StartupInfo,0,sizeof(StartupInfo));
        StartupInfo.cb = sizeof(StartupInfo);
        StartupInfo.wShowWindow = SW_HIDE;
        StartupInfo.hStdOutput = hstdout;
        StartupInfo.hStdInput  = hstdin;
        StartupInfo.hStdError  = 0;
        StartupInfo.dwFlags = STARTF_USESHOWWINDOW | STARTF_USESTDHANDLES;


        PROCESS_INFORMATION pi;
        // Launch it..
        if (!CreateProcess(NULL,                            // lpApplicationName
                           (TCHAR *) progname,              // lpCommandLine
                           NULL,                             // lpProcessAttributes
                           NULL,                             // lpThreadAttributes
                           TRUE,                             // bInheritHandles
                           CREATE_NO_WINDOW,                 // dwCreationFlags
                           (void *)envp,                     // lpEnvironment
                           NULL,                             // lpCurrentDirectory
                           &StartupInfo,                     // lpStartupInfo
                           &pi))                            // lpProcessInformation
        {
            return;
        }
        CloseHandle(pi.hProcess);
        CloseHandle(pi.hThread);

    }

    size_t Write(const char *input, size_t inputLen) 
    {
        DWORD wrote;
        if(!::WriteFile(write, input, inputLen, &wrote, NULL))
        {
        }
        return wrote;
    }

    void CloseRead()
    {
        read.Close();
    }

    void CloseWrite()
    {
        write.Close();
    }

    size_t Read(char *output, size_t outputLen)
    {
        DWORD bread=0;
        if(::ReadFile(read, output, outputLen, &bread, NULL))
        {
            return bread;
        }
        return bread;
    }


protected:
    Handle read, write;
};

struct ProcessPid: public Process
{
    ProcessPid(): pid(0) {}
    ProcessPid(int _pid,DWORD _access): Process(_pid,PROCESS_DUP_HANDLE|_access), pid(_pid) {}
    int GetPid() { return pid; }
    int GetSession()
    {
        DWORD session=-1;
        if(::ProcessIdToSessionId(pid,&session))
            return session;
        return -1;
    }

    int pid;
};

class ProcessList: public std::list<ProcessPid>
{ 
public:
    ProcessList(DWORD _access=SYNCHRONIZE): access(_access)
    {
    }

    void add(int pid)
    {
        ProcessPid p(pid,access);
        if(p)
        {
#if _MSC_VER >= 1300
            push_back(p);
#else
            iterator it=insert(end());
            std::swap(*it,p);
#endif
        }
    }

    void add(const TCHAR* image)
    {
        TCHAR imagepath[MAX_PATH];
        if(image && !::SearchPath(NULL,image,TEXT(".exe"),arraysize(imagepath),imagepath,NULL))
        {
            return;
        }
        size_t len=imagepath ? _tcslen(imagepath) : 0;
        
        for(DWORD buflen=1024*sizeof(DWORD);;buflen*=2)
        {
            DWORD* processes=(DWORD*)_alloca(buflen), size=0; 

            if (::EnumProcesses(processes, buflen, &size))
            {
                if(buflen<=size && buflen<1000*1000)
                    continue;
                for (unsigned i=1;i<size/sizeof(DWORD);i++)
                {
                    TCHAR path[MAX_PATH]={0};
                    Process proc(processes[i], PROCESS_QUERY_INFORMATION|PROCESS_VM_READ|PROCESS_DUP_HANDLE|access);
                    if(proc && proc.GetImageName(path, arraysize(path)))
                    {
                        if(!image || _tcsnicmp(imagepath,path,len)==0 &&
                                (path[len]==0 ||                     // Filename matches
                                 _tcsrchr(path+len,'\\')==path+len)) // Directories match
                        {
                            add(processes[i]);
                        }
                    }
                }
                break;
            }
            else
                throw Error(TEXT("Could not enum processes"));
        }
    }
    DWORD access;
};


}
#endif
