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

#include "win32.hpp"
#include "winprocess.hpp"
#include "Tlhelp32.h"
#include <list>


using namespace win32;

struct Param1
{
    Param1(unsigned _ctrlC)
    {
        HMODULE kernel=::GetModuleHandle("kernel32.dll");
        SetErrorMode=::GetProcAddress(kernel,"SetErrorMode");
        GenerateConsoleCtrlEvent=::GetProcAddress(kernel,"GenerateConsoleCtrlEvent");
        Sleep=::GetProcAddress(kernel,"Sleep");
        Exit=::GetProcAddress(kernel,"ExitProcess");
        ctrlC=_ctrlC;
    }

    FARPROC SetErrorMode;
    FARPROC GenerateConsoleCtrlEvent;
    FARPROC Sleep;
    FARPROC Exit;
    DWORD   ctrlC; 
};

#pragma code_seg(".eclfunc")
#ifndef _WIN64
static DWORD _declspec(naked) WINAPI ExitProc(LPVOID param)
{
    _asm
    {
        push    ebp
        mov     ebp, esp
        sub     esp, __LOCAL_SIZE

        mov     esi,[param]
        
        push    SEM_FAILCRITICALERRORS|SEM_NOGPFAULTERRORBOX
        call    [esi]Param1.SetErrorMode 

        push    0
        push    CTRL_BREAK_EVENT
        call    [esi]Param1.GenerateConsoleCtrlEvent 

        push    [esi]Param1.ctrlC
        call    [esi]Param1.Sleep

        push    0
        call    [esi]Param1.Exit

        mov     esp, ebp
        pop     ebp
        ret     4
    }

}
#else
static DWORD WINAPI ExitProc(LPVOID param)
{
    return 0;
}
#endif
#pragma code_seg()



class ProcessKillList: public ProcessList
{ 
public:
    ProcessKillList(): ProcessList(SYNCHRONIZE|PROCESS_QUERY_INFORMATION|PROCESS_TERMINATE)
    {
        _restart=false;
        _other=true;
        _killchildren=false;
        ctrlC=0;
        timeout=1000;
    }

    bool isEmpty() const
    {
        return empty();
    }

    void kill(ProcessPid &pid,DWORD mysession)
    {
        if (_killchildren) {
            HANDLE hProcessSnap = hProcessSnap = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0); 
            if (hProcessSnap != INVALID_HANDLE_VALUE) {
                PROCESSENTRY32 pe32;
                memset(&pe32,0,sizeof(pe32));
                pe32.dwSize = sizeof(PROCESSENTRY32); 
                if (Process32First(hProcessSnap, &pe32)) { 
                    do {
                        if (pe32.th32ParentProcessID==pid.GetPid()) {
                            ProcessPid childpid(pe32.th32ProcessID,access);
                            printf("child: ");
                            kill(childpid,mysession);
                        }
                    } while (Process32Next(hProcessSnap, &pe32)); 
                }
                CloseHandle (hProcessSnap); 
            }
            else 
                printf("Could not take process snapshot, no children killed\n");
        }

        printf("%d - ",pid.GetPid());

        DWORD exit=0;
        if(::GetExitCodeProcess(pid,&exit) && exit!=STILL_ACTIVE)
        {
            printf("exited with code %d\n",exit);
        }
        else
        { 
            if(pid.GetSession()!=mysession)
            {
                printf("different session (%d) - ",pid.GetSession());
                if(!_other)
                {
                    printf(" skipped\n");
                    return;
                }
            }
            if(Process(pid.GetPid(),PROCESS_TERMINATE).Terminate())
            {
                printf("terminated\n");
            }
            else
            {
                Error::perror();
            }
        }
    }

    void kill()
    {
        Param1 param(ctrlC);
        DWORD session=-1;
        ::ProcessIdToSessionId(::GetCurrentProcessId(),&session);
        bool empty=true;

        for(iterator ik=begin();ik!=end();ik++)
        {
            if(session==ik->GetSession())
            {
                RunRemote(*ik,ExitProc,4096,&param,sizeof(param));
                empty=false;
            }
        }

        if(!empty)
        {
            wait(timeout+ctrlC);
        }

        for(iterator it=begin();it!=end();it++)
        {
            kill(*it,session);
        }
        
    }

    void superuser()
    {
        ProcessToken token(TOKEN_ADJUST_PRIVILEGES);
        if(!token.AdjustPrivilege(SE_DEBUG_NAME,true))
            Error::perror();
    }

    void restart(const char* _path)
    {
        _restart=true;
        if(_path && *_path)
        {
            char path[MAX_PATH];
            _fullpath(path,_path,sizeof(path));
            startupDir.resize(mbstowcs(0,path,0)+1);
            mbstowcs(&startupDir.begin()[0],path,strlen(path));
        }
    }

    void setTimeout(unsigned msecs)
    {
        timeout=msecs;
    }

    void setCtrlC(unsigned msecs)
    {
        ctrlC=msecs;
    }

    void setOther(bool o)
    {
        _other=o;
    }

    void setKillChildren(bool o)
    {
        _killchildren=o;
    }

protected:
    void wait(unsigned msecs)
    {
        HANDLE handles[MAXIMUM_WAIT_OBJECTS];
        unsigned count=0;
        for(iterator ih=begin();ih!=end();ih++)
        {
            handles[count++]=*ih;
            if(count>=MAXIMUM_WAIT_OBJECTS)
            {
                if(::WaitForMultipleObjects(count,handles,TRUE,msecs)==WAIT_FAILED)
                    Error::perror();
                count=0;
            }
        }
        if(count)
           if(::WaitForMultipleObjects(count,handles,TRUE,msecs)==WAIT_FAILED)
           {
               Error::perror();
           }

    }


    void RunRemote(HANDLE h, void* func,unsigned fsize,void* param, unsigned psize)
    {
        Process proc(h,PROCESS_CREATE_THREAD|PROCESS_VM_OPERATION|PROCESS_QUERY_INFORMATION|PROCESS_VM_READ|PROCESS_VM_WRITE);
        RemoteMemoryAsync code(proc,func,fsize,PAGE_EXECUTE_READWRITE);
        RemoteMemoryAsync data(proc,param,psize,PAGE_READWRITE);
        if(!code || !data || !RemoteThread(proc, reinterpret_cast<PTHREAD_START_ROUTINE>((void*)code), data, 0, 4096))
        {
            Error::perror();
        }
    }
 
    bool _restart;
    bool _other;
    bool _killchildren;
    wstring startupDir;
    unsigned timeout,ctrlC;
};

void usage()
{
    printf("pskill pid - kills a pid\n" 
           "pskill [-sr] [-ttimeout] [-ctimeout] filename1 filename2 ... - kills instances of named processes\n"
           "    -t      Time in seconds to wait for the process to exit, default is 2 seconds.\n"
           "    -c      Send Ctr-C signal first, the process has <timeout> seconds to exit.\n"
           "    -s      Enable SE_DEBUG privilege so the system processes can be killed.\n"
           "    -o      Terminate processes from the same terminal session only.\n"
           "    -a      Terminate child processes also.\n"
           "    -r[dir] Restart the process with the new working directory specified.\n");
    exit(2);
}

int main(int argc, char** argv)
{
    try
    {
        ProcessKillList procs;
        for(int i=1;i<argc;i++)
        {
            if(argv[i][0]=='-' || argv[i][0]=='/')
            {
                const char* arg=argv[i]+1;
                switch(tolower(*arg))
                {
                case 's':
                    procs.superuser();
                    break;

                case 'r':
                    procs.restart(arg+1);
                    break;

                case 't':
                    procs.setTimeout(1000*atoi(arg+1));
                    break;

                case 'c':
                    procs.setCtrlC(1000*atoi(arg+1));
                    break;

                case 'o':
                    procs.setOther(false);
                    break;

                case 'a':
                    procs.setKillChildren(true);
                    break;

                default:
                    usage();
                }
            }
            else
            {
                const char* arg=argv[i];
                if(atoi(arg))
                {
                    printf("Killing process %s\n", arg);
                    procs.add(atoi(arg));
                }
                else
                {
                    char imagepath[MAX_PATH];
                    if(::SearchPath(NULL,arg,".exe",sizeof(imagepath),imagepath,NULL))
                    {
                        printf("Killing processes matching %s\n", imagepath);
                        procs.add(imagepath);
                    }
                    else
                    {
                        printf("Can not find %s\n", arg);
                    }
                }
            }
        }
        if(!procs.isEmpty())
            procs.kill();

    }   
    catch(Error& e)
    {
        printf(e.GetMessage());
    }
    return 0;
}
