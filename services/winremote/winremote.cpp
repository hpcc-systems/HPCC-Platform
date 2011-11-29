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

#ifdef _WIN32 // Just to shut up any code-checkers on linux side

#pragma warning(disable:4786)

#include "win32.hpp"
#include "winprocess.hpp"

#include "winremote.hpp"
#include "jiface.hpp"
#include "jstring.hpp"
#include "jexcept.hpp"
#include "jiter.ipp"

#include "jpqueue.hpp"

#include "service/svcmsg.hpp"
#include "psexec.rh"

#include <functional>
#include <algorithm>
#include <vector>

#include "Lm.h" 
using namespace win32;

class StdOutputStream: public CInterface, implements IRemoteOutput
{
public:
    IMPLEMENT_IINTERFACE;
    virtual void write(const char* ip,const char* data) 
    {
        StringBuffer buf(ip);
        buf.append(" > ");
        buf.append(data);
        
        fputs(buf.str(),stdout);
    }
} stdOut;

extern "C" WINREMOTE_API IRemoteOutput* queryStdout()
{
    return &stdOut;
}


class StdErrorStream: public CInterface, implements IRemoteOutput
{
public:
    IMPLEMENT_IINTERFACE;
    virtual void write(const char* ip,const char* data) 
    {
        StringBuffer buf(ip);
        buf.append(" ! ");
        buf.append(data);
        
        fputs(buf.str(),stderr);
    }
} stdErr;

extern "C" WINREMOTE_API IRemoteOutput* queryStderr()
{
    return &stdErr;
}

class ServiceTask: public CInterface, implements ITask
{
public:
    IMPLEMENT_IINTERFACE;
    ServiceTask(const char* _remote,const char* _user, const char* _pwd): 
        remote(_remote), user(_user), pwd(_pwd), stopped(false)
    {
    }

    int run()
    {
        try
        {
            checkStopped();
            SharePoint ipc(remote,"IPC$",user,pwd);
            checkStopped();
            SharePoint admin(remote,"ADMIN$",user,pwd);

            remoteConnected();

            StringBuffer pipename;
            pipename.appendf("\\\\%s\\Pipe\\"PSEXECPIPE,remote);
            File pipe;

            for(int retry=5;;retry--)
            {
                checkStopped();
                if(retry<=0)
                    throw Error("Unable to connect/start service");

                if(!::WaitNamedPipe(pipename.str(),500))
                {
                    DWORD err=::GetLastError();
                    if(err==ERROR_FILE_NOT_FOUND)
                    {
                        if(!CopySvcExeToRemoteMachine() || !InstallAndStartRemoteService())
                        {
                            if(!stopped)
                                ::Sleep(500);
                            continue;
                        }
                    }
                    else
                        throw SystemError("Wait pipe failed");
                }

                if(pipe.Open(pipename.str(),GENERIC_READ|GENERIC_WRITE,0,OPEN_EXISTING,FILE_FLAG_WRITE_THROUGH))
                {
                    pipe.SetPipeMode(PIPE_READMODE_MESSAGE);
                    break;
                }
                else if(::GetLastError()!=ERROR_FILE_NOT_FOUND)
                    throw SystemError("Open pipe failed");
            }
            serviceConnected(pipe);
        }
        catch(const Error& e)
        {
            reportError(e.GetCode(),e.GetMessage());
        }
        return 0;
    }

    bool stop()
    {
        stopped=true;
        return true;
    }

    void checkStopped()
    {
        if(stopped)
            throw Error("Cancelled");
    }

protected:

    virtual void remoteConnected() {}
    virtual void serviceConnected(File& pipe) {}
    virtual void reportError(int code,const char* error) {}

    bool CopySvcExeToRemoteMachine()
    {
        HMODULE hInstance = ::GetModuleHandle("winremote.dll"); 

        // Find the binary file in resources
        HRSRC hSvcExecutableRes = ::FindResource(hInstance, MAKEINTRESOURCE(IDR_PSEXEC),RT_RCDATA);

        HGLOBAL hSvcExecutable = ::LoadResource(hInstance, hSvcExecutableRes);

        LPVOID pSvcExecutable = ::LockResource(hSvcExecutable);

        if (!pSvcExecutable)
            throw win32::SystemError("Could not find service executable");

        DWORD executableSize = ::SizeofResource(hInstance, hSvcExecutableRes);

        CHAR targetpath[_MAX_PATH];
        _snprintf(targetpath,arraysize(targetpath),"\\\\%s\\ADMIN$\\System32\\%s", remote, PSEXECSVCEXE);
        targetpath[_MAX_PATH-1]=0;

        // Copy binary file from resources to \\remote\ADMIN$\System32
        File svc(targetpath, GENERIC_WRITE, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL|FILE_FLAG_WRITE_THROUGH);
        if(!svc)
            switch(::GetLastError())
            {
                case ERROR_SHARING_VIOLATION:
                {
//                    NetSessionDel(server,client,NULL);
                    return false;
                }
                default:
                    throw win32::SystemError("Could not open service executable");
            }

        checkStopped();

        DWORD bytes=svc.Write(pSvcExecutable, executableSize);
        if(bytes!=executableSize)
            throw win32::SystemError("Could not copy service executable %d",bytes);
        ::FlushFileBuffers(svc);
        return true;
    }


    struct ScmHandle
    {
        ScmHandle(SC_HANDLE h): handle(h) {}
        ~ScmHandle() { ::CloseServiceHandle(handle); }

        operator SC_HANDLE() { return handle; }

        SC_HANDLE handle;
    };
    
    struct TempService: public ScmHandle
    {
        TempService(SC_HANDLE h): ScmHandle(h) {}
        ~TempService() { ::DeleteService(handle); }

        SC_HANDLE operator = (SC_HANDLE h)
        {
            if(handle)
                ::CloseServiceHandle(handle);
            return handle=h;
        }

    };

    bool InstallAndStartRemoteService()
    {
        checkStopped();
        ScmHandle scm(::OpenSCManager(remote, NULL, SC_MANAGER_ALL_ACCESS));

        if(!scm)
            throw win32::SystemError("Could not open remote SCM %s",remote);

        checkStopped();
        // Maybe it's already there and installed, let's try to run
        TempService svc(::OpenService(scm, SERVICENAME, SERVICE_ALL_ACCESS));

        if(!svc && !(svc=::CreateService(scm, SERVICENAME, SERVICEDISPLAYNAME, SERVICE_ALL_ACCESS, SERVICE_WIN32_OWN_PROCESS,
                                    SERVICE_DEMAND_START, SERVICE_ERROR_NORMAL, TEXT("%SystemRoot%\\system32\\")PSEXECSVCEXE, NULL, NULL, NULL, NULL, NULL)))
        {
            throw win32::SystemError("Could not create remote service %s",remote);
        }

        for(;;)
        {
            checkStopped();
            SERVICE_STATUS status;
            if(!::QueryServiceStatus(svc,&status))
                throw win32::SystemError("Could not query service status");
            switch(status.dwCurrentState)
            {
            case SERVICE_RUNNING:
                return true;

            case SERVICE_STOPPED:
                if (!::StartService(svc, 0, NULL))
                {
                    SystemError e("Could not start remote service %s",remote);
                    throw win32::SystemError(e);
                }
            case SERVICE_START_PENDING:
                ::Sleep(100);
                continue;

            case SERVICE_STOP_PENDING:
                return false;

            default:
                throw win32::SystemError("Service state is %d",status.dwCurrentState);
            }

        }
        return false;
    }

    StringAttr  remote,user,pwd;
    volatile bool stopped;

    struct SharePoint
    {
        SharePoint(const char* _remote, const char* _share, const char * _user, const char * _pwd)
        {
            remote.appendf("\\\\%s\\%s",_remote,_share);

            NETRESOURCE nr;
            nr.dwType = RESOURCETYPE_ANY;
            nr.lpLocalName = NULL;
            nr.lpRemoteName = (char*)remote.str();
            nr.lpProvider = NULL;

            for(;;)
            {
                //Establish connection (using username/pwd)
                DWORD rc = ::WNetAddConnection2(&nr, _pwd, _user, FALSE);
                if(rc==NO_ERROR)
                    break;

                win32::SystemError e("Could not connect");

                if(rc==ERROR_SESSION_CREDENTIAL_CONFLICT && ::WNetCancelConnection2((char*)remote.str(), 0, FALSE)==NO_ERROR)
                    continue;

                throw e;
            }
        }

        ~SharePoint()
        {
            ::WNetCancelConnection2((char*)remote.str(), 0, FALSE);
        }

        StringBuffer remote;
    };
};

//typedef WaitQueue<Linked<ServiceTask> > ConnectQueueType;


struct RemoteCommand: public CInterface, implements IRemoteCommand
{
public:
    IMPLEMENT_IINTERFACE;
    RemoteCommand(): copyFile(0), priority(NORMAL_PRIORITY_CLASS), session(0), async(true), status(0), pid(0), network(false)
    {
    }

    virtual void setCommand(const char * _command)
    {
        command.set(_command);
    }

    virtual void setPriority(int _priority) 
    {
        priority=_priority;
    }
    
    virtual void setCopyFile(bool overwrite) 
    {
        copyFile= overwrite ? 2 : 1;
    }

    virtual void setWorkDirectory(const char * _dir) 
    {
        dir.set(_dir);
    }
    
    virtual void setSession(int _session) 
    {
        session=_session;
    }

    virtual void setOutputStream(IRemoteOutput * _out)
    {
        out.set(_out);
        async=false;
    }

    virtual void setErrorStream(IRemoteOutput * _err) 
    {
        err.set(_err);
    }

    virtual int getErrorStatus() const
    {
        return status;
    }
    
    virtual int getPid() const
    {
        return pid;
    }

    virtual void setNetworkLogon(bool set) 
    {
       network=set;
    }

    StringAttr command, dir;
    int priority;
    int copyFile;
    int session;
    bool async;
    bool network;
    Linked<IRemoteOutput> out,err;
    int status;
    int pid;
};

interface ITaskListener
{
    virtual void startIO(const char* machine,DWORD pid,HANDLE in,IRemoteOutput* out,IRemoteOutput* err)=0;
};


class ConnectTask: public ServiceTask
{
public:
    ConnectTask(const char* _remote,const char* _user, const char* _pwd,RemoteCommand* _cmd,ITaskListener* _tl): 
        ServiceTask(_remote, _user, _pwd), cmd(_cmd), tl(_tl)
    {
    }

protected:
    virtual void remoteConnected() 
    {
        if(cmd->copyFile)
            copy(cmd->command,cmd->copyFile>1);
    }

    virtual void serviceConnected(File& pipe)
    {
        CmdMessage cmdmsg("EXEC",cmd->network ? user : NULL,cmd->network ? pwd : NULL);
        if(pipe.Write(&cmdmsg,sizeof(cmdmsg))!=sizeof(cmdmsg))
            throw win32::SystemError("Could not talk to the server");

        ExecMessage msg(cmd->command,cmd->dir,cmd->async ? ExecMessage::NoWait : 0, cmd->priority,cmd->session);
        ExecResult ret;

        if(pipe.Transact(&msg,sizeof(msg),&ret,sizeof(ret))!=sizeof(ret))
            throw win32::SystemError("Could not talk to the server");

        if(ret.err!=ERROR_SUCCESS)
            throw win32::Error("Server returned error %d",ret.err);

        checkStopped();

        if(msg.IsNoWait())
        {
            cmd->pid=ret.pid;
        }
        else 
        {
            CHAR buf[MAX_PATH];
            
            File pipeout(cmdmsg.GetStdout(buf,arraysize(buf),remote),GENERIC_READ,0,OPEN_EXISTING,FILE_FLAG_OVERLAPPED);
            if(!pipeout)
                throw win32::SystemError("Could not connect to STDOUT");

            File pipeerr(cmdmsg.GetStderr(buf,arraysize(buf),remote),GENERIC_READ,0,OPEN_EXISTING,FILE_FLAG_OVERLAPPED);
            if(!pipeerr)
                throw win32::SystemError("Could not connect to STERR");

            File pipein(cmdmsg.GetStdin(buf,arraysize(buf),remote),GENERIC_WRITE,0,OPEN_EXISTING,FILE_FLAG_WRITE_THROUGH);
            if(!pipein)
                throw win32::SystemError("Could not connect to STDIN");

            DWORD status=ERROR_SUCCESS;
            if(pipe.Transact(&status,sizeof(status),&ret,sizeof(ret))!=sizeof(ret))
                throw win32::SystemError("Could not talk to the server");
            if(ret.err!=ERROR_SUCCESS)
                throw win32::SystemError("Server returned error %d",ret.err);

            cmd->pid=ret.pid;

            if(tl)
            {
                if(cmd->out)
                    tl->startIO(remote,ret.pid,pipeout.get(),cmd->out,cmd->err);
                if(cmd->err)
                    tl->startIO(remote,ret.pid,pipeerr.get(),cmd->err,cmd->err);
            }
        }
    }

    virtual void reportError(int code,const char* error) 
    {
        if(!cmd) return;
        
        cmd->status=code;

        if(cmd->err)
        {
            cmd->err->write(remote,error);
        }
    }

    void copy(const char* cmd,bool overwrite)
    {
#ifdef _UNICODE    
        LPWSTR * wcmd=cmd;
#else
        size32_t cmdsize=strlen(cmd)+1;
        LPWSTR wcmd=(LPWSTR)_alloca(sizeof(WCHAR)*cmdsize);
        mbstowcs(wcmd,cmd,cmdsize);
#endif

        WCHAR imagepath[_MAX_PATH]={0};

        int numargs=0;
        LPWSTR * args=::CommandLineToArgvW(wcmd,&numargs);
        if(args)
        {
            if(numargs>0)
            {
                ::SearchPathW(NULL,args[0],L".exe",arraysize(imagepath),imagepath,NULL);
            }
            ::GlobalFree(args);
        }

        if(!*imagepath)
            throw win32::SystemError("Could not find file %s",cmd);


        WCHAR drive[_MAX_DRIVE];
        WCHAR dir[_MAX_DIR];
        WCHAR fname[_MAX_FNAME];
        WCHAR ext[_MAX_EXT];

        // Gets the file name and extension
        _wsplitpath(imagepath, drive, dir, fname, ext);

        WCHAR targetpath[_MAX_PATH];
        _snwprintf(targetpath,arraysize(targetpath),L"\\\\%S\\ADMIN$\\System32\\%s%s", remote, fname, ext);
        targetpath[_MAX_PATH-1]=0;

        // Copy the Command's exe file to \\remote\ADMIN$\System32
        if(!::CopyFileW(imagepath, targetpath, overwrite ? FALSE:TRUE) && ::GetLastError()!=ERROR_FILE_EXISTS)
            throw win32::SystemError("Could not copy file %S to %S",imagepath,targetpath);
    }

private:
    Linked<RemoteCommand> cmd;
    ITaskListener* tl;
};


interface ITaskOutput
{
    virtual void write(const char* machine,const char* data,size32_t len)=0;
};


class OutputTask: public CInterface, implements ITask
{
public:
    IMPLEMENT_IINTERFACE;

    OutputTask(const char* _remote,HANDLE _input,IRemoteOutput* _output,IRemoteOutput* _err): 
        remote(_remote), output(_output), err(_err)
    {
        input.set(_input);
    }

    int run()
    {
        thread=Handle(::GetCurrentThread(),THREAD_ALL_ACCESS).get();
        try
        {
            StringBuffer sb;
            char buf[4096];

            for(;input;)
            {
                OVERLAPPED ovl={0};
                if(!input.Read(buf,sizeof(buf),&ovl))
                {
                    switch(::GetLastError())
                    {
                    case ERROR_IO_PENDING:
                        break;

                    case ERROR_HANDLE_EOF:
                    case ERROR_BROKEN_PIPE:
                    case ERROR_OPERATION_ABORTED:
                        input.Close();
                        continue;

                    default:
                        throw win32::SystemError("Read pipe");

                    }
                }

                DWORD ret=input.Wait(INFINITE,true);
                switch(ret)
                {
                case WAIT_OBJECT_0:
                    {
                        size32_t count=input.GetResult(&ovl,false);
                        if(count)
                        {
                            char *b;
                            char *endl;
                            for(b=buf;endl=(char*)memchr(b,'\n',count-(b-buf));b=endl+1)
                            {

                                size32_t sz=endl-b+1;
                                print("%s%.*s",sb.str(),sz,b);
                                sb.clear();
                            }

                            sb.append(count-(b-buf),b);
                        }
                        else
                        {
                            switch(::GetLastError())
                            {
                                case ERROR_IO_PENDING:
                                    continue;

                                case ERROR_HANDLE_EOF:
                                case ERROR_BROKEN_PIPE:
                                case ERROR_OPERATION_ABORTED:
                                    input.Close();
                                    continue;

                                default:
                                    throw win32::SystemError("Read pipe");
                            }
                        }
                        break;
                    }

                case WAIT_IO_COMPLETION:
                    input.Close();
                    continue;

                default:
                    throw win32::SystemError("Wait");
                }
            }
            if(sb.length())
                print("%s\n",sb.str());
        }
        catch(win32::Error& e)
        {
            if(err)
                err->write(remote,e.GetMessage());
        }
        catch(...)
        {
            if(err)
                err->write(remote,"Unknown Error");
        }
        return 0;
    }

    bool stop()
    {
        return ::QueueUserAPC(StopProc,thread,(ULONG_PTR)(HANDLE)input)!=0;
    }

protected:

    void print(const char* format,...) __attribute__((format(printf, 2, 3)))
    {
        if(!output)
            return;

        StringBuffer buf;
        va_list marker;
        va_start(marker,format);
        buf.valist_appendf(format,marker);
        va_end(marker);
        output->write(remote,buf.str());
    }

    StringAttr remote;
    File input;
    Linked<IRemoteOutput> output,err;
    HANDLE thread;

    static VOID CALLBACK StopProc(ULONG_PTR param)
    {
        HANDLE h=(HANDLE)param;
        ::CancelIo(h);
        ::CloseHandle(h);
    }
};


interface IProcessList
{
    virtual void add(const char* machine,int pid,int parent,int session,const wchar_t* app,const wchar_t* cmd,const wchar_t* dir)=0;
};


class ListTask: public ServiceTask
{
public:
    ListTask(const char* _remote,const char* _user, const char* _pwd,const char* _path,IProcessList* _pl,IRemoteOutput* _err): 
        ServiceTask(_remote, _user, _pwd), path(_path), pl(_pl), err(_err)
    {
    }

    virtual void serviceConnected(File& pipe)
    {
        CmdMessage cmdmsg("LIST",user,pwd);
        if(pipe.Write(&cmdmsg,sizeof(cmdmsg))!=sizeof(cmdmsg))
            throw win32::SystemError("Could not talk to the server");

        ListMessage msg(path);
        if(pipe.Write(&msg,sizeof(msg))!=sizeof(msg))
            throw win32::SystemError("Could not talk to the server");

        for(;;)
        {
            ListResult ret;
            if(pipe.Read(&ret,sizeof(ret))==sizeof(ret))
            {
                if(pl)
                {
                    pl->add(remote,ret.GetPid(),ret.GetParent(),ret.GetSession(),ret.GetAppName(),ret.GetCommandLine(),ret.GetWorkingDir());
                }
            }
            else
            {
                break;
            }
        }
    }

    virtual void reportError(int code,const char* error) 
    {
        if(err)
            err->write(remote,error);
    }   


private:
    StringAttr path;
    IProcessList* pl;
    Linked<IRemoteOutput> err;
};

class CProcess: public CInterface, implements IRemoteProcess
{
public:
    IMPLEMENT_IINTERFACE;

    CProcess(const char* _machine,int _pid,int _parent,int _session,const wchar_t* _app,const wchar_t* _cmd,const wchar_t* _dir): 
        remote(_machine), pid(_pid), parent(_parent), session(_session)
    {
        wcstombs(app,_app,arraysize(app));
        wcstombs(cmd,_cmd,arraysize(cmd));
        wcstombs(dir,_dir,arraysize(dir));
    }


    virtual int getPid() const
    {
        return pid;
    }

    virtual int getParent() const
    {
        return parent;
    }

    virtual int getSession() const
    {
        return session;
    }

    virtual IStringVal& getMachine(IStringVal & val) const
    {
        val.set(remote);
        return val;
    }

    virtual IStringVal& getApplicationName(IStringVal& val) const
    {
        val.set(app);
        return val;
    }

    virtual IStringVal& getCommandLine(IStringVal& val) const
    {
        val.set(cmd);
        return val;
    }

    virtual IStringVal& getCurrentDirectory(IStringVal& val) const
    {
        val.set(dir);
        return val;
    }

private:
    StringAttr remote;
    int pid, parent, session;
    char app[MAX_PATH], cmd[MAX_COMMAND], dir[MAX_PATH];
};

class CProcessArray: public Array, implements IProcessList
{
    virtual void add(const char* machine,int pid,int parent,int session,const wchar_t* app,const wchar_t* cmd,const wchar_t* dir)
    {
        CProcess* proc=new CProcess(machine,pid,parent,session,app,cmd,dir);
        synchronized block(mutex);
        append(*proc);
    }
    Mutex mutex;
};

class CProcessIterator: public CArrayIteratorOf<IRemoteProcess,IRemoteProcessIterator>
{
public:
    CProcessIterator(CProcessArray *_values) : CArrayIteratorOf<IRemoteProcess,IRemoteProcessIterator>(*_values,0), values(_values) {}
    ~CProcessIterator()
    {
        delete values;
    }
    CProcessArray* values;
};

class RemoteAgent: public CInterface, implements IRemoteAgent, implements ITaskListener
{
public:
    IMPLEMENT_IINTERFACE;
    RemoteAgent(): connect(20), io(400)
    {
    }

    virtual void addMachine(const char * machine, const char * user, const char * pwd) 
    {
        machines.push_back(MachineInfo(machine,user,pwd));
    }

    virtual void startCommand(IRemoteCommand * _cmd)
    {
        RemoteCommand* cmd=dynamic_cast<RemoteCommand*>(_cmd);
        if(!cmd)
            return; //more

        for(Machines it=machines.begin();it!=machines.end();it++)
        {
            connect.put(new ConnectTask(it->machine,it->user,it->password,cmd,this));
        }
    }

    virtual IRemoteProcessIterator * getProcessList(const char * path, IRemoteOutput* _err)
    {
        TaskQueue list(20);
        
        CProcessArray* a=new CProcessArray();
        for(Machines it=machines.begin();it!=machines.end();it++)
        {
            list.put(new ListTask(it->machine,it->user,it->password,path,a,_err));
        }

        return new CProcessIterator(a);
    }

    virtual void stop()
    {
        connect.stop();
        io.stop();
    }

    virtual void wait() 
    {
        connect.join();   
        io.join();   
    }

    virtual void startIO(const char* machine,DWORD pid,HANDLE in,IRemoteOutput* out,IRemoteOutput* err)
    {
        io.put(new OutputTask(machine,in,out,err));
    }

    struct MachineInfo
    {
        MachineInfo(const char* _machine,const char* _user,const char* _password): 
            machine(_machine), user(_user), password(_password) {}
        StringAttr machine,user,password;
    };

    std::list<MachineInfo> machines;
    typedef std::list<MachineInfo>::iterator Machines;
    TaskQueue connect, io;
};


extern "C" WINREMOTE_API IRemoteCommand* createRemoteCommand()
{
    return new RemoteCommand();
}

extern "C" WINREMOTE_API IRemoteAgent* createRemoteAgent()
{
    return new RemoteAgent();
}




BOOL APIENTRY DllMain(HANDLE,DWORD,LPVOID)
{
    return TRUE;
}
#endif
