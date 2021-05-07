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

#ifndef RMTSPAWN_HPP
#define RMTSPAWN_HPP

#ifdef REMOTE_EXPORTS
#define REMOTE_API DECL_EXPORT
#else
#define REMOTE_API DECL_IMPORT
#endif

#define SLAVE_LISTEN_FOR_MASTER_TIMEOUT             5 * 60 * 1000           // 2 minutes
#define MASTER_CONNECT_SLAVE_TIMEOUT                5 * 60 * 1000

#define RMTTIME_CLOSEDOWN                           1000 * 1000         // How long slave to wait for acknowledge of closedown notification
#define RMTTIME_CONNECT_SLAVE                       1000 * 1000         // How long to wait for connection from slave
#define RMTTIME_RESPONSE_MASTER                     10 * 60 * 1000      // How long to wait for a response from master - should be immediate!

enum SpawnKind
{
    SPAWNdfu, SPAWNlast
};

interface IAbortRequestCallback;

extern REMOTE_API ISocket * spawnRemoteChild(SpawnKind kind, const char * exe, const SocketEndpoint & remoteEP, unsigned version, const char *logdir, IAbortRequestCallback * abort = NULL, const char *extra=NULL);
extern REMOTE_API void setRemoteSpawnSSH(
                const char *identfilename,
                const char *username, // if NULL then disable SSH
                const char *passwordenc,
                unsigned timeout,
                unsigned retries,
                const char *exeprefix);
extern REMOTE_API void getRemoteSpawnSSH(
                StringAttr &identfilename,
                StringAttr &username, // if isEmpty then disable SSH
                StringAttr &passwordenc,
                unsigned &timeout,
                unsigned &retries,
                StringAttr &exeprefix);

class REMOTE_API CRemoteParentInfo
{
public:
    CRemoteParentInfo();

    ISocket * queryMasterSocket()               { return socket; }
    bool processCommandLine(int argc, const char * * argv, StringBuffer &logdir);
    void log();
    bool sendReply(unsigned version);

public:
    Owned<ISocket>      socket;
    SocketEndpoint      parent;
    unsigned            replyTag;
    unsigned            port;
    SpawnKind           kind;
};


class REMOTE_API CRemoteSlave
{
public:
    CRemoteSlave(const char * name, unsigned _tag, unsigned _version, bool _stayAlive);

    void run(int argc, const char * * argv);
    virtual bool processCommand(byte action, ISocket * masterSocket, MemoryBuffer & msg, MemoryBuffer & results) = 0;

protected:
    void stopRunning()              { stayAlive = false; }

protected:
    StringAttr      slaveName;
    unsigned        tag;
    bool            stayAlive;
    unsigned        version;
};


//extern REMOTE_API void checkForRemoteAbort(ICommunicator * communicator, mptag_t tag);
//extern REMOTE_API void sendRemoteAbort(INode * node, mptag_t tag);
extern REMOTE_API void checkForRemoteAbort(ISocket * socket);
extern REMOTE_API bool sendRemoteAbort(ISocket * socket);


#endif
