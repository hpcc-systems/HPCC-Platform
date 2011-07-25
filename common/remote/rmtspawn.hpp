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

#ifndef RMTSPAWN_HPP
#define RMTSPAWN_HPP

#ifdef REMOTE_EXPORTS
#define REMOTE_API __declspec(dllexport)
#else
#define REMOTE_API __declspec(dllimport)
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
    bool processCommandLine(int argc, char * argv[], StringBuffer &logdir);
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

    void run(int argc, char * argv[]);
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
