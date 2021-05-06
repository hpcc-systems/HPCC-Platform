/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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


#ifndef RMTCLIENT_IMPL_HPP
#define RMTCLIENT_IMPL_HPP

#include "rmtclient.hpp"

#ifdef _DEBUG
//#define SIMULATE_PACKETLOSS 1
#endif

#if SIMULATE_PACKETLOSS

#define TESTING_FAILURE_RATE_LOST_SEND  10 // per 1000
#define TESTING_FAILURE_RATE_LOST_RECV  10 // per 1000
#define DUMMY_TIMEOUT_MAX (1000*10)


struct DAFSCLIENT_API dummyReadWrite
{
    static ISocket *timeoutreadsock = NULL; // used to trigger
    class X
    {
        dummyReadWrite *parent;
    public:
        X(dummyReadWrite *_parent)
        {
            parent = _parent;
        }
        ~X()
        {
            delete parent;
        }
    };

    class TimeoutSocketException: public CInterface, public IJSOCK_Exception
    {
    public:
        IMPLEMENT_IINTERFACE;

        TimeoutSocketException()
        {
        }

        virtual ~TimeoutSocketException()
        {
        }

        int errorCode() const { return JSOCKERR_timeout_expired; }
        StringBuffer &  errorMessage(StringBuffer &str) const
        {
            return str.append("timeout expired");
        }
        MessageAudience errorAudience() const
        {
            return MSGAUD_user;
        }
    };


    ISocket *sock;

    dummyReadWrite(ISocket *_sock)
    {
        sock = _sock;
    }

    void readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, time_t timeout)
    {
        X x(this);
        unsigned t = msTick();
        unsigned r = getRandom();
        bool timeoutread = (timeoutreadsock==sock);
        timeoutreadsock=NULL;
        if (!timeoutread)
            sock->readtms(buf, min_size, max_size, size_read, timeout);
        if (timeoutread||((TESTING_FAILURE_RATE_LOST_RECV>0)&&(r%1000<TESTING_FAILURE_RATE_LOST_RECV))) {
            PrintStackReport();
            if (timeoutread)
                PROGLOG("** Simulate timeout");
            else
                PROGLOG("** Simulate Packet loss (size %d,%d)",min_size,max_size);
            if (timeout>DUMMY_TIMEOUT_MAX)
                timeout = DUMMY_TIMEOUT_MAX;
            t = msTick()-t;
            if (t<timeout)
                Sleep(timeout-t);
            IJSOCK_Exception *e = new TimeoutSocketException;
            throw e;
        }
    }
    size32_t write(void const* buf, size32_t size)
    {
        X x(this);
        timeoutreadsock=NULL;
        unsigned r = getRandom();
        if ((TESTING_FAILURE_RATE_LOST_SEND>0)&&(r%1000<TESTING_FAILURE_RATE_LOST_SEND)) {
            PrintStackReport();
            PROGLOG("** Simulate Packet loss (size %d)",size);
            timeoutreadsock=sock;
            return size;
        }
        return sock->write(buf,size);
    }
};

#define SOCKWRITE(sock) (new dummyReadWrite(sock))->write
#define SOCKREADTMS(sock) (new dummyReadWrite(sock))->readtms
#else
#define SOCKWRITE(sock) sock->write
#define SOCKREADTMS(sock) sock->readtms
#endif

#ifdef SIMULATE_PACKETLOSS
#define NORMAL_RETRIES      (1)
#define LENGTHY_RETRIES     (1)
#else
#define NORMAL_RETRIES      (3)
#define LENGTHY_RETRIES     (12)
#endif
extern DAFSCLIENT_API void sendDaFsBuffer(ISocket * socket, MemoryBuffer & src, bool testSocketFlag=false);
extern DAFSCLIENT_API size32_t receiveDaFsBufferSize(ISocket * socket, unsigned numtries=NORMAL_RETRIES,CTimeMon *timemon=NULL);
extern DAFSCLIENT_API void receiveDaFsBuffer(ISocket * socket, MemoryBuffer & tgt, unsigned numtries=1, size32_t maxsz=0x7fffffff);
extern DAFSCLIENT_API void cleanupDaFsSocket(ISocket *sock);
extern DAFSCLIENT_API byte traceFlags;
#define TF_TRACE (traceFlags&1)
#define TF_TRACE_PRE_IO (traceFlags&2)
#define TF_TRACE_FULL (traceFlags&4)
#define TF_TRACE_CLIENT_CONN (traceFlags&8)
#define TF_TRACE_TREE_COPY (traceFlags&0x10)
#define TF_TRACE_CLIENT_STATS (traceFlags&0x20)


class CRemoteBase : public CSimpleInterfaceOf<IDaFsConnection>
{
    Owned<ISocket>          socket;
    static SocketEndpoint   lastfailep;
    static unsigned         lastfailtime;
    static CriticalSection  lastFailEpCrit;
    DAFSConnectCfg          connectMethod;

    void connectSocket(SocketEndpoint &ep, unsigned connectTimeoutMs=0, unsigned connectRetries=INFINITE);
    void killSocket(SocketEndpoint &tep);

protected: friend class CRemoteFileIO;

    StringAttr          filename;
    CriticalSection     crit;
    SocketEndpoint      ep;

    void sendRemoteCommand(MemoryBuffer & src, MemoryBuffer & reply, bool retry=true, bool lengthy=false, bool handleErrCode=true);
    void sendRemoteCommand(MemoryBuffer & src, bool retry);
public:
    CRemoteBase(const SocketEndpoint &_ep, const char * _filename);
    CRemoteBase(const SocketEndpoint &_ep, DAFSConnectCfg _connectMethod, const char * _filename);
    void disconnect();
    const char *queryLocalName()
    {
        return filename;
    }
// IDaFsConnection impl.
    virtual void close(int handle) override;
    virtual void send(MemoryBuffer &sendMb, MemoryBuffer &reply) override;
    virtual unsigned getVersion(StringBuffer &ver) override;
    virtual const SocketEndpoint &queryEp() const override;
};

typedef enum { ACScontinue, ACSdone, ACSerror} AsyncCommandStatus;


extern void clientSetDaliServixSocketCaching(bool set);
extern void clientDisconnectRemoteFile(IFile *file);
extern void clientDisconnectRemoteIoOnExit(IFileIO *fileio,bool set);

extern bool clientResetFilename(IFile *file, const char *newname); // returns false if not remote

extern bool clientAsyncCopyFileSection(const char *uuid,    // from genUUID - must be same for subsequent calls
                        IFile *from,                        // expected to be remote
                        RemoteFilename &to,
                        offset_t toofs,                     // (offset_t)-1 created file and copies to start
                        offset_t fromofs,
                        offset_t size,                      // (offset_t)-1 for all file
                        ICopyFileProgress *progress,
                        unsigned timeout,                   // 0 to start, non-zero to wait
                        CFflags copyFlags
                        ); // returns true when done

extern void clientSetRemoteFileTimeouts(unsigned maxconnecttime, unsigned maxreadtime);
extern void clientAddSocketToCache(SocketEndpoint &ep, ISocket *socket);



#endif //
