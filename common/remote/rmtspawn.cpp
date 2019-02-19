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

#include "jliball.hpp"

#include "platform.h"
#include "portlist.h"


#include "remoteerr.hpp"

#include "rmtspawn.hpp"
#include "rmtssh.hpp"
#include "rmtpass.hpp"



LogMsgCategory MCdetailDebugInfo(MCdebugInfo(1000));

/*
How the remote spawning works:

  i) the master starts a slave program using hoagent/ssh, passing a) who the master is and b) what reply tag to use
  ii) the slave starts up, and starts listening on a socket based on the reply tag.
  iii) the master connects to the socket, and is returned the ip/mpsocket that the slave is listening on.
  iv) The master connects to the slave on that mp channel.

  Complications:
    a) slave could fail to start
    b) slave/master could die at any point.
    c) more than one slave can be being started on the same socket/reply tag.

  Timeouts:
    master->slave socket connect 300 seconds + buffer read + delay * 20 attempts (assuming bad connect throws an exception)
    slave for master 5 minutes normally, max 5 mins * 20 * 20 attempts in weird cicumstances
        read buffer with no timeout - could it get stuck here?

  Q's
    What if always connect to an orphaned slave?


MORE: This could be improved.  Really there should be one thing connecting to the socket, that shares all the
      attempted connections.  That would solve the problem of connecting for the wrong slave.  However, since
      it is only a problem for running all the slaves on the same machine its probably not worth worrying about.
*/

static unsigned nextReplyTag;
static StringAttr SSHidentfilename;
static StringAttr SSHusername;
static StringAttr SSHpasswordenc;
static unsigned SSHtimeout;
static unsigned SSHretries;
static StringAttr SSHexeprefix;

void setRemoteSpawnSSH(
                const char *identfilename,
                const char *username, // if NULL then disable SSH
                const char *passwordenc,
                unsigned timeout,
                unsigned retries,
                const char *exeprefix)
{
    SSHidentfilename.set(identfilename);
    SSHusername.set(username);
    SSHpasswordenc.set(passwordenc);
    SSHtimeout = timeout;
    SSHretries = retries;
    SSHexeprefix.set(exeprefix);
}

void getRemoteSpawnSSH(
                StringAttr &identfilename,
                StringAttr &username, // if isEmpty then disable SSH
                StringAttr &passwordenc,
                unsigned &timeout,
                unsigned &retries,
                StringAttr &exeprefix)
{
    identfilename.set(SSHidentfilename);
    username.set(SSHusername);
    passwordenc.set(SSHpasswordenc);
    timeout = SSHtimeout;
    retries = SSHretries;
    exeprefix.set(SSHexeprefix);
}


ISocket * spawnRemoteChild(SpawnKind kind, const char * exe, const SocketEndpoint & childEP, unsigned version, const char *logdir, IAbortRequestCallback * abort, const char *extra)
{
    SocketEndpoint myEP;
    myEP.setLocalHost(0);
    unsigned replyTag = ++nextReplyTag;
    unsigned port = SLAVE_CONNECT_PORT + ((unsigned)kind * NUM_SLAVE_CONNECT_PORT) + getRandom() % NUM_SLAVE_CONNECT_PORT;
    StringBuffer args;

    myEP.getUrlStr(args);
    args.append(' ').append(replyTag).append(' ').append((unsigned)kind).append(" ").append(port);
    if (extra)
        args.append(' ').append(extra);
    else
        args.append(" _");
    if (logdir)
        args.append(' ').append(logdir);


    StringBuffer cmd;
    if (SSHexeprefix.isEmpty())
        cmd.append(exe);
    else {
        const char * tail = splitDirTail(exe,cmd);
        size32_t l = strlen(tail);
        addPathSepChar(cmd).append(SSHexeprefix);
        if ((l>4)&&(memcmp(tail+l-4,".exe",4)==0))  // bit odd but want .bat if prefix on windows
            cmd.append(l-4,tail).append(".bat");
        else
            cmd.append(tail);
    }
    cmd.append(' ').append(args);

    if (abort && abort->abortRequested())
    {
        LOG(MCdetailDebugInfo, unknownJob, "Action aborted before connecting to slave (%3d)", replyTag);
        return NULL;
    }

    if (SSHusername.isEmpty())
    {
#if defined(_WIN32)
        //Run the program directly if it is being run on the local machine - so ssh doesn't need to be running...
        //Change once we have solved the problems with ssh etc. on windows?
        if (childEP.isLocal())
        {
            DWORD runcode;
            if (!invoke_program(cmd.str(), runcode, false))
                return NULL;
        }
        else
#endif

            throw MakeStringException(-1,"SSH user not specified");
    }
    else {
        Owned<IFRunSSH> runssh = createFRunSSH();
        runssh->init(cmd.str(),SSHidentfilename,SSHusername,SSHpasswordenc,SSHtimeout,SSHretries);
        runssh->exec(childEP,NULL,true); // need workdir? TBD
    }

    //Have to now try and connect to the child and get back the port it is listening on
    unsigned attempts = 20;
    SocketEndpoint connectEP(childEP);
    connectEP.port = port;
    LOG(MCdetailDebugInfo, unknownJob, "Start connect to correct slave (%3d)", replyTag);
    IException * error = NULL;
    ISocket * result = NULL;
    while (!result && attempts)
    {
        try
        {
            StringBuffer tmp;
            connectEP.getUrlStr(tmp);
            LOG(MCdetailDebugInfo, unknownJob, "Try to connect to slave %s",tmp.str());
            Owned<ISocket> socket = ISocket::connect_wait(connectEP,MASTER_CONNECT_SLAVE_TIMEOUT);
            if (socket)
            {
                try
                {
                    MemoryBuffer buffer;
                    buffer.setEndian(__BIG_ENDIAN);
                    buffer.append(version);
                    myEP.ipserialize(buffer);
                    buffer.append((unsigned)kind);
                    buffer.append(replyTag);
                    writeBuffer(socket, buffer);

                    bool connected;
                    unsigned slaveTag;
                    readBuffer(socket, buffer.clear(), 100*1000);
                    buffer.read(connected);
                    buffer.read(slaveTag);
                    SocketEndpoint childEP;
                    childEP.deserialize(buffer);
                    if (connected)
                    {
                        assertex(slaveTag == replyTag);
                        LOG(MCdetailDebugInfo, unknownJob, "Connected to correct slave (%3d)", replyTag);
                        result = socket.getClear();
                        break;
                    }
                    unsigned slaveVersion = 5;
                    unsigned slaveKind = kind;
                    if (buffer.getPos() < buffer.length())
                        buffer.read(slaveVersion);
                    if (buffer.getPos() < buffer.length())
                        buffer.read(slaveKind);
                    if ((slaveVersion != version) && (slaveKind == kind))
                    {
                        error = MakeStringException(RFSERR_VersionMismatch, RFSERR_VersionMismatch_Text, version, slaveVersion);
                        break;
                    }
                    if (slaveKind != kind)
                        LOG(MCdetailDebugInfo, unknownJob, "Connected to wrong kind of slave (%d,%d/%d) - try again later",connected,replyTag,slaveTag);
                    else
                        LOG(MCdetailDebugInfo, unknownJob, "Failed to connect to correct slave (%d,%d/%d) - try again later",connected,replyTag,slaveTag);

                    //Wrong slave listening, need to leave time for the other, don't count as an attempt
                    MilliSleep(fastRand() % 5000 + 5000);
                }
                catch (IException * e)
                {
                    StringBuffer s;
                    s.appendf("Retry after exception talking to slave (%d): ",replyTag);
                    e->errorMessage(s);
                    LOG(MCdetailDebugInfo, unknownJob, "%s", s.str());
                    e->Release();
                    //Probably another element just connected, and the listening socket has just been killed.
                    //So try again.  Wait just long enough to give another thread a chance.
                    MilliSleep(10);
                }
            }
        }
        catch (IException * e)
        {
            StringBuffer s;
            LOG(MCdetailDebugInfo, unknownJob, e, s.appendf("Failed to connect to slave (%d) (try again): ", replyTag).str());
            e->Release();
            // No socket listening or contention - try again fairly soon
            MilliSleep(fastRand()%400+100);
            attempts--;
        }

        if (abort && abort->abortRequested())
            break;
    }
    if (error)
        throw error;
    if (!result)
        ERRLOG("Failed to connect to slave (%d)", replyTag);
    return result;
}


//---------------------------------------------------------------------------

CRemoteParentInfo::CRemoteParentInfo()
{
    replyTag = 0;
    kind = SPAWNlast;
    port = 0;
}


bool CRemoteParentInfo::processCommandLine(int argc, char * argv[], StringBuffer &logdir)
{
    if (argc <= 4)
        return false;

    parent.set(argv[1]);
    replyTag = atoi(argv[2]);
    kind = (SpawnKind)atoi(argv[3]);
    port = atoi(argv[4]);
    // 5 is extra (only used in logging)
    if (argc>6)
        logdir.clear().append(argv[6]);

    return true;
}

void CRemoteParentInfo::log()
{
    StringBuffer temp;
    LOG(MCdebugProgress, unknownJob, "Starting remote slave.  Master=%s reply=%d port=%d", parent.getUrlStr(temp).str(), replyTag, port);
}

bool CRemoteParentInfo::sendReply(unsigned version)
{
    unsigned listenAttempts = 20;
    MemoryBuffer buffer;
    buffer.setEndian(__BIG_ENDIAN);
    while (listenAttempts--)
    {
        try
        {
            LOG(MCdebugInfo(1000), unknownJob, "Ready to listen. reply=%d port=%d", replyTag, port);
            Owned<ISocket> listen = ISocket::create(port, 1);
            if (listen)
            {
                unsigned receiveAttempts = 10;
                unsigned connectVersion;
                unsigned connectTag;
                unsigned connectKind;
                StringBuffer masterIPtext;
                while (receiveAttempts--)
                {
                    try
                    {
                        LOG(MCdebugInfo(1000), unknownJob, "Ready to accept connection. reply=%d", replyTag);

                        if (!listen->wait_read(SLAVE_LISTEN_FOR_MASTER_TIMEOUT))
                        {
                            LOG(MCdebugInfo(1000), unknownJob, "Gave up waiting for a connection. reply=%d", replyTag);
                            return false;
                        }

                        Owned<ISocket> connect = listen->accept();
                        readBuffer(connect, buffer.clear());
                        buffer.read(connectVersion);
                        bool same = false;
                        IpAddress masterIP;
                        masterIP.ipdeserialize(buffer);
                        buffer.read(connectKind);
                        if (version == connectVersion)
                        {
                            buffer.read(connectTag);
                            masterIP.getIpText(masterIPtext.clear());

                            LOG(MCdebugInfo(1000), unknownJob, "Process incoming connection. reply=%d got(%d,%s)", replyTag,connectTag,masterIPtext.str());

                            same = (kind == connectKind) && masterIP.ipequals(parent) && (connectTag == replyTag);
                        }
                        else
                        {
                            //If connected to a different kind of slave, fake the version number
                            //so it doesn't think there is a version mismatch
                            //can remove when all .exes have new code.
                            if (connectKind != kind)
                            {
                                LOG(MCdebugInfo(1000), unknownJob, "Connection for wrong slave kind (%u vs %u)- ignore", connectKind, kind);
                            }
                        }

                        buffer.clear().append(same).append(replyTag);
                        SocketEndpoint ep(1U);
                        ep.serialize(buffer);
                        buffer.append(version);
                        buffer.append(kind);
                        writeBuffer(connect, buffer);

                        if (same)
                        {
                            socket.setown(connect.getClear());
                            LOG(MCdebugInfo(1000), unknownJob, "Connection matched - continue....");
                            return true;
                        }
                        if ((connectKind == kind) && (version != connectVersion))
                        {
                            LOG(MCdebugInfo, unknownJob, "Version mismatch - terminating slave process expected %d got %d", version, connectVersion);
                            return false;
                        }
                    }
                    catch (IException * e)
                    {
                        EXCLOG(e, "Error reading information from master: ");
                        e->Release();
                    }
                    MilliSleep(50);
                }
            }
        }
        catch (IException * e)
        {
            EXCLOG(e, "Failed to create master listener: ");
            e->Release();
        }
        MilliSleep(fastRand() % 3000 + 2000);
    }

    return false;
}

//---------------------------------------------------------------------------


CRemoteSlave::CRemoteSlave(const char * _name, unsigned _tag, unsigned _version, bool _stayAlive)
{
    slaveName.set(_name);
    tag = _tag;
    stayAlive = _stayAlive;
    version = _version;
}

void CRemoteSlave::run(int argc, char * argv[])
{
    StringBuffer logFile;
    CRemoteParentInfo info;

    bool paramsok = info.processCommandLine(argc, argv, logFile);
    if (logFile.length()==0) { // not expected! Caller queries logfile location via getConfigurationDirectory
#ifdef _WIN32
        logFile.append("c:\\HPCCSystems\\logs\\ftslave");
#else
        logFile.append("/var/log/HPCCSystems/ftslave");
#endif
    }
    if (logFile.length())
        addPathSepChar(logFile);
    logFile.append(slaveName);
    addFileTimestamp(logFile, true);
    logFile.append(".log");
    attachStandardFileLogMsgMonitor(logFile.str(), 0, MSGFIELD_STANDARD, MSGAUD_all, MSGCLS_all, TopDetail, false, true, true);
    queryLogMsgManager()->removeMonitor(queryStderrLogMsgHandler());        // no point logging output to screen if run remote!
    LOG(MCdebugProgress, unknownJob, "Starting %s %s %s %s %s %s %s",slaveName.get(),(argc>1)?argv[1]:"",(argc>2)?argv[2]:"",(argc>3)?argv[3]:"",(argc>4)?argv[4]:"",(argc>5)?argv[5]:"",(argc>6)?argv[6]:"");


    if (paramsok)
    {
        info.log();
        EnableSEHtoExceptionMapping();

        CachedPasswordProvider passwordProvider;
        setPasswordProvider(&passwordProvider);
        try
        {
            if (info.sendReply(version))
            {
                ISocket * masterSocket = info.queryMasterSocket();

                unsigned timeOut = RMTTIME_RESPONSE_MASTER;
                do
                {
                    MemoryBuffer msg;
                    MemoryBuffer results;
                    results.setEndian(__BIG_ENDIAN);

                    bool ok = false;
                    Linked<IException> error;
                    try
                    {
                        if (!catchReadBuffer(masterSocket, msg, timeOut))
                            throwError(RFSERR_MasterSeemsToHaveDied);
                        msg.setEndian(__BIG_ENDIAN);
                        byte action;
                        msg.read(action);
                        passwordProvider.clear();
                        passwordProvider.deserialize(msg);

                        ok = processCommand(action, masterSocket, msg, results);
                    }
                    catch (IException * e)
                    {
                        PrintExceptionLog(e, slaveName.get());
                        error.setown(e);
                    }
                    catch (RELEASE_CATCH_ALL)
                    {
                        LOG(MCuserWarning, unknownJob, "Server seems to have crashed - close done gracefully");
                        error.setown(MakeStringException(999, "Server seems to have crashed - close done gracefully"));
                    }

                    msg.setEndian(__BIG_ENDIAN);
                    msg.clear().append(true).append(ok);
                    serializeException(error, msg);
                    msg.append(results);
                    catchWriteBuffer(masterSocket, msg);

                    LOG(MCdebugProgress, unknownJob, "Results sent from slave %d", info.replyTag);

                    //Acknowledgement before closing down...
                    msg.clear();
                    if (catchReadBuffer(masterSocket, msg, RMTTIME_RESPONSE_MASTER))
                    {
                        LOG(MCdebugProgress, unknownJob, "Terminate acknowledgement received from master for slave %d", info.replyTag);
                        msg.read(ok);
                        assertex(ok);
                    }
                    else
                        LOG(MCdebugProgress, unknownJob, "No terminate acknowledgement received from master for slave %d", info.replyTag);

                    if (error)
                        break;
                    timeOut = 24*60*60*1000;
                } while (stayAlive);

                LOG(MCdebugProgress, unknownJob, "Terminate acknowledgement received from master for slave %d", info.replyTag);
            }
        }
        catch (IException * e)
        {
            PrintExceptionLog(e, slaveName.get());
            e->Release();
        }

        setPasswordProvider(NULL);
    }
    LOG(MCdebugProgress, unknownJob, "Stopping %s", slaveName.get());
}



#if 0
void checkForRemoteAbort(ICommunicator * communicator, mptag_t tag)
{
    if (communicator->probe(1, tag, NULL, 0)!=0)
    {
        CMessageBuffer msg;
        if (!communicator->recv(msg, 1, tag, NULL))
            throwError(RFSERR_TimeoutWaitMaster);

        bool aborting;
        msg.setEndian(__BIG_ENDIAN);
        msg.read(aborting);
        if (aborting)
            throwAbortException();
    }
}

void sendRemoteAbort(INode * node, mptag_t tag)
{
    CMessageBuffer msg;
    msg.clear().append(true);
    queryWorldCommunicator().send(msg, node, tag, MP_ASYNC_SEND);
}
#endif

void checkForRemoteAbort(ISocket * socket)
{
    if (socket->wait_read(0))
    {
        MemoryBuffer msg;
        if (!catchReadBuffer(socket, msg))
            throwError(RFSERR_TimeoutWaitMaster);

        bool aborting;
        msg.setEndian(__BIG_ENDIAN);
        msg.read(aborting);
        if (aborting)
            throwAbortException();
    }
}

bool sendRemoteAbort(ISocket * socket)
{
    LOG(MCdebugInfo, unknownJob, "Send abort to remote slave (%d)", isAborting());

    MemoryBuffer msg;
    msg.append(true);
    return catchWriteBuffer(socket, msg);
}

#if 0
bool sendSlaveCommand(INode * remote, CMessageBuffer & msg, unsigned tag)
{
    if (!queryWorldCommunicator().send(msg, remote, tag, FTTIME_CONNECT_SLAVE))
        throwError1(DFTERR_TimeoutWaitConnect, url.str());

    bool done;
    for (;;)
    {
        msg.clear();
        if (!queryWorldCommunicator().recv(msg, remote, tag, NULL, FTTIME_PROGRESS))
            throwError1(DFTERR_TimeoutWaitSlave, url.str());

        msg.setEndian(__BIG_ENDIAN);
        msg.read(done);
        if (!done)
            return SCcontinue;
    }

    bool ok;
    msg.read(ok);
    error.setown(deserializeException(msg));
    if (error)
        sprayer.setHadError();

    msg.clear().append(true);
    queryWorldCommunicator().send(msg, remote, tag);
}

#endif
