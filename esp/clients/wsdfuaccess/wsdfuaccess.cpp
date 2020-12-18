/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#include <vector>

#include "jliball.hpp"
#include "jflz.hpp"
#include "daclient.hpp"
#include "dautils.hpp"
#include "seclib.hpp"
#include "environment.hpp"
#include "ws_dfu.hpp"
#include "dafsstream.hpp"
#include "dafdesc.hpp"
#include "dadfs.hpp"
#include "dasess.hpp"
#include "thorcommon.hpp"
#include "digisign.hpp"
#include "rmtclient.hpp"

#include "eclwatch_errorlist.hpp" // only for ECLWATCH_FILE_NOT_EXIST
#include "soapmessage.hpp"

#include "wsdfuaccess.hpp"

using namespace dafsstream;
using namespace cryptohelper;

namespace wsdfuaccess
{

//#define TEST_RETURNTEXTRESPONSE

static std::vector<std::string> dfuServiceUrls;
static CriticalSection dfuServiceUrlCrit;
static std::atomic<unsigned> currentDfuServiceUrl{0};
static std::atomic<bool> dfuServiceUrlsDiscovered{false};

void ensureAccessibleDfuServiceURLList()
{
    bool expected = false;
    if (dfuServiceUrlsDiscovered.compare_exchange_strong(expected, true))
    {
        getAccessibleServiceURLList("WsSMC", dfuServiceUrls);
        if (0 == dfuServiceUrls.size())
            throw MakeStringException(-1, "Could not find any DFU services in the target HPCC configuration.");

        for (auto &s: dfuServiceUrls)
            s = s + "/WsDfu/";
    }
}

static unsigned getNumDfuServiceURL()
{
    ensureAccessibleDfuServiceURLList();
    return dfuServiceUrls.size();
}

/* advances to next DFU service URL, wraps if necessary.
 * If concurrent threads are trying to advance, only 1 will succeed, but this call will update current.
 */
static const char *advanceToNextAvailableDFUServiceURL(unsigned &currentURL)
{
    ensureAccessibleDfuServiceURLList();
    // 1st check if need to rollover
    unsigned expected = dfuServiceUrls.size()-1;
    if (currentDfuServiceUrl.compare_exchange_strong(expected, 0))
        currentURL = 0;
    else
    {
        // try to advance by 1.
        if (currentDfuServiceUrl.compare_exchange_strong(currentURL, currentURL+1))
            currentURL++;
        else // someone else already has, get current dfu service url
            currentURL = currentDfuServiceUrl;
    }
    return dfuServiceUrls[currentURL].c_str();
}

static IClientDFUFileAccessResponse *doLookupDFUFileDeprecated(IClientWsDfu *dfuClient, const char *logicalName, const char *requestId, unsigned expirySecs)
{
    Owned<IClientDFUFileAccessRequest> dfuReq = dfuClient->createDFUFileAccessRequest();

    CDfsLogicalFileName lfn;
    lfn.set(logicalName);

    StringBuffer cluster, lfnName;
    lfn.getCluster(cluster);
    lfn.get(lfnName); // remove cluster if present

    IEspDFUFileAccessRequestBase &requestBase = dfuReq->updateRequestBase();
    requestBase.setName(lfnName);
    requestBase.setCluster(cluster);
    requestBase.setExpirySeconds(expirySecs);
    requestBase.setJobId(requestId);
    requestBase.setAccessRole(CFileAccessRole_External);
    requestBase.setAccessType(CSecAccessType_Read);
    requestBase.setReturnBinTypeInfo(true);

    return dfuClient->DFUFileAccess(dfuReq);
}

static IClientDFUFileAccessResponse *doLookupDFUFile(IClientWsDfu *dfuClient, const char *logicalName, const char *requestId, unsigned expirySecs)
{
    Owned<IClientDFUFileAccessV2Request> dfuReq = dfuClient->createDFUFileAccessV2Request();

    CDfsLogicalFileName lfn;
    lfn.set(logicalName);

    StringBuffer cluster, lfnName;
    lfn.getCluster(cluster);
    lfn.get(lfnName); // remove cluster if present

    dfuReq->setName(lfnName);
    dfuReq->setCluster(cluster);
    dfuReq->setExpirySeconds(expirySecs);
    dfuReq->setRequestId(requestId);
#ifdef TEST_RETURNTEXTRESPONSE
    dfuReq->setReturnTextResponse(true);
#endif

    return dfuClient->DFUFileAccessV2(dfuReq);
}

static IDFUFileAccess *doLookupDFUFileHandleLegacy(const char *serviceUrl, const char *logicalName, const char *requestId, unsigned expirySecs, const char *user, const char *password)
{
    Owned<IClientWsDfu> dfuClient = createWsDfuClient();
    dfuClient->addServiceUrl(serviceUrl);
    dfuClient->setUsernameToken(user, password, "");
    Owned<IClientDFUFileAccessResponse> dfuResp;
    try
    {
        dfuResp.setown(doLookupDFUFile(dfuClient, logicalName, requestId, expirySecs));
    }
    catch (IException *e)
    {
        /* NB: there should really be a different IException class and a specific error code
         * The server knows it's an unsupported method.
         */
        if (SOAP_SERVER_ERROR != e->errorCode())
            throw;
        // fall through and try deprecated method
        e->Release();
    }
    if (!dfuResp)
        dfuResp.setown(doLookupDFUFileDeprecated(dfuClient, logicalName, requestId, expirySecs));

    const IMultiException *excep = &dfuResp->getExceptions(); // NB: warning despite getXX name, this does not Link
    if (excep->ordinality() > 0)
        throw LINK((IMultiException *)excep); // NB - const IException.. not caught in general..

    Owned<IDFUFileAccess> ret = createDFUFileAccess(dfuResp->getAccessInfo().getMetaInfoBlob());

    if (!ret->queryEngineInterface()->queryMeta()) // as a result of legacy WsDFU version
    {
        const MemoryBuffer &binLayout = dfuResp->getAccessInfo().getRecordTypeInfoBin();
        if (0 == binLayout.length())
            throw makeStringExceptionV(0, "lookupDFUFile(%s) - layout missing", logicalName);
        ret->queryEngineInterface()->setLayoutBin(binLayout.length(), binLayout.bytes());
    }
    return ret.getClear();
}

static IClientDFUFileCreateResponse *doCreateDFUFileDeprecated(IClientWsDfu *dfuClient, const char *logicalName, const char *cluster, DFUFileType type, const char *recDef, const char *requestId, unsigned expirySecs)
{
    Owned<IClientDFUFileCreateRequest> dfuReq = dfuClient->createDFUFileCreateRequest();

    dfuReq->setECLRecordDefinition(recDef);
    IEspDFUFileAccessRequestBase &requestBase = dfuReq->updateRequestBase();
    requestBase.setName(logicalName);
    requestBase.setCluster(cluster);
    requestBase.setExpirySeconds(expirySecs);
    requestBase.setJobId(requestId);
    requestBase.setAccessRole(CFileAccessRole_External);
    requestBase.setAccessType(CSecAccessType_Write);
    requestBase.setReturnBinTypeInfo(true);

    return dfuClient->DFUFileCreate(dfuReq);
}

static IClientDFUFileCreateResponse *doCreateDFUFile(IClientWsDfu *dfuClient, const char *logicalName, const char *cluster, DFUFileType type, const char *recDef, const char *requestId, unsigned expirySecs, bool compressed)
{
    Owned<IClientDFUFileCreateV2Request> dfuReq = dfuClient->createDFUFileCreateV2Request();

    dfuReq->setECLRecordDefinition(recDef);
    dfuReq->setName(logicalName);
    dfuReq->setCluster(cluster);
    dfuReq->setExpirySeconds(expirySecs);
    dfuReq->setRequestId(requestId);
    dfuReq->setCompressed(compressed);
#ifdef TEST_RETURNTEXTRESPONSE
    dfuReq->setReturnTextResponse(true);
#endif

    CDFUFileType serviceType;
    switch (type)
    {
        case dft_flat:
            serviceType = CDFUFileType_Flat;
            break;
        case dft_index:
            serviceType = CDFUFileType_Index;
            break;
        default:
            throwStringExceptionV(0, "Invalid DFU file type: %u", (unsigned)type);
    }
    dfuReq->setType(serviceType);
    return dfuClient->DFUFileCreateV2(dfuReq);
}

static IDFUFileAccess *doCreateDFUFileHandleLegacy(const char *serviceUrl, const char *logicalName, const char *cluster, DFUFileType type, const char *recDef, const char *requestId, unsigned expirySecs, bool compressed, const char *user, const char *password)
{
    Owned<IClientWsDfu> dfuClient = createWsDfuClient();
    dfuClient->addServiceUrl(serviceUrl);
    dfuClient->setUsernameToken(user, password, "");

    Owned<IClientDFUFileCreateResponse> dfuResp;
    try
    {
        dfuResp.setown(doCreateDFUFile(dfuClient, logicalName, cluster, type, recDef, requestId, expirySecs, compressed));
    }
    catch (IException *e)
    {
        /* NB: there should really be a different IException class and a specific error code
         * The server knows it's an unsupported method.
         */
        if (SOAP_SERVER_ERROR != e->errorCode())
            throw;
        // fall through and try deprecated method
        e->Release();
    }
    if (!dfuResp)
    {
        if (compressed)
            WARNLOG("createDFUFile(%s), legacy esp server does not support creating compressed files", logicalName);
        dfuResp.setown(doCreateDFUFileDeprecated(dfuClient, logicalName, cluster, type, recDef, requestId, expirySecs));
    }

    const IMultiException *excep = &dfuResp->getExceptions(); // NB: warning despite getXX name, this does not Link
    if (excep->ordinality() > 0)
        throw LINK((IMultiException *)excep); // NB: - const IException.. not caught in general..

    Owned<IDFUFileAccess> ret = createDFUFileAccess(dfuResp->getAccessInfo().getMetaInfoBlob(), dfuResp->getFileId());
    // NB: patch up record definition if server didn't return it (because legacy WsDFU version)
    if (!ret->queryEngineInterface()->queryProperties().hasProp("ECL"))
        ret->queryEngineInterface()->queryProperties().setProp("ECL", recDef);
    if (!ret->queryEngineInterface()->queryMeta()) // as a result of legacy WsDFU version
    {
        const MemoryBuffer &binLayout = dfuResp->getAccessInfo().getRecordTypeInfoBin();
        if (0 == binLayout.length())
            throw makeStringExceptionV(0, "createDFUFile(%s) - layout missing", logicalName);
        ret->queryEngineInterface()->setLayoutBin(binLayout.length(), binLayout.bytes());
    }

    return ret.getClear();
}

static void doPublishDFUFile(const char *serviceUrl, IDFUFileAccess *dfuFile, bool overwrite, const char *user, const char *password)
{
    Owned<IClientWsDfu> dfuClient = createWsDfuClient();
    dfuClient->addServiceUrl(serviceUrl);
    dfuClient->setUsernameToken(user, password, "");

    Owned<IClientDFUFilePublishRequest> dfuReq = dfuClient->createDFUFilePublishRequest();

    dfuReq->setFileId(dfuFile->queryFileId());

    dfuReq->setOverwrite(overwrite); // NB: WsDfu min_ver 1.50
    IFileDescriptor &fileDesc = dfuFile->queryEngineInterface()->queryFileDescriptor();

    MemoryBuffer mb;
    fileDesc.serialize(mb);
    dfuReq->setFileDescriptorBlob(mb); // NB: WsDfu min_ver 1.50

    const char *eclRecDef = fileDesc.queryProperties().queryProp("ECL");
    dfuReq->setECLRecordDefinition(eclRecDef); // NB: WsDfu depv_ver < 1.50

    Owned<IClientDFUFilePublishResponse> dfuResp = dfuClient->DFUFilePublish(dfuReq);

    const IMultiException *excep = &dfuResp->getExceptions(); // NB: warning despite getXX name, this does not Link
    if (excep->ordinality() > 0)
        throw LINK((IMultiException *)excep); // NB: - const IException.. not caught in general..
}

// wrapper to the doLookupDFUFile, that discovers and tries DFUService URL's
IDFUFileAccess *lookupDFUFile(const char *logicalName, const char *requestId, unsigned expirySecs, const char *user, const char *password)
{
    unsigned currentUrl;
    const char *espServiceUrl = advanceToNextAvailableDFUServiceURL(currentUrl);
    unsigned c = getNumDfuServiceURL(); // max attempts
    while (c)
    {
        try
        {
            /* JCSMORE - where would locking fit in?
             * *IF* Esp established lock, then there'd be no association with this client (no state), and if Esp restarted lock would be lost,
             * if this client died, the lock would remain (on Esp).
             *
             * Idea:
             * 1) Esp establishes lock on behalf of this client.
             * 2) This client sends keep-alive packets every N seconds (To Esp).
             * 3) Esp ensures lock remains alive somehow (something (Esp?) could keep persistent [written] state of active locks?)
             * 4) If no keep-alive for a lock, Esp closes it.
             *
             * Would require the ability (in Dali) to create locks without session association.
             * As long as Dali is the lock manager, Would probably be best if the keep-alive packets were
             * forwarded to Dali, and it managed the live/stale locks.
             */

            return doLookupDFUFileHandleLegacy(espServiceUrl, logicalName, requestId, expirySecs, user, password);
        }
        catch (IJSOCK_Exception *e)
        {
            EXCLOG(e, nullptr);
            e->Release();
        }
        catch (IException *e)
        {
            if (ECLWATCH_FILE_NOT_EXIST == e->errorCode())
            {
                e->Release();
                return nullptr; // not found
            }
            throw;
        }
        espServiceUrl = advanceToNextAvailableDFUServiceURL(currentUrl);
        --c;
    }
    StringBuffer msg("Failed to contact DFU service: { ");
    for (auto &url: dfuServiceUrls)
        msg.append(url.c_str());
    msg.append("}");
    throw makeStringException(0, msg.str());
}

IDFUFileAccess *lookupDFUFile(const char *logicalName, const char *requestId, unsigned expirySecs, IUserDescriptor *userDesc)
{
    assertex(userDesc);
    StringBuffer user, password;
    userDesc->getUserName(user);
    userDesc->getPassword(password);
    IDFUFileAccess *ret = lookupDFUFile(logicalName, requestId, expirySecs, user, password);
    if (ret)
        ret->setFileOption(dfo_compressedRemoteStreams);
    return ret;
}


// wrapper to the doCreateDFUFile, that discovers and tries DFUService URL's
IDFUFileAccess *createDFUFile(const char *logicalName, const char *cluster, DFUFileType type, const char *recDef, const char *requestId, unsigned expirySecs, bool compressed, const char *user, const char *password)
{
    unsigned currentUrl;
    const char *espServiceUrl = advanceToNextAvailableDFUServiceURL(currentUrl);
    unsigned c = getNumDfuServiceURL(); // max attempts
    while (c)
    {
        try
        {
            return doCreateDFUFileHandleLegacy(espServiceUrl, logicalName, cluster, type, recDef, requestId, expirySecs, compressed, user, password);
        }
        catch (IJSOCK_Exception *e)
        {
            EXCLOG(e, nullptr);
            e->Release();
        }
        advanceToNextAvailableDFUServiceURL(currentUrl);
        --c;
    }
    StringBuffer msg("Failed to contact DFU service: { ");
    for (auto &url: dfuServiceUrls)
        msg.append(url.c_str());
    msg.append("}");
    throw makeStringException(0, msg.str());
}

// NB: no way to create grouped flat file output at the moment, but not sure would ever want to support that.
IDFUFileAccess *createDFUFile(const char *logicalName, const char *cluster, DFUFileType type, const char *recDef, const char *requestId, unsigned expirySecs, bool compressed, IUserDescriptor *userDesc)
{
    assertex(userDesc);
    StringBuffer user, password;
    userDesc->getUserName(user);
    userDesc->getPassword(password);
    return createDFUFile(logicalName, cluster, type, recDef, requestId, expirySecs, compressed, user, password);
}

// wrapper to the doPublishDFUFile, that discovers and tries DFUService URL's
void publishDFUFile(IDFUFileAccess *dfuFile, bool overwrite, const char *user, const char *password)
{
    unsigned currentUrl;
    const char *espServiceUrl = advanceToNextAvailableDFUServiceURL(currentUrl);
    unsigned c = getNumDfuServiceURL(); // max attempts
    while (c)
    {
        try
        {
            doPublishDFUFile(espServiceUrl, dfuFile, overwrite, user, password);
            return;
        }
        catch (IJSOCK_Exception *e)
        {
            EXCLOG(e, nullptr);
            e->Release();
        }
        advanceToNextAvailableDFUServiceURL(currentUrl);
        --c;
    }
    StringBuffer msg("Failed to contact DFU service: { ");
    for (auto &url: dfuServiceUrls)
        msg.append(url.c_str());
    msg.append("}");
    throw makeStringException(0, msg.str());
}

void publishDFUFile(IDFUFileAccess *dfuFile, bool overwrite, IUserDescriptor *userDesc)
{
    assertex(userDesc);
    StringBuffer user, password;
    userDesc->getUserName(user);
    userDesc->getPassword(password);
    publishDFUFile(dfuFile, overwrite, user, password);
}



/*
 * createDFUFileAccess() and encodeDFUFileMeta() will normally be called by the DFU service
 * via a DFS file request. So that the meta info blob can be returned to the client of the service.
 * However, for testing purposes it's also useful to create these blobs elsewhere directly from IFileDescriptor's
 */
IPropertyTree *createDFUFileMetaInfo(const char *fileName, IFileDescriptor *fileDesc, const char *requestId, const char *accessType, unsigned expirySecs,
                                     IUserDescriptor *userDesc, const char *keyPairName, unsigned port, bool secure, unsigned maxFileAccessExpirySeconds)
{
    /*
     * version
     * fileName
     * requestId [optional]
     * accessType [const "READ" for this method]
     * user
     * port (int)      // port # of dafilesrv srvice to connect to
     * secure (bool)   // if true = SSL connection
     * keyPairName      // name of key pair to use
     * expiryTime      // (seconds) timeout for validity of this request
     * jsonTypeInfo     // JSON representation of the file's record definition
     */
    Owned<IPropertyTree> metaInfo = createPTree();

    metaInfo->setProp("logicalFilename", fileName);
    if (!isEmptyString(requestId))
        metaInfo->setProp("requestId", requestId);
    metaInfo->setProp("accessType", accessType);
    StringBuffer userStr;
    if (userDesc)
        metaInfo->setProp("user", userDesc->getUserName(userStr).str());

    // key, port, secure
    metaInfo->setPropInt("port", port);
    metaInfo->setPropBool("secure", secure);
    if (!isEmptyString(keyPairName))
        metaInfo->setProp("keyPairName", keyPairName);

    // expiry time
    if (expirySecs > maxFileAccessExpirySeconds)
        expirySecs = maxFileAccessExpirySeconds;
    time_t now;
    time(&now);
    CDateTime expiryDt;
    expiryDt.set(now + expirySecs);
    StringBuffer expiryTime;
    expiryDt.getString(expiryTime);
    metaInfo->setProp("expiryTime", expiryTime);

    // layout
    MemoryBuffer binLayout;
    if (getDaliLayoutInfo(binLayout, fileDesc->queryProperties()))
        metaInfo->setPropBin("binLayout", binLayout.length(), binLayout.toByteArray());

    // file meta info
    INode *node1 = fileDesc->queryNode(0);
    SocketEndpoint ep = node1->endpoint();
    unsigned dafilesrvVersion = getCachedRemoteVersion(node1->endpoint(), secure);

    if (dafilesrvVersion < DAFILESRV_STREAMGENERAL_MINVERSION)
    {
        metaInfo->setPropInt("version", 1); // legacy format
        extractFilePartInfo(*metaInfo, *fileDesc);
    }
    else
    {
        metaInfo->setPropInt("version", DAFILESRV_METAINFOVERSION);
        IPropertyTree *fileInfoTree = metaInfo->setPropTree("FileInfo");
        fileDesc->serializeTree(*fileInfoTree);
    }
    return metaInfo.getClear();
}

StringBuffer &encodeDFUFileMeta(StringBuffer &metaInfoBlob, IPropertyTree *metaInfo, IConstEnvironment *environment)
{
    MemoryBuffer metaInfoMb;

    /* NB: If file access security is disabled in the environment, or on a per cluster basis
     * keyPairName will be blank. In that case the meta data is returned in plain format.
     * NB2: Dafilesrv's would also require file access security to be disabled in that case,
     * otherwise they will be denied access.
     * Should be part of the same configuration setup.
     */
#ifdef _USE_OPENSSL
    if (metaInfo->hasProp("keyPairName") && environment) // without it, meta data is not encrypted
    {
        MemoryBuffer metaInfoBlob;
        metaInfo->serialize(metaInfoBlob);

        const char *keyPairName = metaInfo->queryProp("keyPairName");
        const char *privateKeyFName = environment->getPrivateKeyPath(keyPairName);
        Owned<CLoadedKey> privateKey = loadPrivateKeyFromFile(privateKeyFName, nullptr);
        StringBuffer metaInfoSignature;
        digiSign(metaInfoSignature, metaInfoBlob.length(), metaInfoBlob.bytes(), *privateKey);

        Owned<IPropertyTree> metaInfoEnvelope = createPTree();
        metaInfoEnvelope->setProp("signature", metaInfoSignature);
        metaInfoEnvelope->setPropBin("metaInfoBlob", metaInfoBlob.length(), metaInfoBlob.bytes());
        metaInfoEnvelope->serialize(metaInfoMb.clear());
    }
    else
#endif
        metaInfo->serialize(metaInfoMb);

    MemoryBuffer compressedMetaInfoMb;
    fastLZCompressToBuffer(compressedMetaInfoMb, metaInfoMb.length(), metaInfoMb.bytes());
    JBASE64_Encode(compressedMetaInfoMb.bytes(), compressedMetaInfoMb.length(), metaInfoBlob, false);
    return metaInfoBlob;
}



} // namespace wsdfuaccess


#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#include "dafsserver.hpp"
#include "rmtfile.hpp"
#include "dafscommon.hpp"
#include "portlist.h"

using namespace wsdfuaccess;
class DFUAccessTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(DFUAccessTests);
        CPPUNIT_TEST(testStartServer);
        CPPUNIT_TEST(testDaFsStreamingStd);
        CPPUNIT_TEST(testDaFsStreamingCompressed);
        CPPUNIT_TEST(testDaFsStreamingGrouped);
        CPPUNIT_TEST(testDaFsStreamingCompressedAndGrouped);
        CPPUNIT_TEST(testFinish);
    CPPUNIT_TEST_SUITE_END();

    static unsigned serverPort;
    StringBuffer basePath;
    Owned<CSimpleInterface> serverThread;
    Owned<IFileDescriptor> fileDesc;
protected:
    void testStartServer()
    {
        Owned<ISocket> socket;

        unsigned endPort = MP_END_PORT;
        while (1)
        {
            try
            {
                socket.setown(ISocket::create(serverPort));
                break;
            }
            catch (IJSOCK_Exception *e)
            {
                if (e->errorCode() != JSOCKERR_port_in_use)
                {
                    StringBuffer eStr;
                    e->errorMessage(eStr);
                    e->Release();
                    CPPUNIT_ASSERT_MESSAGE(eStr.str(), 0);
                }
                else if (serverPort == endPort)
                {
                    e->Release();
                    CPPUNIT_ASSERT_MESSAGE("Could not find a free port to use for remote file server", 0);
                }
            }
            ++serverPort;
        }

        basePath.append("//");
        SocketEndpoint ep(serverPort);
        ep.getUrlStr(basePath);

        char cpath[_MAX_DIR];
        if (!GetCurrentDirectory(_MAX_DIR, cpath))
            CPPUNIT_ASSERT_MESSAGE("Current directory path too big", 0);
        else
            basePath.append(cpath);
        addPathSepChar(basePath);

        class CServerThread : public CSimpleInterface, implements IThreaded
        {
            CThreaded threaded;
            Owned<IRemoteFileServer> server;
            Linked<ISocket> socket;
        public:
            CServerThread(IRemoteFileServer *_server, ISocket *_socket) : server(_server), socket(_socket), threaded("CServerThread")
            {
                threaded.init(this);
            }
            ~CServerThread()
            {
                threaded.join();
            }
        // IThreaded
            virtual void threadmain() override
            {
                DAFSConnectCfg sslCfg = SSLNone;
                server->run(sslCfg, socket, nullptr, nullptr);
            }
        };
        Owned<IRemoteFileServer> server = createRemoteFileServer();
        serverThread.setown(new CServerThread(QUERYINTERFACE(server.getClear(), IRemoteFileServer), socket.getClear()));
    }
    void testDaFsStreaming(bool compressed, bool grouped)
    {
        const char *thorInstance = "mythor";
        const char *groupName = thorInstance;
        const char *fname = ".::dfuaccess::testfname1";
        IUserDescriptor *userDesc = nullptr;
        const char *keyPairName = nullptr;
        unsigned port = 0;
        bool secure = false;
        unsigned expiryTime = 60;
        unsigned maxFileAccessExpirySeconds = 300;

        unsigned numRecsPerPart = 1000;

        const char *eclRecDef = "{ string5 f1; string10 f2; };";
        size32_t fixedRecSize = 15;

        fileDesc.setown(createFileDescriptor());

        GroupType groupType;
        StringBuffer basedir;

        SocketEndpointArray eps;
        SocketEndpoint ep(".", serverPort);
        eps.append(ep);
        Owned<IGroup> group = createIGroup(eps);

        fileDesc.setown(createFileDescriptor(fname, "thor", "mythor", group));
        fileDesc->queryProperties().setProp("ECL", eclRecDef);
        if (grouped)
            fileDesc->queryProperties().setPropBool("@grouped", true);

        Owned<IPropertyTree> metaInfo = createDFUFileMetaInfo(fname, fileDesc, "cppunit-test1", "WRITE", 30,
                                                              userDesc, keyPairName, port, secure, maxFileAccessExpirySeconds);
        StringBuffer metaInfoBlob;
        encodeDFUFileMeta(metaInfoBlob, metaInfo, nullptr);

        Owned<IDFUFileAccess> newFile = createDFUFileAccess(metaInfoBlob);
        newFile->setStreamReplyLimitK(1); // set a low limit to force testing of continuation etc.

        if (compressed)
            newFile->setFileOption(dfo_compressedRemoteStreams);
        else
            newFile->clearFileOption(dfo_compressedRemoteStreams);

        CRC32 writeCrc32;
        // write
        unsigned n = newFile->queryNumParts();
        for (unsigned p=0; p<n; p++)
        {
            Owned<IDFUFilePartWriter> writer = newFile->createFilePartWriter(p);
            writer->start();

            for (unsigned r=0; r<numRecsPerPart; r++)
            {
                VStringBuffer rowData("%5u%10u", r, r);
                writer->write(fixedRecSize, rowData.str());
                writeCrc32.tally(fixedRecSize, rowData.str());
                if (grouped)
                    writer->write(0, nullptr); // eog
            }
        }
        newFile->setFilePropertyInt("@recordCount", numRecsPerPart);

        // publish would normally happen here, but this unittest is self-contained (no esp etc.)


        CRC32 readCrc32;
        // read back
        for (unsigned p=0; p<n; p++)
        {
            Owned<IDFUFilePartReader> reader = newFile->createFilePartReader(p, 0, nullptr, true);
            reader->start();

            for (unsigned r=0; r<numRecsPerPart; r++)
            {
                size32_t sz;
                const void *row = reader->nextRow(sz);
                if (!row)
                {
                    row = reader->nextRow(sz);
                    assertex(row);
                }
                readCrc32.tally(sz, row);
#ifdef _DEBUG
                printf("%.*s%.*s\n", 5, (const char *)row, 10, ((const char *)row)+5);
#endif
            }
        }
        if (writeCrc32.get() != readCrc32.get())
        {
            VStringBuffer errMsg("DFU write/read test: crc's don't match. Write crc=%x, read crc=%x", writeCrc32.get(), readCrc32.get());
            CPPUNIT_ASSERT_MESSAGE(errMsg.str(), 0);
        }


        // read back from fetch stream
        class CFPosStream : public CSimpleInterfaceOf<IFPosStream>
        {
            offset_t nextFPos = 0;
            offset_t endPos = 0;
            size32_t fixedSz;
        public:
            CFPosStream(unsigned numRecs, size32_t _fixedSz) : fixedSz(_fixedSz)
            {
                endPos = numRecs * fixedSz;
            }
            virtual bool next(offset_t &fpos) override
            {
                fpos = nextFPos;
                nextFPos += fixedSz * 10; // skip ahead 10 recs
                return fpos < endPos;
            }
            virtual bool peekAvailable() const override
            {
                return nextFPos < endPos;
            }
        };

        Owned<IFPosStream> fposStream = new CFPosStream(numRecsPerPart, fixedRecSize+(grouped?1:0));
        for (unsigned p=0; p<n; p++)
        {
            Owned<IDFUFilePartReader> reader = newFile->createFilePartReader(p, 0, nullptr, false);

            reader->setFetchStream(fposStream);

            reader->start();

            for (unsigned r=0; r<numRecsPerPart; r++)
            {
                size32_t sz;
                const void *row = reader->nextRow(sz);
                if (!row)
                    break;
#ifdef _DEBUG
                printf("%.*s%.*s\n", 5, (const char *)row, 10, ((const char *)row)+5);
#endif
            }
        }
    }
    void testDaFsStreamingStd()
    {
        testDaFsStreaming(false, false);
    }
    void testDaFsStreamingCompressed()
    {
        testDaFsStreaming(true, false);
    }
    void testDaFsStreamingGrouped()
    {
        testDaFsStreaming(false, true);
    }
    void testDaFsStreamingCompressedAndGrouped()
    {
        testDaFsStreaming(true, true);
    }
    void testFinish()
    {
        // clearup
        if (fileDesc)
        {
            RemoteFilename rfn;
            fileDesc->getFilename(0, 0, rfn);
            StringBuffer path;
            rfn.getPath(path);
            Owned<IFile> iFile = createIFile(path);
            iFile->remove();
        }

        SocketEndpoint ep(serverPort);
        Owned<ISocket> sock = ISocket::connect_timeout(ep, 60 * 1000);
        CPPUNIT_ASSERT(RFEnoerror == stopRemoteServer(sock));

        serverThread.clear();
    }
};

/* MP_START_PORT -> MP_END_PORT is the MP reserved dynamic port range, and is used here for convenience.
 * MP_START_PORT is used as starting point to find an available port for the temporary dafilesrv service in these unittests.
 * All (MP) components using this range always check and find an unused port.
 */
unsigned DFUAccessTests::serverPort = MP_START_PORT;


CPPUNIT_TEST_SUITE_REGISTRATION( DFUAccessTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( DFUAccessTests, "DFUAccessTests" );


#endif // _USE_CPPUNIT
