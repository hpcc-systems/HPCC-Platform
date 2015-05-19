/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef DASDS_HPP
#define DASDS_HPP

#include "dasubs.ipp"
#include "dasess.hpp"

#define RTM_NONE        0x00
#define RTM_LOCK_READ   0x01        // non-exclusive lock
#define RTM_LOCK_WRITE  0x02        // exclusive lock
#define RTM_LOCK_HOLD   0x04        // long-term lock
#define RTM_LOCK_SUB    0x10        // locks all descendants as well as self
#define RTM_SUB         0x20        // loads entire sub-tree
#define RTM_CREATE      0x40        // replace existing elements
#define RTM_CREATE_UNIQUE (RTM_CREATE | 0x80)   // used with xpath pointing to parent where a newly unique name branch is to be created
#define RTM_CREATE_ADD    (RTM_CREATE | 0x100)  // add to existing elements
#define RTM_CREATE_QUERY  (RTM_CREATE | 0x200)  // creates branch if connect path doesn't exist
#define RTM_DELETE_ON_DISCONNECT 0x400  // auto delete connection root on disconnection.


#define RTM_LOCKBASIC_MASK  (RTM_LOCK_READ | RTM_LOCK_WRITE | RTM_LOCK_HOLD)
#define RTM_CREATE_MASK     (RTM_CREATE | RTM_CREATE_UNIQUE | RTM_CREATE_ADD | RTM_CREATE_QUERY)

#define RTM_MODE(X, M) ((X & M) == M)

// Minimum versions for various server capabilities
#define SDS_SVER_MIN_GETXPATHS_CONNECT "3.2"
#define SDS_SVER_MIN_APPEND_OPT "3.3"
#define SDS_SVER_MIN_GETIDS "3.5"
#define SDS_SVER_MIN_NODESUBSCRIBE "3.12"


enum SDSNotifyFlags { SDSNotify_None=0x00, SDSNotify_Data=0x01, SDSNotify_Structure=0x02, SDSNotify_Added=(SDSNotify_Structure+0x04), SDSNotify_Deleted=(SDSNotify_Structure+0x08), SDSNotify_Renamed=(SDSNotify_Structure+0x10) };
interface ISDSSubscription : extends IInterface
{
    virtual void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen=0, const void *valueData=NULL) = 0;
};

interface ISDSNodeSubscription : extends IInterface
{
    virtual void notify(SubscriptionId id, SDSNotifyFlags flags, unsigned valueLen=0, const void *valueData=NULL) = 0;
};

interface ISDSConnectionSubscription : extends IInterface
{
    virtual void notify() = 0;
};

interface IRemoteConnection : extends IInterface
{
    virtual IPropertyTree *queryRoot() = 0;
    virtual IPropertyTree *getRoot() = 0;
    virtual void changeMode(unsigned mode, unsigned timeout=MP_WAIT_FOREVER, bool suppressReloads=false) = 0;
    virtual void rollback() = 0;
    virtual void rollbackChildren(const char *xpath=NULL, bool force=false) = 0;
    virtual void rollbackChildren(IPropertyTree *parent, bool force=false) = 0;
    virtual void reload(const char *xpath = NULL) = 0;
    virtual void commit() = 0;
    virtual SessionId querySessionId() const = 0;
    virtual unsigned queryMode() const = 0;
    virtual void close(bool deleteRoot=false) = 0; // invalidates root.
    virtual SubscriptionId subscribe(ISDSConnectionSubscription &notify) = 0;
    virtual void unsubscribe(SubscriptionId id) = 0;
    virtual IPropertyTreeIterator *getElements(const char *xpath, IPTIteratorCodes flags = iptiter_null) = 0; // performs 'total' server-side iteration and sends back paths for more efficient client->server access.
};

/////////////

interface IMultipleConnector : extends IInterface
{
    virtual void addConnection(const char *xpath, unsigned mode) = 0;
    virtual unsigned queryConnections() = 0;
    virtual void getConnectionDetails(unsigned which, StringAttr &xpath, unsigned &mode) = 0;
    virtual void serialize(MemoryBuffer &dst) = 0;
};

extern da_decl IMultipleConnector *createIMultipleConnector();

interface IRemoteConnections : extends IInterface
{
    virtual IRemoteConnection *queryConnection(unsigned which) = 0;
    virtual unsigned queryConnections() = 0;
};

interface ILockInfoCollection;
interface ISDSManager
{
    virtual ~ISDSManager() { }
    virtual IRemoteConnection *connect(const char *xpath, SessionId id, unsigned mode, unsigned timeout) = 0;
    virtual IRemoteConnections *connect(IMultipleConnector *mConnect, SessionId id, unsigned timeout) = 0; // timeout applies to each connection
    virtual SubscriptionId subscribe(const char *xpath, ISDSSubscription &notify, bool sub=true, bool sendValue=false) = 0;
    virtual SubscriptionId subscribeExact(const char *xpath, ISDSNodeSubscription &notify, bool sendValue=false) = 0;
    virtual void unsubscribe(SubscriptionId id) = 0;
    virtual void unsubscribeExact(SubscriptionId id) = 0;
    virtual ILockInfoCollection *getLocks(const char *ipPattern=NULL, const char *xpathPattern=NULL) = 0;
    virtual StringBuffer &getUsageStats(StringBuffer &out) = 0;
    virtual StringBuffer &getConnections(StringBuffer &out) = 0;
    virtual StringBuffer &getSubscribers(StringBuffer &out) = 0;
    virtual StringBuffer &getExternalReport(StringBuffer &out) = 0;
    virtual IPropertyTree &queryProperties() const = 0;
    virtual IPropertyTreeIterator *getElementsRaw(const char *xpath,INode *remotedali=NULL, unsigned timeout=MP_WAIT_FOREVER) = 0;
    virtual void setConfigOpt(const char *opt, const char *value) = 0;
    virtual unsigned queryCount(const char *xpath) = 0;
    virtual bool updateEnvironment(IPropertyTree *newEnv, bool forceGroupUpdate, StringBuffer &response) = 0;
};

extern da_decl const char *queryNotifyHandlerName(IPropertyTree *tree);
extern da_decl bool setNotifyHandlerName(const char *handlerName, IPropertyTree *tree);

extern da_decl ISDSManager &querySDS();

// log detail
#define DEFAULT_LOG_SDS_DETAIL 100

// server side (perhaps should move into own header)
interface ISDSNotifyHandler : extends IInterface
{
     virtual void removed(IPropertyTree &tree) = 0;
//   virtual void expired(IPropertyTree &tree) = 0; // TBD
};

// interface to allow different implementations of external server tree values.
interface IExternalHandler : extends IInterface
{
    virtual void resetAsExternal(IPropertyTree &tree) = 0;
    virtual void write(const char *name, IPropertyTree &tree) = 0;
    virtual void read(const char *name, IPropertyTree &owner, MemoryBuffer &mb, bool withValue=true) = 0;
    virtual void readValue(const char *name, MemoryBuffer &mb) = 0;
    virtual void remove(const char *name) = 0;
    virtual bool isValid(const char *name) = 0;
    virtual StringBuffer &getName(StringBuffer &fName, const char *base) = 0;
    virtual StringBuffer &getFilename(StringBuffer &fName, const char *base) = 0;
};

interface ISDSManagerServer : extends ISDSManager
{
    virtual void installNotifyHandler(const char *handlerName, ISDSNotifyHandler *handler) = 0;
    virtual bool removeNotifyHandler(const char *handlerName) = 0;
    virtual IPropertyTree *lockStoreRead() const = 0; // read-only direct access to store
    virtual void unlockStoreRead() const = 0;
    virtual bool setSDSDebug(StringArray &params, StringBuffer &reply)=0;
    virtual unsigned countConnections() = 0;
    virtual unsigned countActiveLocks() = 0;
    virtual unsigned queryExternalSizeThreshold() const = 0;
    virtual void setExternalSizeThreshold(unsigned _size) = 0;
    virtual bool queryRestartOnError() const = 0;
    virtual void setRestartOnError(bool _restart) = 0;
    virtual unsigned queryRequestsPending() const = 0;
    virtual unsigned queryXactCount() const = 0;
    virtual unsigned queryXactMeanTime() const = 0;
    virtual unsigned queryXactMaxTime() const = 0;
    virtual unsigned queryXactMinTime() const = 0;
    virtual unsigned queryConnectMeanTime() const = 0;
    virtual unsigned queryConnectMaxTime() const = 0;
    virtual unsigned queryCommitMeanTime() const = 0;
    virtual unsigned queryCommitMaxTime() const = 0;
    virtual unsigned queryCommitMeanSize() const = 0;
    virtual void saveRequest() = 0;
    virtual bool unlock(__int64 connectionId, bool closeConn, StringBuffer &connectionInfo) = 0;
};


enum SDSExceptionCodes
{
    SDSExcpt_InappropriateXpath,
    SDSExcpt_LockTimeout,
    SDSExcpt_UnknownConnection,
    SDSExcpt_DistributingTransaction,
    SDSExcpt_Reload,
    SDSExcpt_StoreMismatch,
    SDSExcpt_RequestingStore,
    SDSExcpt_BadMode,
    SDSExcpt_LoadInconsistency,
    SDSExcpt_RenameFailure,
    SDSExcpt_UnknownTreeId,
    SDSExcpt_AbortDuringConnection,
    SDSExcpt_InvalidVersionSyntax,
    SDSExcpt_VersionMismatch,
    SDSExcpt_AmbiguousXpath,
    SDSExcpt_OpenStoreFailed,
    SDSExcpt_OrphanedNode,
    SDSExcpt_ServerStoppedLockAborted,
    SDSExcpt_ConnectionAbsent,
    SDSExcpt_OpeningExternalFile,
    SDSExcpt_FailedToCommunicateWithServer,
    SDSExcpt_MissingExternalFile,
    SDSExcpt_FileCreateFailure,
    SDSExcpt_UnrecognisedCommand,
    SDSExcpt_LoadAborted,
    SDSExcpt_IPTError,
    SDSExcpt_Unsupported,
    SDSExcpt_StoreInfoMissing,
    SDSExcpt_ClientCacheDirty,
    SDSExcpt_InvalidSessionId,
    SDSExcpt_LockHeld,
    SDSExcpt_SubscriptionParseError,
    SDSExcpt_SubscriptionNoMatch
};

interface ISDSException : extends IException { };

extern da_decl ISDSManagerServer &querySDSServer();

class da_decl CSDSServerLockBlock
{
    Linked<IPropertyTree> root;
public:
    CSDSServerLockBlock() { lock(); }
    ~CSDSServerLockBlock() { unlock(); }
    IPropertyTree *query() { return root; }
    void lock()
    {
        root.set(querySDSServer().lockStoreRead());
    }
    void unlock()
    {
        if (root)
        {
            root.clear();
            querySDSServer().unlockStoreRead();
        }
    }
    inline IPropertyTree * operator -> () const         { return root; }
    inline operator IPropertyTree *() const             { return root; }
};

interface IDaliServer;
extern da_decl IDaliServer *createDaliSDSServer(IPropertyTree *store); // called for coven members

extern da_decl unsigned querySDSLockTimeoutCount();

// utility

interface IStoreHelper : extends IInterface
{
    virtual StringBuffer &getDetachedDeltaName(StringBuffer &detachName) = 0;
    virtual bool loadDelta(const char *filename, IFile *iFile, IPropertyTree *root) = 0;
    virtual bool loadDeltas(IPropertyTree *root, bool *errors=NULL) = 0;
    virtual bool detachCurrentDelta() = 0;
    virtual void saveStore(IPropertyTree *root, unsigned *newEdition=NULL, bool currentEdition=false) = 0;
    virtual unsigned queryCurrentEdition() = 0;
    virtual StringBuffer &getCurrentStoreFilename(StringBuffer &res, unsigned *crc=NULL) = 0;
    virtual StringBuffer &getCurrentDeltaFilename(StringBuffer &res, unsigned *crc=NULL) = 0;
    virtual StringBuffer &getCurrentStoreInfoFilename(StringBuffer &res) = 0;
    virtual void backup(const char *filename) = 0;
    virtual StringBuffer &getPrimaryLocation(StringBuffer &location) = 0;
    virtual StringBuffer &getBackupLocation(StringBuffer &backupLocation) = 0;
};

enum
{
    SH_External             = 0x0001,
    SH_RecoverFromIncErrors = 0x0002,
    SH_BackupErrorFiles     = 0x0004,
    SH_CheckNewDelta        = 0x0008,
};
extern da_decl IStoreHelper *createStoreHelper(const char *storeName, const char *location, const char *remoteBackupLocation, unsigned configFlags, unsigned keepStores=0, unsigned delay=5000, const bool *abort=NULL);
extern da_decl bool applyXmlDeltas(IPropertyTree &root, IIOStream &stream, bool stopOnError=false);
extern da_decl bool traceAllTransactions(bool on); // server only 

extern da_decl void LogRemoteConn(IRemoteConnection *conn); // logs info about a connection

#endif
