#ifndef SARUNCMD_HPP
#define SARUNCMD_HPP

#ifdef SARUNCMD_API_EXPORTS
    #define SARUNCMD_API DECL_EXPORT
#else
    #define SARUNCMD_API DECL_IMPORT
#endif

class SARUNCMD_API ListWURequests : public CSimpleInterfaceOf<IInterface>
{
public:
    ListWURequests(const char *_outputFields, bool _archived, bool _online)
        : outputFields(_outputFields), archived(_archived), online(_online) {};

    StringAttr wuid, cluster, owner, jobName, state, outputFields;
    StringAttr beforeWU, afterWU;
    StringBuffer fromDate, toDate;
    bool archived = false, online = false, includeDT = false;
    unsigned maxNumberWUs = 500;
    bool descending = false;
};

interface ISashaCmdExecutor : extends IInterface
{
    virtual StringBuffer &getVersion(StringBuffer &version) const = 0;
    virtual StringBuffer &getLastServerMessage(StringBuffer &lastServeressage) const = 0;
    virtual bool restoreECLWorkUnit(const char *wuid) const = 0;
    virtual bool restoreDFUWorkUnit(const char *wuid) const = 0;
    virtual bool archiveECLWorkUnit(const char *wuid) const = 0;
    virtual bool archiveDFUWorkUnit(const char *wuid) const = 0;
    virtual bool backupECLWorkUnit(const char *wuid) const = 0;
    virtual bool backupDFUWorkUnit(const char *wuid) const = 0;
    virtual bool listECLWorkUnit(ListWURequests *req) const = 0;
    virtual bool listDFUWorkUnit(ListWURequests *req) const = 0;
};

extern SARUNCMD_API ISashaCmdExecutor *createSashaCmdExecutor(const SocketEndpoint &ep, unsigned defaultTimeoutSecs=60);

#endif
