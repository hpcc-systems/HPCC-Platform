#ifndef SACMD_HPP
#define SACMD_HPP

enum SashaCommandAction
{
    SCA_null,
    SCA_GETVERSION,
    SCA_STOP,
    SCA_LIST,
    SCA_RESTORE,
    SCA_ARCHIVE,
    SCA_GET, // returns WUs in results
    SCA_BACKUP,
    SCA_XREF, // ids are clusters to xref
    SCA_COALESCE_SUSPEND,
    SCA_COALESCE_RESUME,
    SCA_WORKUNIT_SERVICES_GET,
    SCA_LISTDT,
    SCA_LIST_WITH_MATCHING_COUNT
};


interface INode;
class CDateTime;

interface ISashaCommand: extends IInterface
{
    virtual SashaCommandAction getAction() = 0;
    virtual void setAction(SashaCommandAction action) = 0;
    virtual void addId(const char *id) = 0;
    virtual unsigned numIds() = 0;
    virtual void clearIds() = 0;
    virtual bool getId(unsigned i, StringBuffer &id) = 0;
    virtual const char *queryId(unsigned i) = 0;
    virtual const char *queryAfter() = 0;
    virtual void setAfter(const char *val) = 0;
    virtual const char *queryBefore() = 0;
    virtual void setBefore(const char *val) = 0;
    virtual const char *queryState() = 0;
    virtual void setState(const char *val) = 0;
    virtual const char *queryOwner() = 0;
    virtual void setOwner(const char *val) = 0;         
    virtual const char *queryCluster() = 0;
    virtual void setCluster(const char *val) = 0;
    virtual const char *queryJobName() = 0;
    virtual void setJobName(const char *val) = 0;
    virtual const char *queryOutputFormat() = 0;
    virtual void setDfuCmdName(const char *val) = 0;
    virtual const char *queryDfuCmdName() = 0;
    virtual void setOutputFormat(const char *val) = 0;
    virtual bool getOnline() = 0;
    virtual void setOnline(bool val) = 0;
    virtual bool getArchived() = 0;
    virtual void setArchived(bool val) = 0;
    virtual unsigned getStart() = 0;
    virtual void setStart(unsigned val) = 0;
    virtual unsigned getLimit() = 0;
    virtual void setLimit(unsigned val) = 0;
    virtual unsigned numResults() = 0;
    virtual void clearResults() = 0;
    virtual bool getResult(unsigned i, StringBuffer &res) = 0;
    virtual bool addResult(const char *res) = 0;
    virtual bool resultsOverflowed() = 0;
    virtual void setXslt(const char *xslt) = 0;     

    virtual bool send(INode *node,unsigned timeout=0) = 0;
    virtual bool accept(unsigned timeout) = 0;
    virtual void cancelaccept() = 0;
    virtual bool reply(bool clearBuf) = 0;
    virtual bool IDSWithMatchingNumberReply() = 0;

    virtual bool getDFU() = 0;
    virtual void setDFU(bool val) = 0;

    virtual bool getWUSmode() = 0;                      // extended interface for workunit services
    virtual void setWUSmode(bool val) = 0;
    virtual void setPriority(const char *val) = 0;
    virtual const char *queryPriority() = 0;
    virtual void setFileRead(const char *val) = 0;
    virtual const char *queryFileRead() = 0;
    virtual void setFileWritten(const char *val) = 0;
    virtual const char *queryFileWritten() = 0;
    virtual void setRoxieCluster(const char *val) = 0;
    virtual const char *queryRoxieCluster() = 0;
    virtual void setEclContains(const char *val) = 0;
    virtual const char *queryEclContains() = 0;
    virtual bool WUSreply() = 0;    // if only one byte is WUSstatus
    virtual byte getWUSresult(MemoryBuffer &mb) = 0;
    virtual void setWUSresult(MemoryBuffer &mb) = 0;

    virtual void addDT(CDateTime &dt) = 0;
    virtual void getDT(CDateTime &dt,unsigned i) = 0;   // returned by SCA_LISTDT

    virtual int getNumberOfIdsMatching() = 0;
    virtual void setNumberOfIdsMatching(int val) = 0;
};

#define WUS_STATUS_OK           ((byte)0)

extern ISashaCommand *createSashaCommand();

#endif
