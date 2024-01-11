#ifndef WFCONTEXT_HPP
#define WFCONTEXT_HPP

#include "eclrtl.hpp"
#include "eclhelper.hpp"

class ECLRTL_API IndirectCodeContext : implements ICodeContext
{
protected:
    ICodeContext * ctx;
public:
    IndirectCodeContext(ICodeContext * _ctx = NULL) : ctx(_ctx) {}

    void set(ICodeContext * _ctx) { ctx = _ctx; }
    virtual const char *loadResource(unsigned id) override;
    virtual void setResultBool(const char *name, unsigned sequence, bool value) override;
    virtual void setResultData(const char *name, unsigned sequence, int len, const void * data) override;
    virtual void setResultDecimal(const char * stepname, unsigned sequence, int len, int precision, bool isSigned, const void *val) override;
    virtual void setResultInt(const char *name, unsigned sequence, __int64 value, unsigned size) override;
    virtual void setResultRaw(const char *name, unsigned sequence, int len, const void * data) override;
    virtual void setResultReal(const char * stepname, unsigned sequence, double value) override;
    virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer) override;
    virtual void setResultString(const char *name, unsigned sequence, int len, const char * str) override;
    virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 value, unsigned size) override;
    virtual void setResultUnicode(const char *name, unsigned sequence, int len, UChar const * str) override;
    virtual void setResultVarString(const char * name, unsigned sequence, const char * value) override;
    virtual void setResultVarUnicode(const char * name, unsigned sequence, UChar const * value) override;
    virtual bool getResultBool(const char * name, unsigned sequence) override;
    virtual void getResultData(unsigned & tlen, void * & tgt, const char * name, unsigned sequence) override;
    virtual void getResultDecimal(unsigned tlen, int precision, bool isSigned, void * tgt, const char * stepname, unsigned sequence) override;
    virtual void getResultRaw(unsigned & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) override;
    virtual void getResultSet(bool & isAll, size32_t & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) override;
    virtual __int64 getResultInt(const char * name, unsigned sequence) override;
    virtual double getResultReal(const char * name, unsigned sequence) override;
    virtual void getResultString(unsigned & tlen, char * & tgt, const char * name, unsigned sequence) override;
    virtual void getResultStringF(unsigned tlen, char * tgt, const char * name, unsigned sequence) override;
    virtual void getResultUnicode(unsigned & tlen, UChar * & tgt, const char * name, unsigned sequence) override;
    virtual char *getResultVarString(const char * name, unsigned sequence) override;
    virtual UChar *getResultVarUnicode(const char * name, unsigned sequence) override;
    virtual unsigned getResultHash(const char * name, unsigned sequence) override;
    virtual unsigned getExternalResultHash(const char * wuid, const char * name, unsigned sequence) override;
    virtual char *getWuid() override;
    virtual void getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) override;
    virtual void executeGraph(const char * graphName, bool realThor, size32_t parentExtractSize, const void * parentExtract) override;
    virtual char * getExpandLogicalName(const char * logicalName) override;
    virtual void addWuException(const char * text, unsigned code, unsigned severity, const char *source) override;
    virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort) override;
    virtual IUserDescriptor *queryUserDescriptor() override;
    virtual IThorChildGraph * resolveChildQuery(__int64 activityId, IHThorArg * colocal) override;
    virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 hash) override;
    virtual unsigned getNodes() override;
    virtual unsigned getNodeNum() override;
    virtual char *getFilePart(const char *logicalPart, bool create) override;
    virtual unsigned __int64 getFileOffset(const char *logicalPart) override;
    virtual IDistributedFileTransaction *querySuperFileTransaction() override;
    virtual char *getEnv(const char *name, const char *defaultValue) const override;
    virtual char *getJobName() override;
    virtual char *getJobOwner() override;
    virtual char *getClusterName() override;
    virtual char *getGroupName() override;
    virtual char * queryIndexMetaData(char const * lfn, char const * xpath) override;
    virtual unsigned getPriority() const override;
    virtual char *getPlatform() override;
    virtual char *getOS() override;
    virtual IEclGraphResults * resolveLocalQuery(__int64 activityId) override;
    virtual char *getEnv(const char *name, const char *defaultValue);
    virtual unsigned logString(const char *text) const override;
    virtual const IContextLogger &queryContextLogger() const override;
    virtual IDebuggableContext *queryDebugContext() const override;
    virtual IEngineRowAllocator * getRowAllocator(IOutputMetaData * meta, unsigned activityId) const override;
    virtual IEngineRowAllocator * getRowAllocatorEx(IOutputMetaData * meta, unsigned activityId, unsigned heapFlags) const override;
    virtual const char *cloneVString(const char *str) const override;
    virtual const char *cloneVString(size32_t len, const char *str) const override;
    virtual void getResultRowset(size32_t & tcount, const byte * * & tgt, const char * name, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) override;
    virtual void getResultDictionary(size32_t & tcount, const byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher) override;
    virtual void getRowXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags) override;
    void getRowJSON(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags) override;
    virtual const void * fromXml(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace) override;
    virtual const void * fromJson(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace) override;
    virtual IEngineContext *queryEngineContext() override;
    virtual char *getDaliServers() override;
    virtual IWorkUnit *updateWorkUnit() const override;
    virtual ISectionTimer * registerTimer(unsigned activityId, const char * name);
    virtual unsigned getGraphLoopCounter() const override;
    virtual void addWuExceptionEx(const char * text, unsigned code, unsigned severity, unsigned audience, const char *source) override;
    virtual unsigned getElapsedMs() const override;
    virtual unsigned getWorkflowId() const override;
};

class ECLRTL_API GlobalCodeContextExtra : implements IGlobalCodeContext
{
private:
    // Wraps ICodeContext and overrides getWorkflowId() method
    class CodeContextEx : public IndirectCodeContext
    {
        unsigned wfid;
    public:
        CodeContextEx(ICodeContext * _ctx, unsigned _wfid) : IndirectCodeContext(_ctx), wfid(_wfid) {}
        virtual unsigned getWorkflowId() const override { return wfid; }
    } codeContextEx;
    IGlobalCodeContext * gctx;
public:
    GlobalCodeContextExtra(IGlobalCodeContext * _gctx, unsigned _wfid) : codeContextEx(_gctx->queryCodeContext(), _wfid), gctx(_gctx) {}
    virtual ICodeContext * queryCodeContext() override;
    virtual void fail(int code, const char *msg) override;
    virtual bool isResult(const char * name, unsigned sequence) override;
    virtual unsigned getWorkflowIdDeprecated() override;
    virtual void doNotify(char const * name, char const * text);
    virtual int queryLastFailCode() override;
    virtual void getLastFailMessage(size32_t & outLen, char * & outStr, const char * tag);
    virtual bool fileExists(const char * filename) override;
    virtual void deleteFile(const char * logicalName) override;
    virtual void selectCluster(const char * cluster) override;
    virtual void restoreCluster() override;
    virtual void setWorkflowCondition(bool value) override;
    virtual void returnPersistVersion(const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile) override;
    virtual void setResultDataset(const char * name, unsigned sequence, size32_t len, const void *val, unsigned numRows, bool extend) override;
    virtual void getEventName(size32_t & outLen, char * & outStr) override;
    virtual void getEventExtra(size32_t & outLen, char * & outStr, const char * tag) override;
    virtual void doNotify(char const * name, char const * text, const char * target) override;
};

#endif
