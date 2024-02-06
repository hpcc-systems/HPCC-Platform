#include "platform.h"
#include "wfcontext.hpp"

const char *IndirectCodeContext::loadResource(unsigned id)
{
    return ctx->loadResource(id);
}

void IndirectCodeContext::setResultBool(const char *name, unsigned sequence, bool value)
{
    ctx->setResultBool(name, sequence, value);
}

void IndirectCodeContext::setResultData(const char *name, unsigned sequence, int len, const void * data)
{
    ctx->setResultData(name, sequence, len, data);
}

void IndirectCodeContext::setResultDecimal(const char * stepname, unsigned sequence, int len, int precision, bool isSigned, const void *val)
{
    ctx->setResultDecimal(stepname, sequence, len, precision, isSigned, val);
}

void IndirectCodeContext::setResultInt(const char *name, unsigned sequence, __int64 value, unsigned size)
{
    ctx->setResultInt(name, sequence, value, size);
}

void IndirectCodeContext::setResultRaw(const char *name, unsigned sequence, int len, const void * data)
{
    ctx->setResultRaw(name, sequence, len, data);
}

void IndirectCodeContext::setResultReal(const char * stepname, unsigned sequence, double value)
{
    ctx->setResultReal(stepname, sequence, value);
}

void IndirectCodeContext::setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer)
{
    ctx->setResultSet(name, sequence, isAll, len, data, transformer);
}

void IndirectCodeContext::setResultString(const char *name, unsigned sequence, int len, const char * str)
{
    ctx->setResultString(name, sequence, len, str);
}

void IndirectCodeContext::setResultUInt(const char *name, unsigned sequence, unsigned __int64 value, unsigned size)
{
    ctx->setResultUInt(name, sequence, value, size);
}

void IndirectCodeContext::setResultUnicode(const char *name, unsigned sequence, int len, UChar const * str)
{
    ctx->setResultUnicode(name, sequence, len, str);
}

void IndirectCodeContext::setResultVarString(const char * name, unsigned sequence, const char * value)
{
    ctx->setResultVarString(name, sequence, value);
}

void IndirectCodeContext::setResultVarUnicode(const char * name, unsigned sequence, UChar const * value)
{
    ctx->setResultVarUnicode(name, sequence, value);
}

bool IndirectCodeContext::getResultBool(const char * name, unsigned sequence)
{
    return ctx->getResultBool(name, sequence);
}

void IndirectCodeContext::getResultData(unsigned & tlen, void * & tgt, const char * name, unsigned sequence)
{
    ctx->getResultData(tlen, tgt, name, sequence);
}

void IndirectCodeContext::getResultDecimal(unsigned tlen, int precision, bool isSigned, void * tgt, const char * stepname, unsigned sequence)
{
    ctx->getResultDecimal(tlen, precision, isSigned, tgt, stepname, sequence);
}

void IndirectCodeContext::getResultRaw(unsigned & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
{
    ctx->getResultRaw(tlen, tgt, name, sequence, xmlTransformer, csvTransformer);
}

void IndirectCodeContext::getResultSet(bool & isAll, size32_t & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
{
    ctx->getResultSet(isAll, tlen, tgt, name, sequence, xmlTransformer, csvTransformer);
}

__int64 IndirectCodeContext::getResultInt(const char * name, unsigned sequence)
{
    return ctx->getResultInt(name, sequence);
}

double IndirectCodeContext::getResultReal(const char * name, unsigned sequence)
{
    return ctx->getResultReal(name, sequence);
}

void IndirectCodeContext::getResultString(unsigned & tlen, char * & tgt, const char * name, unsigned sequence)
{
    ctx->getResultString(tlen, tgt, name, sequence);
}

void IndirectCodeContext::getResultStringF(unsigned tlen, char * tgt, const char * name, unsigned sequence)
{
    ctx->getResultStringF(tlen, tgt, name, sequence);
}

void IndirectCodeContext::getResultUnicode(unsigned & tlen, UChar * & tgt, const char * name, unsigned sequence)
{
    ctx->getResultUnicode(tlen, tgt, name, sequence);
}

char *IndirectCodeContext::getResultVarString(const char * name, unsigned sequence)
{
    return ctx->getResultVarString(name, sequence);
}

UChar *IndirectCodeContext::getResultVarUnicode(const char * name, unsigned sequence)
{
    return ctx->getResultVarUnicode(name, sequence);
}

unsigned IndirectCodeContext::getResultHash(const char * name, unsigned sequence)
{
    return ctx->getResultHash(name, sequence);
}

unsigned IndirectCodeContext::getExternalResultHash(const char * wuid, const char * name, unsigned sequence)
{
    return ctx->getExternalResultHash(wuid, name, sequence);
}

char *IndirectCodeContext::getWuid()
{
    return ctx->getWuid();
}

void IndirectCodeContext::getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
{
    ctx->getExternalResultRaw(tlen, tgt, wuid, stepname, sequence, xmlTransformer, csvTransformer);
}

void IndirectCodeContext::executeGraph(const char * graphName, bool realThor, size32_t parentExtractSize, const void * parentExtract)
{
    ctx->executeGraph(graphName, realThor, parentExtractSize, parentExtract);
}

char * IndirectCodeContext::getExpandLogicalName(const char * logicalName)
{
    return ctx->getExpandLogicalName(logicalName);
}

void IndirectCodeContext::addWuException(const char * text, unsigned code, unsigned severity, const char *source)
{
    ctx->addWuException(text, code, severity, source);
}

void IndirectCodeContext::addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort)
{
    ctx->addWuAssertFailure(code, text, filename, lineno, column, isAbort);
}

IUserDescriptor *IndirectCodeContext::queryUserDescriptor()
{
    return ctx->queryUserDescriptor();
}

IThorChildGraph * IndirectCodeContext::resolveChildQuery(__int64 activityId, IHThorArg * colocal)
{
    return ctx->resolveChildQuery(activityId, colocal);
}

unsigned __int64 IndirectCodeContext::getDatasetHash(const char * name, unsigned __int64 hash)
{
    return ctx->getDatasetHash(name, hash);
}

unsigned IndirectCodeContext::getNodes()
{
    return ctx->getNodes();
}

unsigned IndirectCodeContext::getNodeNum()
{
    return ctx->getNodeNum();
}

char *IndirectCodeContext::getFilePart(const char *logicalPart, bool create)
{
    return ctx->getFilePart(logicalPart, create);
}

unsigned __int64 IndirectCodeContext::getFileOffset(const char *logicalPart)
{
    return ctx->getFileOffset(logicalPart);
}

IDistributedFileTransaction *IndirectCodeContext::querySuperFileTransaction()
{
    return ctx->querySuperFileTransaction();
}

char *IndirectCodeContext::getEnv(const char *name, const char *defaultValue) const
{
    return ctx->getEnv(name, defaultValue);
}

char *IndirectCodeContext::getJobName()
{
    return ctx->getJobName();
}

char *IndirectCodeContext::getJobOwner()
{
    return ctx->getJobOwner();
}

char *IndirectCodeContext::getClusterName()
{
    return ctx->getClusterName();
}

char *IndirectCodeContext::getGroupName()
{
    return ctx->getGroupName();
}

char *IndirectCodeContext::queryIndexMetaData(char const * lfn, char const * xpath)
{
    return ctx->queryIndexMetaData(lfn, xpath);
}

unsigned IndirectCodeContext::getPriority() const
{
    return ctx->getPriority();
}

char *IndirectCodeContext::getPlatform()
{
    return ctx->getPlatform();
}

char *IndirectCodeContext::getOS()
{
    return ctx->getOS();
}

IEclGraphResults *IndirectCodeContext::resolveLocalQuery(__int64 activityId)
{
    return ctx->resolveLocalQuery(activityId);
}

char *IndirectCodeContext::getEnv(const char *name, const char *defaultValue)
{
    return ctx->getEnv(name, defaultValue);
}

unsigned IndirectCodeContext::logString(const char *text) const
{
    return ctx->logString(text);
}

const IContextLogger &IndirectCodeContext::queryContextLogger() const
{
    return ctx->queryContextLogger();
}

IDebuggableContext *IndirectCodeContext::queryDebugContext() const
{
    return ctx->queryDebugContext();
}

IEngineRowAllocator *IndirectCodeContext::getRowAllocator(IOutputMetaData * meta, unsigned activityId) const
{
    return ctx->getRowAllocator(meta, activityId);
}

IEngineRowAllocator *IndirectCodeContext::getRowAllocatorEx(IOutputMetaData * meta, unsigned activityId, unsigned heapFlags) const
{
    return ctx->getRowAllocatorEx(meta, activityId, heapFlags);
}

const char *IndirectCodeContext::cloneVString(const char *str) const
{
    return ctx->cloneVString(str);
}

const char *IndirectCodeContext::cloneVString(size32_t len, const char *str) const
{
    return ctx->cloneVString(len, str);
}

void IndirectCodeContext::getResultRowset(size32_t & tcount, const byte * * & tgt, const char * name, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
{
    ctx->getResultRowset(tcount, tgt, name, sequence, _rowAllocator, isGrouped, xmlTransformer, csvTransformer);
}

void IndirectCodeContext::getResultDictionary(size32_t & tcount, const byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher)
{
    ctx->getResultDictionary(tcount, tgt, _rowAllocator, name, sequence, xmlTransformer, csvTransformer, hasher);
}

void IndirectCodeContext::getRowXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags)
{
    ctx->getRowXML(lenResult, result, info, row, flags);
}

void IndirectCodeContext::getRowJSON(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags)
{
    ctx->getRowJSON(lenResult, result, info, row, flags);
}

const void *IndirectCodeContext::fromXml(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
{
    return ctx->fromXml(_rowAllocator, len, utf8, xmlTransformer, stripWhitespace);
}

const void *IndirectCodeContext::fromJson(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
{
    return ctx->fromJson(_rowAllocator, len, utf8, xmlTransformer, stripWhitespace);
}

IEngineContext *IndirectCodeContext::queryEngineContext()
{
    return ctx->queryEngineContext();
}

char *IndirectCodeContext::getDaliServers()
{
    return ctx->getDaliServers();
}

IWorkUnit *IndirectCodeContext::updateWorkUnit() const
{
    return ctx->updateWorkUnit();
}

ISectionTimer *IndirectCodeContext::registerTimer(unsigned activityId, const char * name)
{
    return ctx->registerTimer(activityId, name);
}

unsigned IndirectCodeContext::getGraphLoopCounter() const
{
    return ctx->getGraphLoopCounter();
}

void IndirectCodeContext::addWuExceptionEx(const char * text, unsigned code, unsigned severity, unsigned audience, const char *source)
{
    ctx->addWuExceptionEx(text, code, severity, audience, source);
}

unsigned IndirectCodeContext::getElapsedMs() const
{
    return ctx->getElapsedMs();
}

unsigned IndirectCodeContext::getWorkflowId() const
{
    return ctx->getWorkflowId();
}

ICodeContext * GlobalCodeContextExtra::queryCodeContext()
{
    return &codeContextEx;
}

void GlobalCodeContextExtra::fail(int code, const char *msg)
{
    gctx->fail(code, msg);
}

bool GlobalCodeContextExtra::isResult(const char * name, unsigned sequence)
{
    return gctx->isResult(name, sequence);
}

unsigned GlobalCodeContextExtra::getWorkflowIdDeprecated()
{
    return gctx->getWorkflowIdDeprecated();
}

void GlobalCodeContextExtra::doNotify(char const * name, char const * text)
{
    gctx->doNotify(name, text);
}

int GlobalCodeContextExtra::queryLastFailCode()
{
    return gctx->queryLastFailCode();
}

void GlobalCodeContextExtra::getLastFailMessage(size32_t & outLen, char * & outStr, const char * tag)
{
    gctx->getLastFailMessage(outLen, outStr, tag);
}

bool GlobalCodeContextExtra::fileExists(const char * filename)
{
    return gctx->fileExists(filename);
}

void GlobalCodeContextExtra::deleteFile(const char * logicalName)
{
    gctx->deleteFile(logicalName);
}

void GlobalCodeContextExtra::selectCluster(const char * cluster)
{
    gctx->selectCluster(cluster);
}

void GlobalCodeContextExtra::restoreCluster()
{
    gctx->restoreCluster();
}

void GlobalCodeContextExtra::setWorkflowCondition(bool value)
{
    gctx->setWorkflowCondition(value);
}

void GlobalCodeContextExtra::returnPersistVersion(const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile)
{
    gctx->returnPersistVersion(logicalName, eclCRC, allCRC, isFile);
}

void GlobalCodeContextExtra::setResultDataset(const char * name, unsigned sequence, size32_t len, const void *val, unsigned numRows, bool extend)
{
    gctx->setResultDataset(name, sequence, len, val, numRows, extend);
}

void GlobalCodeContextExtra::getEventName(size32_t & outLen, char * & outStr)
{
    gctx->getEventName(outLen, outStr);
}

void GlobalCodeContextExtra::getEventExtra(size32_t & outLen, char * & outStr, const char * tag)
{
    gctx->getEventExtra(outLen, outStr, tag);
}

void GlobalCodeContextExtra::doNotify(char const * name, char const * text, const char * target)
{
    gctx->doNotify(name, text, target);
}
