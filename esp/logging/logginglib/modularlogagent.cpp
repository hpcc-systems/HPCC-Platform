/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#include "modularlogagent.ipp"

using namespace TraceLoggingPriority;

namespace ModularLogAgent
{

/**
 * Reads a number of bytes from a string of form
 *   [ whitespace ] number [ [ whitespace ] ( 'k' | 'K' | 'm' | 'M' | 'g' | 'G' | 't' | 'T' ) ]
 * Extraction fails if:
 *   - the numeric value is omitted
 *   - the numeric value cannot be contained by templated container type
 *   - an invalid unit designator is given
 *   - the numeric value multiplied by the unit multiplier exceeds the container capacity
 *
 * The implementation is inspired by CLogFailSafe::readSafeRolloverThresholdCfg, with the
 * addition of more robust input validation. It is a stand-alone function that could be
 * relocated and reused to harden similar use cases.
 */
template <typename threshold_t>
bool extractSize(const char* text, threshold_t& threshold)
{
    threshold_t quantity = 0;
    uint64_t    unitMultiplier = 1;
    if (text)
    {
        while (isspace(*text)) text++;
        if (*text)
        {
            const char* quantityPtr = text;
            while (isdigit(*text)) text++;
            StringBuffer quantityBuf(size32_t(text - quantityPtr), quantityPtr);
            if (TokenDeserializer()(quantityBuf, quantity) != Deserialization_SUCCESS)
                return false; // no number small enough for threshold_t found
            while (isspace(*text)) text++;
            switch (toupper(*text))
            {
            case '\0':
                break;
            case 'K':
                unitMultiplier = 1000;
                break;
            case 'M':
                unitMultiplier = 1000000;
                break;
            case 'G':
                unitMultiplier = 1000000000;
                break;
            case 'T':
                unitMultiplier = 1000000000000;
                break;
            default:
                return false; // unrecognized unit
            }
            threshold_t maxSize = std::numeric_limits<threshold_t>::max();
            if (unitMultiplier > maxSize || (maxSize / threshold_t(unitMultiplier) < quantity))
                return false; // the number of bytes overflows threshold_t
        }
    }
    threshold = quantity * threshold_t(unitMultiplier);
    return true;
}

#if defined(__clang__) || defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
#endif

/**
 * Evaluates a format string for compatibility with strftime. Failure prevents the string from
 * being used with strftime.
 */
bool checkTimestampFormat(const char* format, std::set<char>* modifiers)
{
    bool result = true;
    if (format)
    {
        while (result && *format)
        {
            if ('%' == *format)
            {
                switch (*(format + 1))
                {
                case '\0':
                    result = false;
                    break;
                case '%':
                    format += 1; // consumed one additional character
                    break;
                case 'E':
                    if (*(format + 2) && !!strchr("cCxXyY", *(format + 2)))
                    {
                        if (modifiers)
                            modifiers->insert(*(format + 2));
                        format += 2; // consumed two additional characters
                    }
                    else
                        result = false;
                    break;
                case 'O':
                    if (*(format + 2) && !!strchr("deHImMSuUVwWy", *(format + 2)))
                    {
                        if (modifiers)
                            modifiers->insert(*(format + 2));
                        format += 2; // consumed two additional characters
                    }
                    else
                        result = false;
                    break;
                default:
                    if (!!strchr("aAbBcCdDeFgGhHIJmMnprRStTuUVwWxXyY", *(format + 1)))
                    {
                        if (modifiers)
                            modifiers->insert(*(format + 1));
                        format += 1; // consumed one additional character
                    }
                    else
                        result = false;
                    break;
                }
            }
            format++;
        }
    }
    else
        result = false;
    return result;
}
inline bool checkTimestampFormat(const char* format)
{
    return checkTimestampFormat(format, nullptr);
}

/**
 * Create a timestamp string based on a given time. The function uses strftime with a non-literal
 * format string. This normally generates a warning, "-Wformat-nonliteral", that is treated as an
 * error. This warning must be temporarily ignored for the function to compile.
 */
inline StringBuffer& makeTimestamp(struct tm& when, const char* format, StringBuffer& timestamp)
{
    size_t capacity = 100;
    char* buffer = (char*)malloc(capacity);
    size_t length = 0;
    byte tries = 0;
    while (buffer && (length = strftime(buffer, capacity, format, &when)) == 0 && ++tries <= 3)
    {
        capacity *= 2;
        char* tmp = (char*)realloc(buffer, capacity);
        if (!tmp)
        {
            free(buffer);
            buffer = nullptr;
        }
        buffer = tmp;
    }
    if (length)
        timestamp.append(buffer);
    free(buffer);
    return timestamp;
}

/**
 * Create a timestamp string based on the current UTC time.
 */
inline StringBuffer& makeTimestamp(const char* format, StringBuffer& timestamp)
{
    time_t nowTime;
    time(&nowTime);
    struct tm nowTm;
    struct tm* nowInfo = gmtime_r(&nowTime, &nowTm);
    return makeTimestamp(*gmtime_r(&nowTime, &nowTm), format, timestamp);
}

#if defined(__clang__) || defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

//////////////////// CModule ////////////////////

LogMsgDetail CModule::tracePriorityLimit(const LogMsgCategory& category) const
{
    return (m_inheritTracePriorityLimit ? (getEspLogLevel() * 10) : m_tracePriorityLimit);
}

const char* CModule::traceId() const
{
    return m_traceId;
}

void CModule::traceOutput(const LogMsgCategory& category, const char* format, va_list& arguments) const
{
    StringBuffer baseMsg;
    baseMsg.valist_appendf(format, arguments);
    LOG(category, "%s: %s", traceId(), baseMsg.str());
}

bool CModule::configure(const IPTree& configuration, const CModuleFactory& factory)
{
    static const int INHERIT_TRACE_PRIORITY_LIMIT = -1;
    bool result = true;

    // Neither CModule nor TModule<> are logging components, but implementations of IModule will be.
    // Enable the common base(s) to use the derived logging methods.
    m_self = dynamic_cast<ITraceLoggingComponent*>(this);

    m_traceId.append(configuration.queryName());
    const char* name = configuration.queryProp(propName);
    const char* module = configuration.queryProp(IModule::propFactoryKey);
    if (!isEmptyString(name))
    {
        if (module)
            m_traceId.appendf("[%s:%s]", name, module);
        else
            m_traceId.appendf("[%s]", name);
    }
    else if (module)
        m_traceId.appendf("[%s]", module);

    // Determine the desired trace log output level. An inherited value is not pre-set because
    // the inherited value might change at runtime.
    int tracePriorityLimit = configuration.getPropInt(propTracePriorityLimit, INHERIT_TRACE_PRIORITY_LIMIT);
    if (INHERIT_TRACE_PRIORITY_LIMIT == tracePriorityLimit)
    {
        m_inheritTracePriorityLimit = true;
    }
    else
    {
        m_inheritTracePriorityLimit = false;
        m_tracePriorityLimit = LogMsgDetail(tracePriorityLimit);
    }

    // Other initialization
    m_disabled = configuration.getPropBool(propDisabled, m_disabled);

    return true;
}

bool CModule::isEnabled() const
{
    return !m_disabled;
}

StringBuffer& CModule::toString(StringBuffer& str) const
{
    str.appendf("%s(", traceId());
    appendProperties(str);
    str.append(")");
    return str;
}

bool CModule::appendProperties(StringBuffer& str) const
{
    str.appendf("disabled: %d; trace-priority-limit: %d", !isEnabled(), (m_inheritTracePriorityLimit ? -1 : int(m_tracePriorityLimit)));
    return true;
}

//////////////////// CMockAgent ////////////////////

bool CMockAgent::configure(const IPTree& configuration, const CModuleFactory& factory)
{
    bool result = Base::configure(configuration, factory);
    const IPTree* serviceBranch = configuration.queryBranch("GetTransactionSeed");
    if (serviceBranch)
    {
        m_gtsResponse.setown(createGetTransactionSeedResponse());
        m_gtsResponse->setSeedId(serviceBranch->queryProp("@seed-id"));
        m_gtsResponse->setStatusCode(serviceBranch->getPropInt("@status-code", -1));
        m_gtsResponse->setStatusMessage(serviceBranch->queryProp("@status-message"));
    }
    serviceBranch = configuration.queryBranch("GetTransactionId");
    if (serviceBranch)
    {
        m_gtiResponse.setown(new String(serviceBranch->queryProp("@id")));
    }
    serviceBranch = configuration.queryBranch("UpdateLog");
    if (serviceBranch)
    {
        m_ulResponse.setown(createUpdateLogResponse());
        m_ulResponse->setResponse(serviceBranch->queryProp("@response"));
        m_ulResponse->setStatusCode(serviceBranch->getPropInt("@status-code", -1));
        m_ulResponse->setStatusMessage(serviceBranch->queryProp("@status-message"));
    }
    return result;
}

bool CMockAgent::getTransactionSeed(IEspGetTransactionSeedRequest& request, IEspGetTransactionSeedResponse& response)
{
    if (m_gtsResponse)
    {
        response.setSeedId(m_gtsResponse->getSeedId());
        response.setStatusCode(m_gtsResponse->getStatusCode());
        response.setStatusMessage(m_gtsResponse->getStatusMessage());
    }
    else
    {
        response.setStatusCode(-1);
        response.setStatusMessage("unsupported request (getTransactionSeed)");
    }
    return (response.getStatusCode() == 0);
}

void CMockAgent::getTransactionID(StringAttrMapping* fields, StringBuffer& id)
{
    if (m_gtiResponse)
        id.set(m_gtiResponse->str());
    else
        id.set("unsupported request (getTransactionID)");
}

void CMockAgent::updateLog(IEspUpdateLogRequestWrap& request, IEspUpdateLogResponse& response)
{
    if (m_ulResponse)
    {
        response.setResponse(m_ulResponse->getResponse());
        response.setStatusCode(m_ulResponse->getStatusCode());
        response.setStatusMessage(m_ulResponse->getStatusMessage());
    }
    else
    {
        response.setStatusCode(-1);
        response.setStatusMessage("unsupported request (updateLog)");
    }
}

bool CMockAgent::hasService(LOGServiceType type) const
{
    switch (type)
    {
    case LGSTGetTransactionSeed:
        return m_gtsResponse != nullptr;
    case LGSTGetTransactionID:
        return m_gtiResponse != nullptr;
    case LGSTUpdateLOG:
        return m_ulResponse != nullptr;
    default:
        return false;
    }
}

//////////////////// CDelegatingAgent ////////////////////

bool CDelegatingAgent::configure(const IPTree& configuration, const CModuleFactory& factory)
{
    bool result = Base::configure(configuration, factory);

    if (!configureUpdateLog(configuration, factory))
        result = false;

    if (result && !hasService(LGSTGetTransactionSeed) && !hasService(LGSTGetTransactionID) && !hasService(LGSTUpdateLOG))
        uwarnlog(Highest, "no available log agent services");

    return result;
}

bool CDelegatingAgent::getTransactionSeed(IEspGetTransactionSeedRequest& request, IEspGetTransactionSeedResponse& response)
{
    return false;
}

void CDelegatingAgent::getTransactionID(StringAttrMapping* fields, StringBuffer& id)
{
}

void CDelegatingAgent::updateLog(IEspUpdateLogRequestWrap& request, IEspUpdateLogResponse& response)
{
    if (!m_updateLog)
    {
        ierrlog(Major, "UpdateLog service module does not exist");
        response.setStatusCode(-1);
    }
    else if (!m_updateLog->isEnabled())
    {
        m_updateLog->updateLog(nullptr, response);
        response.setStatusCode(-1);
    }
    else
    {
        const char* updateLogRequest = request.getUpdateLogRequest();
        if (!isEmptyString(updateLogRequest))
        {
            m_updateLog->updateLog(updateLogRequest, response);
        }
        else
        {
            IPTree* ulrTree = request.getLogRequestTree();
            if (ulrTree)
            {
                StringBuffer xml;
                toXML(ulrTree, xml);
                m_updateLog->updateLog(xml, response);
            }
            else
            {
                m_updateLog->ierrlog(Major, "requires XML markup; none given");
                response.setStatusCode(-1);
            }
        }
    }
}

bool CDelegatingAgent::hasService(LOGServiceType type) const
{
    switch (type)
    {
    case LGSTGetTransactionSeed: return false;
    case LGSTGetTransactionID:   return false;
    case LGSTUpdateLOG:          return m_updateLog && m_updateLog->isEnabled();
    case LGSTterm:
        ierrlog(Major, "invalid service request for LGSTterm");
        break;
    default:
        ierrlog(Major, "unexpected service request for %d", int(type));
        break;
    }
    return false;
}

bool CDelegatingAgent::configureUpdateLog(const IPTree& configuration, const CModuleFactory& factory)
{
    static Owned<IPTree> defaultConfiguration(createPTreeFromXMLString(VStringBuffer("<%s><%s/></%s>", moduleAgent, moduleUpdateLog, moduleAgent)));
    Linked<const IPTree> effectiveConfiguration;
    const char* updateLogValue = configuration.queryProp(propUpdateLog);
    bool hasScalar = !isEmptyString(updateLogValue);
    bool hasObject = configuration.hasProp(moduleUpdateLog);
    if (hasScalar && hasObject)
    {
        uerrlog(Major, "%s and %s cannot be configured at the same time", propUpdateLog, moduleUpdateLog);
        return false;
    }
    if (hasScalar)
    {
        TokenDeserializer deserializer;
        bool applyDefault = false;
        if (deserializer(updateLogValue, applyDefault) != Deserialization_SUCCESS)
        {
            uerrlog(Major, "expected Boolean value for %s; found '%s'", propUpdateLog, updateLogValue);
            return false;
        }
        if (!applyDefault)
            return true;
        return createAndConfigure(*defaultConfiguration, moduleUpdateLog, factory, factory.m_updateLogs, m_updateLog, *this);
    }
    return createAndConfigure(configuration, moduleUpdateLog, factory, factory.m_updateLogs, m_updateLog, *this);
}

bool CDelegatingAgent::appendProperties(StringBuffer& str) const
{
    bool appended = Base::appendProperties(str);
    bool first = appended;
    if (m_updateLog)
    {
        if (first)
        {
            str.append("; ");
            first = false;
        }
        m_updateLog->toString(str);
        appended = true;
    }
    return appended;
}

//////////////////// CDelegatingUpdateLog ////////////////////

bool CDelegatingUpdateLog::configure(const IPTree& configuration, const CModuleFactory& factory)
{
    bool result = Base::configure(configuration, factory);
    if (!createAndConfigure(configuration, moduleTarget, factory, factory.m_contentTargets, m_target, *this))
        result = false;
    return result;
}

void CDelegatingUpdateLog::updateLog(const char* updateLogRequest, IEspUpdateLogResponse& response)
{
    if (!isEnabled())
    {
        StringBuffer msg;
        msg.appendf("service is disabled");
        response.setStatusCode(-1);
        response.setStatusMessage(msg);
        ierrlog(Major, "%s", msg.str());
    }
    else if (isEmptyString(updateLogRequest))
    {
        response.setStatusCode(0);
        iwarnlog(Highest, "no content provided");
    }
    else
    {
        response.setStatusCode(0); // assume success
        try
        {
            Owned<IEsdlScriptContext> scriptContext(createEsdlScriptContext(nullptr));
            scriptContext->setContent(IContentTarget::sectionOriginal, updateLogRequest);
            Owned<IXpathContext> originalContent(scriptContext->createXpathContext(nullptr, IContentTarget::sectionOriginal, true));
            Owned<IXpathContext> intermediateContent;
            StringAttr finalContent;

            // TODO: apply optional custom transforms to produce intermediateContent

            // TODO: apply optional XSLT transform to produce finalContent

            if (m_target && m_target->isEnabled())
            {
                m_target->updateTarget(*scriptContext, *originalContent, intermediateContent, finalContent, response);
            }
            else if (DataDump <= tracePriorityLimit(MSGAUD_user, MSGCLS_progress))
            {
                VStringBuffer output("log content with %s target:\n  original content:\n", (m_target ? "disabled" : "no"));
                scriptContext->toXML(output, IContentTarget::sectionOriginal);
                if (intermediateContent)
                {
                    output.append("\n  intermediate content:\n");
                    scriptContext->toXML(output, IContentTarget::sectionIntermediate);
                }
                if (finalContent.get())
                {
                    output.appendf("\n  final content:\n%s", finalContent.str());
                }
                uproglog(DataDump, "%s", output.str());
            }
        }
        catch(IException* e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            response.setStatusCode(e->errorCode());
            response.setStatusMessage(msg);
            e->Release();
        }
    }
}

bool CDelegatingUpdateLog::appendProperties(StringBuffer& str) const
{
    bool appended = Base::appendProperties(str);
    bool first = appended;
    if (m_target)
    {
        if (first)
        {
            str.append("; ");
            first = false;
        }
        m_target->toString(str);
        appended = true;
    }
    return true;
}

//////////////////// CContentTarget ////////////////////

void CContentTarget::updateTarget(IEsdlScriptContext& scriptContext, IXpathContext& originalContent, IXpathContext* intermediateContent, const char* finalContent, IEspUpdateLogResponse& response) const
{
    TRACE_BLOCK("CContentTarget::updateTarget");
    if (DataDump <= tracePriorityLimit(MSGAUD_programmer, MSGCLS_information))
    {
        StringBuffer output("updateTarget\n  original content:\n");
        scriptContext.toXML(output, sectionOriginal);
        if (intermediateContent)
        {
            output.append("\n  intermediate content:\n");
            scriptContext.toXML(output, sectionIntermediate);
        }
        if (finalContent)
        {
            output.appendf("\n  final content:\n%s", finalContent);
        }
        iinfolog(DataDump, "%s", output.str());
    }
}

//////////////////// CContentTarget ////////////////////

bool CFileTarget::Pattern::VariableFragment::matches(const char* name, const char* option) const
{
    if (isEmptyString(name))
        return false;
    if (!streq(m_name.c_str(), name))
        return false;
    return (!option || streq(m_option.c_str(), option));
}

bool CFileTarget::Pattern::VariableFragment::resolvedBy(const Variables& variables) const
{
    return variables.find(m_name) != variables.end();
}

StringBuffer& CFileTarget::Pattern::VariableFragment::toString(StringBuffer& pattern, const Variables* variables) const
{
    if (variables)
    {
        Variables::const_iterator it = variables->find(m_name);
        if (it != variables->end())
        {
            m_pattern.m_target.resolveVariable(m_name.c_str(), (m_withOption ? m_option.c_str() : nullptr), it->second.c_str(), pattern);
            return pattern;
        }
    }
    if (m_withOption)
    {
        pattern.appendf("{$%s:%s}", m_name.c_str(), m_option.c_str());
    }
    else
    {
        pattern.appendf("{$%s}", m_name.c_str());
    }
    return pattern;
}

CFileTarget::Pattern::VariableFragment* CFileTarget::Pattern::VariableFragment::clone(const Pattern& pattern) const
{
    Owned<VariableFragment> dup(new VariableFragment(pattern));
    dup->m_name = m_name;
    dup->m_option = m_option;
    dup->m_withOption = m_withOption;
    return dup.getClear();
}

bool CFileTarget::Pattern::contains(const char* name, const char* option) const
{
    for (const Owned<IFragment>& f : m_fragments)
    {
        if (f->matches(name, option))
            return true;
    }
    return false;
}

void CFileTarget::Pattern::appendText(const char* begin, const char* end)
{
    Owned<TextFragment> fragment(new TextFragment(*this));
    fragment->startOffset = m_textCache.length();
    m_textCache.append(size32_t(end - begin), begin);
    fragment->endOffset = m_textCache.length();
    m_fragments.push_back(fragment.getClear());
}

bool CFileTarget::Pattern::setPattern(const char* pattern)
{
    bool result = true;
    m_textCache.clear();
    m_fragments.clear();
    if (!isEmptyString(pattern))
    {
        const char* tmp = pattern;
        const char* fragmentStart = tmp;
        bool inVar = false;
        bool inVarName = false;
        bool inVarOption = true;
        StringBuffer varName;
        StringBuffer varOption;
        while (*tmp)
        {
            switch (*tmp)
            {
            case '{':
                if (inVar)
                {
                    m_target.uerrlog(Major, "file path pattern '%s' contains invalid nested variable markup at offset %zu", pattern, (tmp - pattern));
                    result = false;
                }
                else if ('$' == *(tmp + 1))
                {
                    if (fragmentStart < tmp)
                        appendText(fragmentStart, tmp);
                    inVar = true;
                    inVarName = true;
                    inVarOption = false;
                    tmp++;
                }
                break;
            case ':':
                if (inVar)
                {
                    if (inVarOption)
                    {
                        m_target.uwarnlog(Highest, "file path pattern '%s' contains unexpected option delimiter at offset %zu", pattern, (tmp - pattern));
                    }
                    else if (inVarName)
                    {
                        inVarName = false;
                        inVarOption = true;
                    }
                }
                break;
            case '}':
                if (inVar)
                {
                    if (varName.trim().isEmpty())
                    {
                        m_target.uerrlog(Major, "file path pattern '%s' contains empty variable name at offset %zu", pattern, (fragmentStart - pattern));
                        result = false;
                    }
                    else
                    {
                        Owned<VariableFragment> fragment(new VariableFragment(*this));
                        fragment->m_name.append(varName);
                        if (inVarOption)
                        {
                            varOption.trim();
                            if (!m_target.validateVariable(varName, varOption))
                            {
                                m_target.uerrlog(Major, "file path pattern '%s' contains invalid variable reference (%s:%s) at offset %zu", pattern, varName.str(), varOption.str(), (fragmentStart - pattern));
                                result = false;
                            }
                            else
                            {
                                fragment->m_withOption = true;
                                fragment->m_option.append(varOption);
                                m_fragments.push_back(fragment.getClear());
                            }
                        }
                        else if (!m_target.validateVariable(varName, nullptr))
                        {
                            m_target.uerrlog(Major, "file path pattern '%s' contains invalid variable reference (%s) at offset %zu", pattern, varName.str(), (fragmentStart - pattern));
                            result = false;
                        }
                        else
                        {
                            m_fragments.push_back(fragment.getClear());
                        }
                    }
                    inVarName = false;
                    inVarOption = false;
                    varName.clear();
                    varOption.clear();
                    inVar = false;
                    fragmentStart = tmp + 1;
                }
                break;
            default:
                if (inVarName)
                    varName.append(*tmp);
                if (inVarOption)
                    varOption.append(*tmp);
                break;
            }
            tmp++;
        }
        if (fragmentStart < tmp)
            appendText(fragmentStart, tmp);
    }
    else
    {
        m_target.uerrlog(Major, "file path pattern must not be empty");
        result = false;
    }
    return result;
}

CFileTarget::Pattern* CFileTarget::Pattern::resolve(const Variables& variables) const
{
    Owned<Pattern> resolved(new Pattern(m_target));
    Owned<TextFragment> nextText;
    for (const Owned<IFragment>& f : m_fragments)
    {
        if (f->resolvedBy(variables))
        {
            if (!nextText)
            {
                nextText.setown(new TextFragment(*resolved));
                nextText->startOffset = resolved->m_textCache.length();
            }
            f->toString(resolved->m_textCache, &variables);
            nextText->endOffset = resolved->m_textCache.length();
        }
        else
        {
            if (nextText)
                resolved->m_fragments.push_back(nextText.getClear());
            Owned<IFragment> dup(f->clone(*resolved));
            if (dup)
                resolved->m_fragments.push_back(dup.getClear());
        }
    }
    if (nextText)
    {
        resolved->m_fragments.push_back(nextText.getClear());
    }
    return resolved.getClear();
}

StringBuffer& CFileTarget::Pattern::toString(StringBuffer& output, const Variables* variables) const
{
    for (const Owned<IFragment>& f : m_fragments)
        f->toString(output, variables);
    return output;
}

bool CFileTarget::configure(const IPTree& configuration, const CModuleFactory& factory)
{
    bool result = CContentTarget::configure(configuration, factory);

    if (!configureHeader(configuration))
        result = false;

    if (!configureCreationFormat(configuration, xpathCreationDateFormat, defaultCreationDateFormat, true, false, m_creationDateFormat))
        result = false;
    if (!configureCreationFormat(configuration, xpathCreationTimeFormat, defaultCreationTimeFormat, false, true, m_creationTimeFormat))
        result = false;
    if (!configureCreationFormat(configuration, xpathCreationDateTimeFormat, defaultCreationDateTimeFormat, true, true, m_creationDateTimeFormat))
        result = false;

    if (!configureFileHandling(configuration))
        result = false;

    if (!configurePattern(configuration))
        result = false;

    if (!configureDebugMode(configuration))
        result = false;

    return result;
}

void CFileTarget::updateTarget(IEsdlScriptContext& scriptContext, IXpathContext& originalContent, IXpathContext* intermediateContent, const char* finalContent, IEspUpdateLogResponse& response) const
{
    TRACE_BLOCK("CFileTarget::updateTarget");

    if (!m_debugMode && isEmptyString(finalContent))
    {
        uproglog(Normal, "ignoring update request with empty content");
        return;
    }

    Variables variables;
    readPatternVariables(scriptContext, originalContent, intermediateContent, variables);
    if (variables.find(creationVarName) != variables.end())
        iwarnlog(Critical, "variable %s should not be read from content", creationVarName);

    if (m_debugMode)
    {
        StringBuffer debugContent;
        scriptContext.toXML(debugContent);
        if (debugContent.charAt(debugContent.length() - 1) != '\n')
            debugContent.append('\n');
        if (!isEmptyString(finalContent))
        {
            debugContent.append(finalContent);
            if (debugContent.charAt(debugContent.length() - 1) != '\n')
                debugContent.append('\n');
        }
        updateFile(debugContent, variables, response);
    }
    else
    {
        updateFile(finalContent, variables, response);
    }
}

bool CFileTarget::appendProperties(StringBuffer& str) const
{
    if (CContentTarget::appendProperties(str))
        str << "; ";
    if (m_pattern)
    {
        str << "filepath: '";
        m_pattern->toString(str);
        str << "'; ";
    }
    str << "header-text: '" << m_header << "'; "
        << "rollover-interval: " << m_rolloverInterval << "; "
        << "rollover-size: " << m_rolloverSize << "; "
        << "format-creation-date: '" << m_creationDateFormat << "'; "
        << "format-creation-time: '" << m_creationTimeFormat << "'; "
        << "format-creation-datetime: '" << m_creationDateTimeFormat << "'; ";
    str.appendf("concurrent-files: %zu of %hhu", m_targets.size(), m_concurrentFiles);
    if (!m_targets.empty())
    {
        bool first = true;
        for (TargetMap::value_type& node : m_targets)
        {
            if (!node.second)
                continue;
            if (first)
            {
                str << " (";
                first = false;
            }
            else
                str << ", ";
            str << '\'' << node.second->m_filePath << '\'';
        }
        if (!first)
            str << ')';
    }
    return true;
}

void CFileTarget::updateFile(const char* content, const Variables& variables, IEspUpdateLogResponse& response) const
{
    size_t contentLength = strlen(content);
    if (contentLength > std::numeric_limits<offset_t>::max())
    {
        VStringBuffer body("update content length (%zu bytes) exceeds supported capacity", contentLength);
        ierrlog(Major, "%s", body.str());
        VStringBuffer msg("%s: %s", traceId(), body.str());
        response.setStatusCode(-1);
        response.setStatusMessage(msg);
        return;
    }

    StringBuffer failureTarget;
    bool updated = false;
    Owned<Pattern> pattern(m_pattern->resolve(variables));
    if (pattern)
    {
        StringBuffer key;
        pattern->toString(key, nullptr);

        Owned<File> file(getFile(key));
        if (file)
        {
            Variables creationVars;
            if (needNewFile(contentLength, *file, *pattern, creationVars) &&
                !createNewFile(*file, *pattern, creationVars))
            {
                failureTarget.append(key);
            }
            else
            {
                offset_t originalFileSize = file->m_io->size();
                offset_t remainingContent = contentLength;
                offset_t chunkOffset = 0;
                size32_t chunkSize = std::numeric_limits<size32_t>::max();
                while (remainingContent > chunkSize && writeChunk(*file, chunkOffset, chunkSize, content))
                {
                    chunkOffset += chunkSize;
                    remainingContent -= chunkSize;
                    if (remainingContent < chunkSize)
                        chunkSize = remainingContent;
                }
                if (!remainingContent)
                {
                    updated = true;
                }
                else if (remainingContent <= chunkSize && writeChunk(*file, chunkOffset, remainingContent, content))
                {
                    updated = true;
                }
                else if (file->m_io->size() != originalFileSize)
                {
                    failureTarget.append(file->m_filePath);
                    try
                    {
                        file->m_io->setSize(originalFileSize);
                        if (file->m_io->size() != originalFileSize)
                            ierrlog(Major, "failed to undo incomplete update to '%s'", failureTarget.str());

                    }
                    catch(IException* e)
                    {
                        StringBuffer msg;
                        ierrlog(Major, "exception undoing incomplete update to '%s': %s", failureTarget.str(), e->errorMessage(msg).str());
                        e->Release();
                    }
                }
                file->m_io->flush();
            }
        }
        else
            failureTarget.append(key);
    }
    else
        failureTarget.append("<unknown>");
    if (!updated)
    {
        VStringBuffer msg("%s: update to '%s' failed", traceId(), failureTarget.str());
        response.setStatusCode(-1);
        response.setStatusMessage(msg);
    }
}

void CFileTarget::readPatternVariables(IEsdlScriptContext& scriptContext, IXpathContext& originalContent, IXpathContext* intermediateContent, Variables& variables) const
{
    static Owned<ICompiledXpath> xpathBinding(compileXpath("//LogContent/ESPContext/ESDLBindingID"));
    static Owned<ICompiledXpath> xpathEsdlMethod(compileXpath("//LogContent/UserContext/Context/Row/Common/ESP/Config/Method/@name"));
    static Owned<ICompiledXpath> xpathService(compileXpath("//LogContent/UserContext/Context/Row/Common/ESP/ServiceName"));

    StringBuffer binding, esdlMethod, service;
    if (originalContent.evaluateAsString(xpathBinding, binding) && !binding.isEmpty())
    {
        variables[bindingVarName] = binding.str();
        StringArray parts;
        parts.appendList(binding, ".");
        if (parts.ordinality() == 3)
        {
            variables[processVarName] = parts[0];
            variables[portVarName] = parts[1];
            variables[esdlServiceVarName] = parts[2];
        }
        else
            uwarnlog(Highest, "unexpected format of %s value '%s'; variables %s, %s, and %s are undefined", bindingVarName, binding.str(), processVarName, portVarName, esdlServiceVarName);
    }
    else
        uwarnlog(Highest, "evaluation of xpath '%s' failed; variables %s, %s, %s, and %s are undefined", xpathBinding->getXpath(), bindingVarName, processVarName, portVarName, esdlServiceVarName);
    if (originalContent.evaluateAsString(xpathEsdlMethod, esdlMethod) && !esdlMethod.isEmpty())
    {
        variables[esdlMethodVarName] = esdlMethod.str();
    }
    else
        uwarnlog(Highest, "evaluation of xpath '%s' failed; variable %s is undefined", xpathEsdlMethod->getXpath(), esdlMethodVarName);
    if (originalContent.evaluateAsString(xpathService, service) && !service.isEmpty())
    {
        variables[serviceVarName] = service.str();
    }
    else
        uwarnlog(Highest, "evaluation of xpath '%s' failed; variable %s is undefined", xpathService->getXpath(), serviceVarName);
}

/**
 * Loads a text string to be written at the start of every file.
 */
bool CFileTarget::configureHeader(const IPTree& configuration)
{
    configuration.getProp(xpathHeader, m_header);
    return true;
}

/**
 * Sets up a single creation timestamp format value. The format of each value is expected to
 * be a strftime format string. If the value is expected to represent a date, the value must
 * contain date components. If the value is expected to represent a time, the value must
 * contain time components.
 */
bool CFileTarget::configureCreationFormat(const IPTree& configuration, const char* xpath, const char* defaultValue, bool checkDate, bool checkTime, StringBuffer& format)
{
    configuration.getProp(xpath, format);
    if (format.isEmpty())
    {
        format.append(defaultValue);
        if (format.isEmpty())
        {
            ierrlog(Major, "missing value for '%s'", xpath);
            return false;
        }
    }

    std::set<char> modifiers;
    bool result = checkTimestampFormat(format, &modifiers);
    if (result)
    {
        if (checkDate &&
            !(modifiers.count('Y') && modifiers.count('m') && modifiers.count('d')))
        {
            uerrlog(Major, "'%s' value '%s' missing required date modifiers", xpath, format.str());
            result = false;
        }
        if (checkTime &&
            !(modifiers.count('H') && modifiers.count('M') && modifiers.count('S')))
        {
            uerrlog(Major, "'%s' value '%s' missing required time modifiers", xpath, format.str());
            result = false;
        }
    }
    return result;
}

/**
 * Sets up properties used by the target to manage files, including:
 *   - concurrent files: the total number of open files the agent will maintain at one time
 *   - rollover interval: the frequency at which one file will be closed and another opened
 *     in its place
 *   - rollover size: the maximum size, in bytes, a log file should reach before being closed
 *     and replaced by another; the first write into an empty file is unconstrained
 */
bool CFileTarget::configureFileHandling(const IPTree& configuration)
{
    bool result = true;

    StringBuffer concurrentFiles(configuration.queryProp(xpathConcurrentFiles));
    if (concurrentFiles.isEmpty())
        concurrentFiles.append(defaultConcurrentFiles);
    if (TokenDeserializer()(concurrentFiles, m_concurrentFiles) != Deserialization_SUCCESS)
    {
        uerrlog(Major, "'%s' is an invalid value for '%s'", concurrentFiles.str(), xpathConcurrentFiles);
        result = false;
    }
    else if (0 == m_concurrentFiles)
    {
        uerrlog(Major, "'%s' cannot be zero", xpathConcurrentFiles);
        result = false;
    }

    const char* propValue = configuration.queryProp(xpathRolloverInterval);
    if (isEmptyString(propValue))
        propValue = defaultRolloverInterval;
    if (streq(propValue, rolloverInterval_Daily))
        m_rolloverInterval = DailyRollover;
    else if (streq(propValue, rolloverInterval_None))
        m_rolloverInterval = NoRollover;
    else
    {
        uerrlog(Major, "'%s' is an invalid value for '%s'", propValue, xpathRolloverInterval);
        m_rolloverInterval = UnknownRollover;
        result = false;
    }

    propValue = configuration.queryProp(xpathRolloverSize);
    if (isEmptyString(propValue))
        propValue = defaultRolloverSize;
    if (!extractSize(propValue, m_rolloverSize))
    {
        uerrlog(Major, "'%s' is an invalid value for '%s'", propValue, xpathRolloverSize);
        result = false;
    }

    return result;
}

/**
 * Sets up the file path pattern to be used by the target.
 */
bool CFileTarget::configurePattern(const IPTree& configuration)
{
    const char* propValue = configuration.queryProp(xpathFilePathPattern);
    if (isEmptyString(propValue))
    {
        uerrlog(Major, "'%s' is a required configuration value", xpathFilePathPattern);
        return false;
    }
    else
    {
        m_pattern.setown(new Pattern(*this));
        if (!m_pattern->setPattern(propValue))
            return false;
    }

    return true;
}

bool CFileTarget::configureDebugMode(const IPTree& configuration)
{
    m_debugMode = configuration.getPropBool(xpathDebug, m_debugMode);
    return true;
}

/**
 * Called by a pattern when parsing, validateVariable provides an opportunity for the target to
 * check that a variable reference is well formed. By default, creation timestamp options are
 * reviewed for proper formatting.
 */
bool CFileTarget::validateVariable(const char* name, const char* option) const
{
    bool valid = false;
    if (!isEmptyString(name))
    {
        if (streq(name, creationVarName))
        {
            if (isEmptyString(option) ||
                streq(option, creationVarDateOption) ||
                streq(option, creationVarTimeOption) ||
                streq(option, creationVarDateTimeOption) ||
                streq(option, creationVarCustomOption))
            {
                // Assume that the configuration used for the pre-defined options is
                // validated separately.
                valid = true;
            }
            else
            {
                std::set<char> modifiers;
                if (checkTimestampFormat(option, &modifiers))
                {
                    if (modifiers.empty())
                        uwarnlog(Highest, "variable '%s' option '%s' contains no timestamp modifiers", name, option);
                    valid = true;
                }
            }
        }
        else
        {
            if (option)
                uwarnlog(Highest, "variable '%s' references unused option '%s'", name, option);
            valid = true;
        }
    }
    return valid;
}

/**
 * Performs variable substition for a Pattern instance. Performing substitution within the
 * pattern requires a pattern subclass to support additional variables and a target subclass
 * to support the alternated pattern class. Shifting responsibility to the target limits the
 * additional overhead to only the target.
 *
 * File creation timestamp generation is supplied. All other variables use the given value.
 */
void CFileTarget::resolveVariable(const char* name, const char* option, const char* value, StringBuffer& output) const
{
    if (streq(name, creationVarName))
    {
        const char* format = nullptr;
        if (isEmptyString(option))
            format = creationVarDefaultOption;
        if (streq(option, creationVarDateTimeOption))
            format = m_creationDateTimeFormat;
        else if (streq(option, creationVarDateOption))
            format = m_creationDateFormat;
        else if (streq(option, creationVarTimeOption))
            format = m_creationTimeFormat;
        else if (streq(option, creationVarCustomOption))
            format = (isEmptyString(value) ? m_creationDateTimeFormat : value);
        else
            format = option;

        if (format)
        {
            StringBuffer timestamp;
            if (streq(makeTimestamp(format, timestamp), format))
            {
                if (streq(option, format))
                    uerrlog(Major, "'%s' is an invalid creation timestamp format string", format);
                else
                    uerrlog(Major, "'%s' (derived from option '%s') is an invalid creation timestamp format string", format, option);
            }
            output.append(timestamp);
        }
    }
    else
    {
        if (!isEmptyString(option))
            uinfolog(90, "variable substitution of variable '%s' ignoring option '%s'", name, option);
        if (value)
            output.append(value);
    }
}

/**
 * Obtains a file based on the given key value:
 *   - an existing file is always returned
 *   - if at least one additional file is permitted, a new file is created without changing
 *     existing targets
 *   - until a new file is permitted, the least recently used file is closed; a new file
 *     is created when allowed
 */
CFileTarget::File* CFileTarget::getFile(const char* key) const
{
    // If at least one additional file is supported, reuse or create as needed.
    if (m_targets.size() < m_concurrentFiles)
    {
        Owned<File>& file = m_targets[key];
        if (!file)
            file.setown(new File());
        return file.getLink();
    }
    // No additional files supported, reuse if possible. Ignore if more files exist
    // than are supported.
    TargetMap::iterator it = m_targets.find(key);
    if (it != m_targets.end())
    {
        return it->second.getLink();
    }
    // A new file is needed. Close, based on the last write time, as many files as
    // necessary to enable one new file to be opened. Recurse to create the new file.
    while (m_targets.size() >= m_concurrentFiles)
    {
        TargetMap::iterator oldest = (it = m_targets.begin());
        while (++it != m_targets.end())
        {
            if (oldest->second->m_lastWrite.compare(it->second->m_lastWrite) > 0)
                oldest = it;
        }
        m_targets.erase(oldest);
    }
    return getFile(key);
}

/**
 * Determines if a new physical file is required to process the update reuest. A new file is
 * required if:
 *   - no file I/O object exists in the file
 *   - daily rollover is enabled and the last write occurred on a different date than now
 *   - size rollover is enabled and the new content will cause the size threshold to be
 *     exceeded; the first write to a file will not require a new file regardless of size
 *
 * The variable map may be updated with system variables related to new file naming:
 *   - creationVarName will be added when a new file is required and the pattern references it;
 *     the value added will support a custom timestamp, even when not configured to be custom
 */
bool CFileTarget::needNewFile(size_t contentLength, const File& file, const Pattern& pattern, Variables& systemVariables) const
{
    const char* creationValue = m_creationDateTimeFormat;
    bool needed = !haveFile(file);
    if (!file.m_io) // implies needed is true
    {
        creationValue = m_creationDateFormat;
    }
    if (!needed && DailyRollover == m_rolloverInterval)
    {
        unsigned ly, lm, ld, ty, tm, td;
        CDateTime now;
        now.setNow();
        file.m_lastWrite.getDate(ly, lm, ld, false);
        now.getDate(ty, tm, td, false);
        needed = (ly != ty || lm != tm || ld != td);
    }
    if (!needed && m_rolloverSize)
    {
        size_t currentSize = file.m_io->size();
        if (m_header.length() != currentSize)
            needed = ((currentSize + contentLength) > m_rolloverSize);
    }
    if (needed && pattern.contains(creationVarName, nullptr))
        systemVariables[creationVarName] = creationValue;
    return needed;
}

bool CFileTarget::haveFile(const File& file) const
{
    if (file.m_io)
    {
        try
        {
            Owned<IFile> tmp(createIFile(file.m_filePath));
            if (tmp->exists())
                return true;
            iwarnlog(Major, "open file '%s' does not exist", file.m_filePath.str());
        }
        catch(IException* e)
        {
            StringBuffer msg;
            ierrlog(Major, "exception checking existence of file '%s': %s", file.m_filePath.str(), e->errorMessage(msg).str());
            e->Release();
        }
    }
    return false;
}

/**
 * Assuming a valid file path can be generated from the given pattern and variables, attempts
 * to create and open a new file. If the generated path references an existing file, the path
 * will be adjusted in an attempt to generate a new, unused file name:
 *   - a requested custom creation timestamp that peformed a date subsitution will retry with
 *     a date and time substitution
 *   - all other requests will use createUniqueFile
 */
bool CFileTarget::createNewFile(File& file, const Pattern& pattern, Variables& systemVariables) const
{
    StringBuffer filePath;
    pattern.toString(filePath, &systemVariables);

    try
    {
        if (recursiveCreateDirectoryForFile(filePath))
        {
            Owned<IFileIO> newIO;
            Owned<IFile> newFile(createIFile(filePath));
            if (newFile->exists())
            {
                // The filename is not unique in the file directory. Change something...
                Variables::iterator varIt;
                if (pattern.contains(creationVarName, creationVarCustomOption) &&
                    ((varIt = systemVariables.find(creationVarName)) != systemVariables.end()) &&
                    streq(varIt->second.c_str(), m_creationDateFormat) &&
                    !streq(varIt->second.c_str(), m_creationDateTimeFormat))
                {
                    // The pattern includes a custom creation timestamp and substituted the date only.
                    // Retry with a date and time substitution.
                    varIt->second = m_creationDateTimeFormat;
                    return createNewFile(file, pattern, systemVariables);
                }
                else
                {
                    // The filename is as unique as it can be, as configured. Add "random" text to
                    // resolve the conflict.
                    StringBuffer path, prefix, extension;
                    splitFilename(filePath, nullptr, &path, &prefix, &extension);
                    newIO.setown(createUniqueFile(path, prefix, extension, filePath.clear()));
                }
            }
            else
            {
                newIO.setown(newFile->openShared(IFOcreate, IFSHnone));
            }
            if (!newIO)
            {
                ierrlog(Major, "failed to create and open new file '%s'", filePath.str());
            }
            else if (!m_header.isEmpty() && newIO->write(0, m_header.length(), m_header) != m_header.length())
            {
                ierrlog(Major, "failed to add header to '%s'", filePath.str());
            }
            else
            {
                file.m_io.setown(newIO.getClear());
                file.m_filePath.set(filePath);
                file.m_lastWrite.setNow();
                return true;
            }
        }
        else
        {
            ierrlog(Major, "failed to create directory structure for '%s'", filePath.str());
        }
    }
    catch(IException* e)
    {
        StringBuffer msg;
        ierrlog(Major, "exception creating and opening new file '%s': %s", filePath.str(), e->errorMessage(msg).str());
        e->Release();
    }
    return false;
}

/**
 * Appends a section of data to a targeted file, capturing and recording errors.
 */
bool CFileTarget::writeChunk(File& file, offset_t pos, size32_t len, const char* data) const
{
    try
    {
        size32_t written = file.m_io->write(file.m_io->size(), len, data + pos);
        file.m_lastWrite.setNow();
        if (written == len)
            return true;
        ierrlog(Major, "failed to write %u bytes to '%s'; wrote %u", len, file.m_filePath.str(), written);
    }
    catch(IException* e)
    {
        StringBuffer msg;
        ierrlog(Major, "exception writing %u bytes to '%s': %s", len, file.m_filePath.str(), e->errorMessage(msg).str());
        e->Release();
    }
    return false;
}

//////////////////// CEspLogAgent ////////////////////

const char* CEspLogAgent::getName()
{
    return m_name;
}

bool CEspLogAgent::init(const char* name, const char* type, IPTree* configuration, const char* process)
{
    if (!configuration)
        throw makeStringExceptionV(-1, "%s[%s]: missing configuration", ModularLogAgent::moduleAgent, name);

    m_name.set(name);

    Linked<IPTree> effectiveConfiguration;
    const char* configFile = configuration->queryProp(propConfiguration);
    if (!isEmptyString(configFile))
    {
        Owned<IPTree> tmp = nullptr;
        try
        {
            if (strstr(configFile, ".yaml"))
            {
                tmp.setown(createPTreeFromYAMLFile(configFile));
                if (tmp)
                    tmp.setown(LINK(tmp->queryBranch("*[1]")));
            }
            else if (strstr(configFile, ".xml"))
            {
                tmp.setown(createPTreeFromXMLFile(configFile));
            }
            else
            {
                // Does use of a legacy configuration suggest a more likely option than XML?
                uwarnlog(Highest, "%s[%s]: '%s' does not identify an expected content type; assuming XML", ModularLogAgent::moduleAgent, name, configFile);
                tmp.setown(createPTreeFromXMLFile(configFile));
            }
            if (!tmp)
                uerrlog(Major, "%s[%s]: property tree extraction failed from '%s'", ModularLogAgent::moduleAgent, name, configFile);
            else if (!streq(tmp->queryName(), ModularLogAgent::moduleAgent))
                uwarnlog(Major, "%s[%s]: unexpected configuration root '%s' in '%s'", ModularLogAgent::moduleAgent, name, tmp->queryName(), configFile);
        }
        catch (IException* e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            uerrlog(Major, "%s[%s]: exception extracting property tree from '%s': %s", ModularLogAgent::moduleAgent, name, configFile, e->errorMessage(msg).str());
            e->Release();
        }
        if (tmp)
            effectiveConfiguration.set(tmp);
        else
            return false;
    }
    else
    {
        effectiveConfiguration.set(configuration);
    }

    const char* module = effectiveConfiguration->queryProp(IModule::propFactoryKey);
    m_factory->m_agents.create(m_agent, configuration, *this);
    if (!m_agent)
    {
        if (module)
            throw makeStringExceptionV(-1, "%s[%s:%s]: agent module creation failed", ModularLogAgent::moduleAgent, name, module);
        else
            throw makeStringExceptionV(-1, "%s[%s]: agent module creation failed", ModularLogAgent::moduleAgent, name);
    }

    bool result = m_agent->configure(*effectiveConfiguration, *m_factory);
    if (Trivial <= tracePriorityLimit(MSGAUD_user, MSGCLS_information))
    {
        StringBuffer desc;
        iproglog(1, "%s[%s]: configured %s", ModularLogAgent::moduleAgent, name, m_agent->toString(desc).str());
    }
    if (!result)
        m_agent.clear();
    return result;
}

bool CEspLogAgent::initVariants(IPTree* configuration)
{
    Owned<IPTreeIterator> variantNodes(configuration->getElements("//Variant"));
    ForEach(*variantNodes)
    {
        IPTree& variant = variantNodes->query();
        const char* type = variant.queryProp("@type");
        const char* group = variant.queryProp("@group");
        Owned<CEspLogAgentVariant> instance = new CEspLogAgentVariant(getName(), type, group);
        CEspLogAgentVariants::iterator it = m_variants.find(instance);

        if (m_variants.end() == it)
            m_variants.insert(instance.getClear());
    }

    if (m_variants.empty())
        m_variants.insert(new CEspLogAgentVariant(getName(), nullptr, nullptr));
    return true;
}

bool CEspLogAgent::getTransactionSeed(IEspGetTransactionSeedRequest& request, IEspGetTransactionSeedResponse& response)
{
    return m_agent && m_agent->getTransactionSeed(request, response);
}

void CEspLogAgent::getTransactionID(StringAttrMapping* fields, StringBuffer& id)
{
    if (m_agent)
        m_agent->getTransactionID(fields, id);
}

bool CEspLogAgent::updateLog(IEspUpdateLogRequestWrap& request, IEspUpdateLogResponse& response)
{
    if (m_agent)
        m_agent->updateLog(request, response);
    // The return value is not used by the caller.
    // Other agents return true regardless of outcome.
    return true;
}

bool CEspLogAgent::hasService(LOGServiceType type)
{
    return m_agent && m_agent->hasService(type);
}

IEspUpdateLogRequestWrap* CEspLogAgent::filterLogContent(IEspUpdateLogRequestWrap* unfilteredRequest)
{
    return m_filter.filterLogContent(unfilteredRequest);
}

IEspLogAgentVariantIterator* CEspLogAgent::getVariants() const
{
    return new CVariantIterator(*this);
}

CEspLogAgent::CEspLogAgent(const CModuleFactory& factory)
{
    m_factory.setown(&factory);
}

//////////////////// CModuleFactory ////////////////////

CModuleFactory::CModuleFactory()
{
    using namespace ModularLogAgent;
    m_agents.add<CDelegatingAgent>(keyDefault);
    m_agents.add<CMockAgent>("mock");
    m_updateLogs.add<CDelegatingUpdateLog>(keyDefault);
    m_contentTargets.add<CContentTarget>(keyDefault);
    m_contentTargets.add<CFileTarget>(keyFile);
}

} // namespace ModularLogAgent
