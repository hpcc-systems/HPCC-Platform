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

#include "jexcept.hpp"
#include "thorherror.h"
#include "roxiehelper.hpp"
#include "roxiedebug.hpp"
#include "portlist.h"
#include "eclrtl.hpp"
#include "deftype.hpp"
#include "eclhelper.hpp"
#include "thorcommon.hpp"

//=======================================================================================

bool CDebugCommandHandler::checkCommand(IXmlWriter &out, const char *&supplied, const char *expected)
{
    unsigned i = 0;
    loop
    {
        if (!supplied[i])
        {
            out.outputBeginNested(expected, true);
            supplied = expected;
            return true;
        }
        if (!expected[i])
            return false;
        if (supplied[i] != expected[i])
            return false;
        i++;
    }
}

void CDebugCommandHandler::doDebugCommand(IPropertyTree *query, IDebuggerContext *debugContext, FlushingStringBuffer &output)
{
    const char *commandName = query->queryName();
    CommonXmlWriter out(0, 1);
    if (strnicmp(commandName, "b", 1)==0 && checkCommand(out, commandName, "breakpoint"))
    {
        const char *mode = query->queryProp("@mode");
        const char *id = query->queryProp("@id");
        const char *action = query->queryProp("@action");
        const char *fieldName = query->queryProp("@fieldName");
        const char *condition = query->queryProp("@condition");
        bool caseSensitive = query->getPropBool("@caseSensitive", false);
        const char *value = query->queryProp("@value");
        unsigned rowCount = query->getPropInt("@rowCount", 0);
        const char *rowCountMode = query->queryProp("@rowCountMode");
        debugContext->addBreakpoint(&out, mode, id, action, fieldName, condition, value, caseSensitive, rowCount, rowCountMode);
    }
    else if (strnicmp(commandName, "c", 1)==0 && checkCommand(out, commandName, "continue"))
    {
        const char *mode = query->queryProp("@mode");
        const char *id = query->queryProp("@id");
        debugContext->debugContinue(&out, mode, id);
    }
    else if (strnicmp(commandName, "c", 1)==0 && checkCommand(out, commandName, "changes"))
    {
        unsigned sequence = query->getPropInt("@sequence", -1);
        debugContext->debugChanges(&out, sequence);
    }
    else if (strnicmp(commandName, "c", 1)==0 && checkCommand(out, commandName, "counts"))
    {
        // MORE It could be argued that this is really more a "changes" with a "global" vs "active" option....
        unsigned sequence = query->getPropInt("@sequence", -1);
        debugContext->debugCounts(&out, sequence, false);
    }
    else if (strnicmp(commandName, "d", 1)==0 && checkCommand(out, commandName, "delete"))
    {
        const char *idx = query->queryProp("@idx");
        if (!idx)
            throw MakeStringException(THORHELPER_DEBUG_ERROR, "delete must specify breakpoint index, or all");
        if (stricmp(idx, "all")==0)
            debugContext->removeAllBreakpoints(&out);
        else 
        {
            char *ep;
            unsigned bpIdx = strtoul(idx, &ep, 10);
            if (ep != idx && *ep==0)
            {
                if (bpIdx)
                    debugContext->removeBreakpoint(&out, bpIdx);
                else
                    debugContext->removeAllBreakpoints(&out);
            }
            else
                throw MakeStringException(THORHELPER_DEBUG_ERROR, "Invalid value for idx - expected number or all");
        }
    }
    else if (strnicmp(commandName, "g", 1)==0 && checkCommand(out, commandName, "graph"))
    {
        const char *graphName = query->queryProp("@name");
        if (!graphName)
        {
            bool original = query->getPropBool("@original", true);
            debugContext->getCurrentGraphXGMML(&out, original);
        }
        else if (stricmp(graphName, "all")==0)
            debugContext->getQueryXGMML(&out);
        else
            debugContext->getGraphXGMML(&out, graphName);
    }
    else if (strnicmp(commandName, "g", 1)==0 && checkCommand(out, commandName, "get"))
    {
        const char *name = query->queryProp("@name");
        const char *id = query->queryProp("@id");
        debugContext->debugGetConfig(&out, name, id);
    }
    else if (strnicmp(commandName, "i", 1)==0 && checkCommand(out, commandName, "interrupt"))
    {
        debugContext->debugInterrupt(&out);
    }
    else if (strnicmp(commandName, "l", 1)==0 && checkCommand(out, commandName, "list"))
    {
        const char *idx = query->queryProp("@idx");
        if (!idx || strcmp(idx, "all")==0)
            debugContext->listAllBreakpoints(&out);
        else 
        {
            char *ep;
            unsigned  bpIdx = strtoul(idx, &ep, 10);
            if (ep != idx && *ep==0)
                debugContext->listBreakpoint(&out, bpIdx);
            else
                throw MakeStringException(THORHELPER_DEBUG_ERROR, "Invalid value for idx - expected number or all");
        }
    }
    else if (strnicmp(commandName, "n", 1)==0 && checkCommand(out, commandName, "next"))
    {
        debugContext->debugNext(&out);
    }
    else if (strnicmp(commandName, "o", 1)==0 && checkCommand(out, commandName, "over"))
    {
        debugContext->debugOver(&out);
    }
    else if (strnicmp(commandName, "p", 1)==0 && checkCommand(out, commandName, "print"))
    {
        const char *edgeId = query->queryProp("@edgeId");
        unsigned numRows = query->getPropInt("@numRows", 1);
        unsigned startRow= query->getPropInt("@startRow", 0);
        debugContext->debugPrint(&out, edgeId, startRow, numRows);
    }
    else if (strnicmp(commandName, "q", 1)==0 && checkCommand(out, commandName, "quit"))
    {
        debugContext->debugQuit(&out);
    }
    else if (strnicmp(commandName, "r", 1)==0 && checkCommand(out, commandName, "run"))
    {
        debugContext->debugRun(&out);
    }
    else if (strnicmp(commandName, "s", 1)==0 && checkCommand(out, commandName, "step"))
    {
        const char *mode = query->queryProp("@mode");
        debugContext->debugStep(&out, mode);
    }
    else if (strnicmp(commandName, "s", 1)==0 && checkCommand(out, commandName, "status"))
    {
        debugContext->debugStatus(&out);
    }
    else if (strnicmp(commandName, "s", 1)==0 && checkCommand(out, commandName, "skip"))
    {
        debugContext->debugSkip(&out);
    }
    else if (strnicmp(commandName, "s", 1)==0 && checkCommand(out, commandName, "search"))
    {
        const char *fieldName = query->queryProp("@fieldName");
        const char *condition = query->queryProp("@condition");
        if (!condition) 
            condition="contains";
        const char *value = query->queryProp("@value");
        bool caseSensitive = query->getPropBool("@caseSensitive", false);
        bool fullRows = query->getPropBool("@fullRows", false);
        debugContext->debugSearch(&out, fieldName, condition, value, caseSensitive, fullRows);
    }
    else if (strnicmp(commandName, "s", 1)==0 && checkCommand(out, commandName, "set"))
    {
        const char *name = query->queryProp("@name");
        const char *value = query->queryProp("@value");
        const char *id = query->queryProp("@id");
        debugContext->debugSetConfig(&out, name, value, id);
    }
    else if (strnicmp(commandName, "v", 1)==0 && checkCommand(out, commandName, "variable"))
    {
        const char *name = query->queryProp("@name");
        const char *type = query->queryProp("@type");
        debugContext->debugPrintVariable(&out, name, type);
    }
    else if (strnicmp(commandName, "w", 1)==0 && checkCommand(out, commandName, "where"))
    {
        debugContext->debugWhere(&out);
    }
    else
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Unknown command %s", commandName);
    out.outputEndNested(commandName);
    output.append(out.str());
}

//=======================================================================================

MAKEValueArray(const RtlTypeInfo *, RtlTypeInfoArray);

class SimpleFieldSearcher : public CInterface, implements IRowMatcher
{
protected:
    unsigned searchStringLength;
    const char *searchString;
    StringAttr expression;
    StringAttr searchFieldName;
    MemoryBuffer searchQString;

    bool matchSeen;
    bool boolValueSet;
    bool intValueSet;
    bool uintValueSet;
    bool realValueSet;
    bool decimalValueSet;
    bool udecimalValueSet;
    bool qstringValueSet;
    bool boolValue;

    __int64 intValue;
    double realValue;

    BreakpointConditionMode mode;
    bool caseSensitive;
    RtlTypeInfoArray recordTypesCanMatch;
    RtlTypeInfoArray recordTypesCannotMatch;

    bool checkCondition(int diff)
    {
        switch (mode)
        {
            case BreakpointConditionLess: 
                return (diff < 0);
            case BreakpointConditionLessEqual:
                return (diff <= 0);
            case BreakpointConditionGreater:
                return (diff > 0);
            case BreakpointConditionGreaterEqual:
                return (diff >= 0);
            case BreakpointConditionNotEqual:
                return (diff != 0);
            default:
                return (diff == 0);
        }
    }

    inline bool checkFieldName(const char *fieldname)
    {
        return searchFieldName.isEmpty() || stricmp(searchFieldName, fieldname)==0;
    }

public:
    SimpleFieldSearcher(const char *_searchFieldName, const char *_expression, BreakpointConditionMode _mode, bool _caseSensitive) 
        : searchFieldName(_searchFieldName), expression(_expression), mode(_mode), caseSensitive(_caseSensitive)
    {
        searchString = expression.get();
        searchStringLength = expression.length();
        matchSeen = false;

        boolValueSet = false;
        intValueSet = false;
        uintValueSet = false;
        realValueSet = false;
        decimalValueSet = false;
        udecimalValueSet = false;
        qstringValueSet = false;

        if (searchString)
        {
            if (stricmp(searchString, "true")==0)
                boolValueSet = boolValue = true;
            else if (stricmp(searchString, "false")==0)
            {   
                boolValueSet = true; boolValue = false; 
            }
            else
            {
                // MORE - should really support decimal, real, and perhaps esoteric forms of integer (0x.... etc) here....
                const char *v = searchString;
                bool isNegative = false;
                while (isspace(*v))
                    v++;
                if (*v=='-')
                {
                    isNegative = true;
                    v++;
                }
                if (isdigit(*v))
                {
                    intValue = 0;
                    char c;
                    loop
                    {
                        c = *v++;
                        if ((c >= '0') && (c <= '9')) 
                            intValue = intValue * 10 + (c-'0');
                        else
                            break;
                    }
                    switch (c)
                    {
                    case ' ':
                        while (isspace(c = *v))
                            v++;
                        if (c)
                            break;
                        // fall into...
                    case '\0':
                        intValueSet = true;
                        if (isNegative)
                            intValue = -intValue;
                        else
                            uintValueSet = true;
                        break;
// MORE - want something like...
//                  case '.':
//                      TempDecimal.setString().getDecimal()

                    }
                }
            }
        }
    }

    virtual bool matched() const { return matchSeen; }
    virtual void reset() { matchSeen = false; }
    virtual const char *queryFieldName() const { return searchFieldName; }
    virtual const char *queryValue() const { return expression; }
    virtual bool queryCaseSensitive() const { return caseSensitive; }
    virtual void serialize(MemoryBuffer &out) const
    {
        out.append(searchFieldName).append((char) mode).append(expression).append(caseSensitive);
    }

    bool canMatchAny(const RtlTypeInfo *recordType)
    {
        bool sawMatch = false;
        ForEachItemIn (idx1, recordTypesCanMatch)
        {
            if (recordTypesCanMatch.item(idx1) == recordType)
            {
                if (idx1)
                    recordTypesCanMatch.swap(0, idx1);
                return true;
            }
        }

        ForEachItemIn (idx2, recordTypesCannotMatch)
        {
            if (recordTypesCannotMatch.item(idx2) == recordType)
            {
                if (idx2)
                    recordTypesCannotMatch.swap(0, idx2);
                return false;
            }
        }

        const RtlFieldInfo * const * fields = recordType->queryFields();
        assertex(fields);
        while (*fields)
        {
            const RtlTypeInfo *childType = fields[0]->type->queryChildType();
            if (childType)
            {
                // MORE - get funky with fieldname.fieldname ? Possible future feature....
                if (canMatchAny(childType))
                    sawMatch = true;
            }
            if (stricmp(fields[0]->name->getAtomNamePtr(), searchFieldName)==0)
                sawMatch = true;
            fields++;
        }
        if (sawMatch)
            recordTypesCanMatch.add(recordType, 0);
        else
            recordTypesCannotMatch.add(recordType, 0);
        return sawMatch;
    }

    virtual bool canMatchAny(IOutputMetaData *meta)
    {
        bool canMatch = true; // safest assumption...
        if (searchFieldName.length())
        {
            const RtlTypeInfo *typeInfo = meta->queryTypeInfo();
            assertex(typeInfo);
            if (!canMatchAny(typeInfo))
                canMatch = false;
        }
        return canMatch;
    }

    IMPLEMENT_IINTERFACE;
    virtual void outputQuoted(const char *text) { }

    virtual void outputString(unsigned len, const char *field, const char *fieldname) 
    {
        if (!matchSeen && checkFieldName(fieldname) && (len >= searchStringLength))
        {
            int diff = caseSensitive ? memcmp(field,searchString,searchStringLength) : memicmp(field,searchString,searchStringLength);
            if (!diff) // little bit suboptimal as some modes don't care about difference between equal and gt - but who cares!
            {
                // if any remaining char is not space, it's gt
                unsigned i = searchStringLength;
                while (i < len)
                {
                    if (field[i] != ' ')
                    {
                        diff++;
                        break;
                    }
                    i++;
                }
            }
            matchSeen = checkCondition(diff);
        }
    }

    virtual void outputBool(bool field, const char *fieldname) 
    {
        if (boolValueSet && !matchSeen && checkFieldName(fieldname))
        {
            matchSeen = checkCondition((int) field - (int) boolValue);
        }
    }
    virtual void outputData(unsigned len, const void *field, const char *fieldname) 
    {
        // MORE
    }
    virtual void outputInt(__int64 field, const char *fieldname) 
    {
        if (intValueSet && !matchSeen && checkFieldName(fieldname))
        {
            matchSeen = checkCondition((field == intValue) ? 0 : ((field > intValue) ? 1 : -1));
        }
    }
    virtual void outputUInt(unsigned __int64 field, const char *fieldname) 
    {
        // NOTE - "contains" is interpreted as "equals" on numeric fields
        if (uintValueSet && !matchSeen && checkFieldName(fieldname))
        {
            matchSeen = checkCondition((field == intValue) ? 0 : ((field > (unsigned __int64) intValue) ? 1 : -1));
        }
    }
    virtual void outputReal(double field, const char *fieldname) 
    {
        // NOTE - "contains" is interpreted as "equals" on numeric fields
        if (realValueSet && !matchSeen && checkFieldName(fieldname))
        {
            matchSeen = checkCondition((field == realValue) ? 0 : ((field > realValue) ? 1 : -1));
        }
    }

    virtual void outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname) 
    {
        // Searching/breaking on decimal not supported at the moment
    }
    virtual void outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname) 
    {
        // Searching/breaking on decimal not supported at the moment
    }
    virtual void outputUnicode(unsigned len, const UChar *field, const char *fieldname) 
    {
        // Searching/breaking on unicode not supported at the moment
    }
    virtual void outputQString(unsigned len, const char *field, const char *fieldname) 
    {
        if (!matchSeen && checkFieldName(fieldname))
        {
            if (!qstringValueSet)
            {
                size32_t outlen;
                char *out;
                rtlStrToQStrX(outlen, out, searchStringLength, searchString);
                searchQString.setBuffer(outlen, out, true);
            }
            matchSeen = checkCondition(rtlCompareQStrQStr(len, field, searchQString.length(), searchQString.toByteArray()));
        }
    }
    virtual void outputUtf8(unsigned len, const char *field, const char *fieldname) 
    {
        // Searching/breaking on unicode not supported at the moment
    }

    virtual void outputBeginNested(const char *fieldname, bool nestChildren) 
    {
        // nothing for now
    }
    virtual void outputEndNested(const char *fieldname) 
    {
        // nothing for now
    }
    virtual void outputBeginArray(const char *fieldname)
    {
        // nothing for now
    }
    virtual void outputEndArray(const char *fieldname)
    {
        // nothing for now
    }
    virtual void outputSetAll()
    {
        // nothing for now
    }
};

class ContainsFieldSearcher : public SimpleFieldSearcher
{
    // For most field types, just use equals version. Only meaningful for string types (and only implemented so far for plain ascii)
public:
    ContainsFieldSearcher(const char *_searchFieldName, const char *_expression, bool _caseSensitive=true) : SimpleFieldSearcher(_searchFieldName, _expression, BreakpointConditionContains, _caseSensitive)
    {
        // This is where you set up Boyer-Moore tables...
    }

    virtual void outputString(unsigned len, const char *field, const char *fieldname) 
    {
        if (!matchSeen && checkFieldName(fieldname) && (len >= searchStringLength)) // could swap those last two tests - last is faster but filters less out...
        {
            if (searchStringLength==1)
            {
                if (memchr(field, searchString[0], len))
                    matchSeen = true;
            }
            else
            {
                unsigned steps = len-searchStringLength+1;
                for ( unsigned i = 0; i < steps; i++ )
                {
                    if ( !memcmp((char *)field+i,searchString,searchStringLength) ) // MORE - Should I use Boyer-Moore? Probably. Is it worth it in (typically) small search targets?
                    {
                        matchSeen = true;
                        return;
                    }
                }
            }
        }
    }
};

class ContainsNoCaseFieldSearcher : public ContainsFieldSearcher
{
    // For most field types, just use equals version. Only meaningful for string types (and only implemented so far for plain ascii)
public:
    ContainsNoCaseFieldSearcher(const char *_searchFieldName, const char *_expression) : ContainsFieldSearcher(_searchFieldName, _expression, false)
    {
    }

    virtual void outputString(unsigned len, const char *field, const char *fieldname) 
    {
        if (!matchSeen && checkFieldName(fieldname) && (len >= searchStringLength))
        {
            unsigned steps = len-searchStringLength+1;
            for ( unsigned i = 0; i < steps; i++ )
            {
                if ( !memicmp((char *)field+i,searchString,searchStringLength) )
                {
                    matchSeen = true;
                    return;
                }
            }
        }
    }
};

class StartsWithFieldSearcher : public SimpleFieldSearcher
{
    // For most field types, just use equals version. Only meaningful for string types (and only implemented so far for plain ascii)
public:
    StartsWithFieldSearcher(const char *_searchFieldName, const char *_expression, bool _caseSensitive) : SimpleFieldSearcher(_searchFieldName, _expression, BreakpointConditionStartsWith, _caseSensitive)
    {
    }

    virtual void outputString(unsigned len, const char *field, const char *fieldname) 
    {
        if (!matchSeen && checkFieldName(fieldname) && (len >= searchStringLength))
        {
            int diff = caseSensitive ? memcmp(field,searchString,searchStringLength) : memicmp(field,searchString,searchStringLength);
            if (diff == 0)
                matchSeen = true;
        }
    }
};

extern IRowMatcher *createRowMatcher(const char *fieldName, BreakpointConditionMode condition, const char *value, bool caseSensitive)
{
    switch (condition)
    {
    case BreakpointConditionNone:
    case BreakpointConditionEOG:
    case BreakpointConditionEOF:
        return NULL;
    case BreakpointConditionLess:
    case BreakpointConditionLessEqual:
    case BreakpointConditionGreater:
    case BreakpointConditionGreaterEqual:
    case BreakpointConditionNotEqual:
    case BreakpointConditionEquals:
        return new SimpleFieldSearcher(fieldName, value, condition, caseSensitive);
    case BreakpointConditionContains:
        if (caseSensitive)
            return new ContainsFieldSearcher(fieldName, value);
        else
            return new ContainsNoCaseFieldSearcher(fieldName, value);
    case BreakpointConditionStartsWith:
        return new StartsWithFieldSearcher(fieldName, value, caseSensitive);
    default: 
        throwUnexpected();
    }
}

extern IRowMatcher *createRowMatcher(MemoryBuffer &serialized)
{
    StringAttr fieldName;
    char condition;
    StringAttr value;
    bool caseSensitive;

    serialized.read(fieldName).read(condition).read(value).read(caseSensitive);
    return createRowMatcher(fieldName, (BreakpointConditionMode) condition, value, caseSensitive);
}

//===============================================================================================

CBreakpointInfo::CBreakpointInfo(BreakpointMode _mode, const char *_id, BreakpointActionMode _action,
                                           const char *_fieldName, BreakpointConditionMode _condition, const char *_value, bool _caseSensitive,
                                           unsigned _rowCount, BreakpointCountMode _rowCountMode)
                                           : mode(_mode), id(_id), action(_action), condition(_condition),
                                           rowCount(_rowCount), rowCountMode(_rowCountMode)
{
    uid = nextUID();
    rowMatcher.setown(createRowMatcher(_fieldName, _condition, _value, _caseSensitive));
}

CBreakpointInfo::CBreakpointInfo(BreakpointMode _mode) : mode(_mode)
{
    uid = 0; // reserved for the dummy breakpoint
    action = BreakpointActionContinue;
    rowCount = 0;
    rowCountMode = BreakpointCountNone;
    condition = BreakpointConditionNone;
}

CBreakpointInfo::CBreakpointInfo(MemoryBuffer &from)
{
    from.read(uid);
    from.read(id);
    char temp;
    from.read(temp); mode = (BreakpointMode) temp;
    from.read(temp); action = (BreakpointActionMode) temp;
    from.read(rowCount);
    from.read(temp); rowCountMode = (BreakpointCountMode) temp;
    from.read(temp); condition = (BreakpointConditionMode) temp;
    switch (condition)
    {
    case BreakpointConditionNone:
    case BreakpointConditionEOG:
    case BreakpointConditionEOF:
        break;
    default:
        StringAttr fieldName, value;
        bool caseSensitive;
        from.read(fieldName);
        from.read(value);
        from.read(caseSensitive);
        rowMatcher.setown(createRowMatcher(fieldName, condition, value, caseSensitive));
    }
}

void CBreakpointInfo::serialize(MemoryBuffer &to) const
{
    to.append(uid);
    to.append(id);
    to.append((char) mode);
    to.append((char) action);
    to.append(rowCount);
    to.append((char) rowCountMode);
    to.append((char) condition);
    if (rowMatcher)
    {
        to.append(rowMatcher->queryFieldName());
        to.append(rowMatcher->queryValue());
        to.append(rowMatcher->queryCaseSensitive());
    }
}

SpinLock CBreakpointInfo::UIDlock;
unsigned CBreakpointInfo::nextUIDvalue;

unsigned CBreakpointInfo::queryUID() const
{
    return uid;
}

void CBreakpointInfo::noteEdge(IActivityDebugContext &edge)
{
    activeEdges.append(edge);
}

void CBreakpointInfo::removeEdge(IActivityDebugContext &edge)
{
    activeEdges.zap(edge);
}

bool CBreakpointInfo::equals(IBreakpointInfo &other) const
{
    CBreakpointInfo *bp = QUERYINTERFACE(&other, CBreakpointInfo);
    if (bp)
    {
        // NOTE - not very efficient but that's not an issue here!
        CommonXmlWriter me(0,0);
        CommonXmlWriter him(0,0);
        toXML(&me);
        bp->toXML(&him);
        return strcmp(me.str(), him.str())==0;
    }
    else
        return false;
}

bool CBreakpointInfo::canMatchAny(IOutputMetaData *meta)
{
    if (rowMatcher)
        return rowMatcher->canMatchAny(meta);
    else
        return true;
}

bool CBreakpointInfo::matches(const void *row, bool isEOF, unsigned edgeRowCount, IOutputMetaData *meta) const
{
    // First check the conditions
    switch (condition)
    {
    case BreakpointConditionNone:
        break;

    case BreakpointConditionEOG:
        if (row)
            return false;
        break;

    case BreakpointConditionEOF:
        if (row || !(meta->isGrouped() || isEOF))
            return false;
        break;

    default:
        if (rowMatcher)
        {
            if (!row)
                return false;
            rowMatcher->reset();
            meta->toXML((const byte *) row, *rowMatcher);
            if (!rowMatcher->matched())
                return false; // expression failed
        }
        else
            throwUnexpected();
    }

    // ok, we've been hit.
    if (rowCount)
    {
        switch (rowCountMode)
        {
        case BreakpointCountEquals:
            return (edgeRowCount == rowCount);
        case BreakpointCountAtleast:
            return (edgeRowCount >= rowCount);
        default:
            throwUnexpected();
        }
    }
    else
        return true;
}

bool CBreakpointInfo::idMatch(BreakpointMode _mode, const char *_id) const
{
    if (mode==_mode)
    {
        if (mode==BreakpointModeGraph && !id.length())
            return true;
        if (strcmp(id.get(), _id)==0)
            return true;
        // See if it matches up to the first .
        const char *dotpos = strchr(_id, '.');
        if (dotpos)
        {
            unsigned len = dotpos-_id;
            if (len == id.length() && strncmp(id.get(), _id, len)==0)
                return true;
        }
    }
    return false;
}

BreakpointMode CBreakpointInfo::queryMode() const
{
    return mode;
}

BreakpointActionMode CBreakpointInfo::queryAction() const
{
    return action;
}

void CBreakpointInfo::toXML(IXmlWriter *output) const
{
    output->outputCString(BreakpointModes[mode], "@mode"); 
    if (id) output->outputCString(id, "@id"); 
    output->outputCString(BreakpointActionModes[action], "@action"); 
    if (condition != BreakpointConditionNone)
    {
        output->outputCString(BreakpointConditionModes[condition], "@condition"); 
        if (rowMatcher)
        {
            if (rowMatcher->queryFieldName()) output->outputCString(rowMatcher->queryFieldName(), "@fieldName");
            if (rowMatcher->queryValue()) output->outputCString(rowMatcher->queryValue(), "@value");
            output->outputBool(rowMatcher->queryCaseSensitive(), "@caseSensitive");
        }
    }
    if (rowCount)
    {
        output->outputInt(rowCount, "@rowCount"); 
        output->outputCString(BreakpointCountModes[rowCountMode], "@rowCountMode"); 
    }
    ForEachItemIn(edgeIdx, activeEdges)
    {
        IActivityDebugContext &edge = activeEdges.item(edgeIdx);
        output->outputBeginNested("edge", false);
        output->outputCString(edge.queryEdgeId(), "@edgeId");
        output->outputEndNested("edge");
    }
}

//=======================================================================================

DebugActivityRecord::DebugActivityRecord (IActivityBase *_activity, unsigned _iteration, unsigned _channel, unsigned _sequence)
        : activity(_activity),iteration(_iteration),channel(_channel),sequence(_sequence)
{
    localCycles = 0;
    totalCycles = 0;
    idText.append(activity->queryId());
    if (iteration || channel) 
        idText.appendf(".%d", iteration);
    if (channel) 
        idText.appendf("#%d", channel);
}

void DebugActivityRecord::outputId(IXmlWriter *output, const char *fieldName)
{
    output->outputString(idText.length(), idText.str(), fieldName);
}

const char *DebugActivityRecord::queryIdString()
{
    return idText;
}

void DebugActivityRecord::outputProperties(IXmlWriter *output)
{
    if (properties)
    {
        Owned<IPropertyIterator> iterator = properties->getIterator();
        iterator->first();
        while (iterator->isValid())
        {
            const char *propName = iterator->getPropKey();
            const char *propValue = properties->queryProp(propName);
            output->outputBeginNested("att", false); output->outputCString(propName, "@name"); output->outputCString(propValue, "@value"); output->outputEndNested("att");
            iterator->next();
        }
    }
    if (localCycles)
    {
        output->outputBeginNested("att", false); 
        output->outputCString("localTime", "@name"); 
        output->outputUInt((unsigned) (cycle_to_nanosec(localCycles)/1000), "@value"); 
        output->outputEndNested("att");
    }
    if (totalCycles)
    {
        output->outputBeginNested("att", false); 
        output->outputCString("totalTime", "@name"); 
        output->outputUInt((unsigned) (cycle_to_nanosec(totalCycles)/1000), "@value"); 
        output->outputEndNested("att");
    }
}

void DebugActivityRecord::setProperty(const char *propName, const char *propValue, unsigned _sequence)
{
    if (!properties)
        properties.setown(createProperties(false));
    properties->setProp(propName, propValue);
    sequence = _sequence;
}

void DebugActivityRecord::updateTimes(unsigned _sequence)
{
    unsigned __int64 newTotal = activity->queryTotalCycles();
    unsigned __int64 newLocal = activity->queryLocalCycles();
    if (localCycles != newLocal || totalCycles != newTotal)
    {
        localCycles = newLocal;
        totalCycles = newTotal;
        sequence = _sequence;
    }
}

//=======================================================================================

const char * CBaseDebugContext::queryStateString(DebugState state)
{
    switch (state)
    {
    case DebugStateCreated: return "created";
    case DebugStateLoading: return "loading";
    case DebugStateRunning: return "running";
    case DebugStateEdge: return "edge";
    case DebugStateBreakpoint: return "breakpoint";
    case DebugStateReady: return "created";
    case DebugStateGraphCreate: return "graph create";
    case DebugStateGraphStart: return "graph start";
    case DebugStateGraphEnd: return "graph end";
    case DebugStateGraphAbort: return "graph abort";
    case DebugStateException: return "exception";
    case DebugStateFailed: return "failed";
    case DebugStateFinished: return "finished";
    case DebugStateUnloaded: return "unloaded";
    case DebugStateQuit: return "quit";
    case DebugStateDetached: return "detached";
    case DebugStateLimit: return "limit hit";
    case DebugStateGraphFinished: return "graph finished";

    default: throwUnexpected();
    }
}

bool CBaseDebugContext::_checkPendingBreakpoints(DebugState state, const char *graphName)
{
    bool stop = false;
    ForEachItemIn(idx, breakpoints)
    {
        IBreakpointInfo &bp = breakpoints.item(idx);
        if (bp.queryMode()==BreakpointModeGraph)
        {
            stop = stop || bp.idMatch(BreakpointModeGraph, graphName);
        }
        else
        {
            if (state==DebugStateGraphStart)
                currentGraph->setBreakpoint(bp);
        }
    }
    return stop;
}

CBaseDebugContext::CBaseDebugContext(const IContextLogger &_logctx) : logctx(_logctx)
{
    currentGraph = NULL;
    graphChangeSequence = 0;
    prevGraphChangeSequence = 0;
    pendingBreakpointsDone = false;
    sequence = 1;
    defaultHistoryCapacity = 10;
    executeSequentially = true;
    running = true;
    detached = false;
    watchState = WatchStateContinue;
    currentBreakpointUID = (unsigned) -1;
    debuggerActive = 0;
    currentState = DebugStateCreated;
    skipRequested = false;
    stopOnLimits = true;
    debugCyclesAdjust = 0;
}

unsigned __int64 CBaseDebugContext::getCyclesAdjustment() const
{
    return debugCyclesAdjust;
}

void CBaseDebugContext::noteGraphChanged()
{
    graphChangeSequence++;
}

IGlobalEdgeRecord *CBaseDebugContext::getEdgeRecord(const char *edgeId)
{
    Linked<IGlobalEdgeRecord> edgeRecord = globalCounts.getValue(edgeId);
    if (!edgeRecord)
    {
        edgeRecord.setown(new DebugEdgeRecord(sequence));
        globalCounts.setValue(edgeId, edgeRecord);
    }
    return edgeRecord.getClear();
}

void CBaseDebugContext::addBreakpoint(IBreakpointInfo &bp)
{
    if (currentGraph)
        currentGraph->setBreakpoint(bp);
    breakpoints.append(bp);
}

void CBaseDebugContext::removeBreakpoint(IBreakpointInfo &bp)
{
    ForEachItemIn(idx, breakpoints)
    {
        if (breakpoints.item(idx).queryUID()==bp.queryUID())
        {
            if (currentGraph)
                currentGraph->removeBreakpoint(breakpoints.item(idx));
            breakpoints.remove(idx);
            break;
        }
    }
}

unsigned CBaseDebugContext::queryChannel() const
{
    return 0;
}

void CBaseDebugContext::debugInitialize(const char *id, const char *_queryName, bool _breakAtStart)
{
    throwUnexpected(); // Should only happen on server
}

void CBaseDebugContext::debugTerminate()
{
    throwUnexpected(); // Should only happen on server
}

IBreakpointInfo *CBaseDebugContext::findBreakpoint(unsigned uid) const
{
    ForEachItemIn(idx, breakpoints)
    {
        if (breakpoints.item(idx).queryUID()==uid)
            return &breakpoints.item(idx);
    }
    // we don't really expect to get here...
    throwUnexpected();
    return NULL;
}

BreakpointActionMode CBaseDebugContext::checkBreakpoint(DebugState state, IActivityDebugContext *probe, const void *extra)
{
    if (detached)
        return BreakpointActionContinue;
    class ActivityTimer
    {
        unsigned __int64 startCycles;
        unsigned __int64 &accumulator;
    public:
        inline ActivityTimer(unsigned __int64 &_accumulator) : accumulator(_accumulator)
        {
            startCycles = get_cycles_now();
        }
        inline ~ActivityTimer()
        {
            unsigned __int64 elapsed = get_cycles_now() - startCycles;
            accumulator += elapsed;
        }
    } timer(debugCyclesAdjust);
    CriticalBlock b(breakCrit); // Note - this may block for a while if program is suspended!
    

    assertex(running);

    bool stop = false;
    currentNode.clear();
    if (watchState == WatchStateQuit)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Query aborted by debugger");
    if (state==DebugStateEdge)
    {
        // Check whether there is a breakpoint etc active for this activityId
        IBreakpointInfo *bp;
        if (currentBreakpointUID != (unsigned) -1)
            bp = findBreakpoint(currentBreakpointUID);
        else if (probe)
            bp = probe->debuggerCallback(sequence, extra);
        else
            bp = NULL;
        if (bp)
        {
            BreakpointActionMode action = bp->queryAction();
            if (action != BreakpointActionBreak)
                return action;
            stop = true;
            currentBreakpointUID = bp->queryUID();
        }
        else
        {
            switch (watchState)
            {
            case WatchStateStep:
                stop = true; 
                break;
            case WatchStateNext:
                stop = (probe==nextActivity); // MORE - proxy activities need to be compared more carefully
                break;
            case WatchStateOver:
                stop = (probe->queryInputActivity()==nextActivity);  // MORE - doesn't work where multiple inputs or when debugging slave graph - think about proxy too
                break;
            case WatchStateGraph:
            case WatchStateContinue:
                stop = false; 
                break;
            default:
                throwUnexpected();
                break;
            }
        }
    }
    else if (state==DebugStateGraphCreate)
        pendingBreakpointsDone = false;
    else if (state==DebugStateGraphStart || state==DebugStateGraphEnd || state==DebugStateGraphAbort)
    {
        stop = _checkPendingBreakpoints(state, (const char *) extra);
        pendingBreakpointsDone = true;
        switch (watchState)
        {
        case WatchStateGraph:
            stop = true;
            break;
        case WatchStateStep:
        case WatchStateNext:
            stop = true; 
            break;
        case WatchStateContinue:
            // stop already set above...
            break;
        default:
            throwUnexpected();
            break;
        }
#if 0
// I don't think this works, and the global count stuff will supercede it
        if (state==DebugStateGraphEnd || state==DebugStateGraphAbort)
        {
            CommonXmlWriter finalGraphXML(0, 1);
            currentGraph->getXGMML(&finalGraphXML, 0, false);
            if (!completedGraphs)
                completedGraphs.setown(createProperties(false));
            completedGraphs->setProp(currentGraph->queryGraphName(), finalGraphXML.str());
        }
#endif
    }
    else if (state==DebugStateFinished)
    {
        currentGraph = NULL;
        graphChangeSequence++;
        stop = true;
    }
    else if (state==DebugStateException)
    {
        IException *E = (IException *) extra;
        if (E != currentException)
        {
            currentException.set(E);
            stop = true;
        }
        else
            stop = false;
    }
    else if (state==DebugStateLimit)
    {
        if (stopOnLimits)
        {
            stop = true;
            IActivityBase *activity = (IActivityBase *) extra;
            assertex(currentGraph);
            currentNode.setown(currentGraph->getNodeByActivityBase(activity));
        }
    }
    else
        stop = true;
    if (!stop)
        return BreakpointActionContinue;
    {
        CriticalBlock b(debugCrit);
        currentActivity.set(probe);
        running = false;
        currentState = state;
        if (currentActivity)
            logctx.CTXLOG("DEBUG: paused waiting for debugger, state %s, edge %s", queryStateString(state), currentActivity->queryEdgeId());
        else
            logctx.CTXLOG("DEBUG: paused waiting for debugger, state %s, edge '(none)'", queryStateString(state));
        if (debuggerActive) // Was the debugger actively waiting for the program to hit a breakpoint?
        {
            debuggerSem.signal(debuggerActive);
            debuggerActive = 0;
        }
    }
    waitForDebugger(state, probe);
    {
        CriticalBlock b(debugCrit);
        running = true;
        BreakpointActionMode ret = skipRequested ? BreakpointActionSkip : BreakpointActionContinue;
        skipRequested = false;
        currentBreakpointUID = (unsigned) -1;
        return ret;
    }
}

void CBaseDebugContext::noteManager(IDebugGraphManager *mgr)
{
    currentGraph = mgr;
    graphChangeSequence++;
}

void CBaseDebugContext::releaseManager(IDebugGraphManager *mgr)
{
    if(currentGraph==mgr)
        currentGraph = NULL;
    graphChangeSequence++;
}

unsigned CBaseDebugContext::querySequence()
{
    return ++sequence;
}

unsigned CBaseDebugContext::getDefaultHistoryCapacity() const
{
    return defaultHistoryCapacity;
}

bool CBaseDebugContext::getExecuteSequentially() const
{
    return executeSequentially;
}

void CBaseDebugContext::checkDelayedBreakpoints(IActivityDebugContext *edge)
{
    if (pendingBreakpointsDone) // Don't bother if the graph is still being created.... we can do them all more efficiently. 
    {
        // This code is for edges created AFTER the bulk of the graph is created, which missed out on the normal delayed breakpoint setting...
        graphChangeSequence++;
        ForEachItemIn(bpIdx, breakpoints)
        {
            IBreakpointInfo &breakpoint = breakpoints.item(bpIdx);
            if (breakpoint.idMatch(BreakpointModeEdge, edge->queryEdgeId()))
                edge->setBreakpoint(breakpoint);
        }
    }
}

void CBaseDebugContext::serialize(MemoryBuffer &buff) const
{
    buff.append((unsigned short) breakpoints.length());
    ForEachItemIn(idx, breakpoints)
    {
        breakpoints.item(idx).serialize(buff);
    }
    buff.append(running).append(detached).append((unsigned char) watchState).append(sequence).append(skipRequested).append(stopOnLimits);
}

void CBaseDebugContext::deserialize(MemoryBuffer &buff)
{
    // MORE - this is rather inefficient - we remove all breakpoints then reapply. But may be good enough...
    if (currentGraph)
    {
        ForEachItemIn(idx, breakpoints)
        {
            IBreakpointInfo &bp = breakpoints.item(idx);
            currentGraph->removeBreakpoint(bp);
        }
    }
    breakpoints.kill();
    unsigned short numBreaks;
    buff.read(numBreaks);
    while (numBreaks--)
    {
        IBreakpointInfo &bp = *new CBreakpointInfo(buff);
        breakpoints.append(bp);
        if (currentGraph)
            currentGraph->setBreakpoint(bp);
    }
    pendingBreakpointsDone = true;
    unsigned char watchStateChar;
    buff.read(running).read(detached).read(watchStateChar).read(sequence).read(skipRequested).read(stopOnLimits);
    watchState = (WatchState) watchStateChar;
}


//=======================================================================================

void CBaseServerDebugContext::doStandardResult(IXmlWriter *output) const
{
    const char *stateString = running ? "running" : queryStateString(currentState);
    output->outputInt(sequence, "@sequence");
    output->outputString(strlen(stateString), stateString, "@state");
    if (currentNode)
        currentNode->outputId(output, "@nodeId");
    if (currentActivity)
        output->outputCString(currentActivity->queryEdgeId(), "@edgeId");
    if (skipRequested)
        output->outputBool(true, "@skip");
    if (currentGraph)
        output->outputCString(currentGraph->queryGraphName(), "@graphId");
    output->outputInt(graphChangeSequence, "@graphSequenceNum");
    if (graphChangeSequence != prevGraphChangeSequence)
        output->outputBool(true, "@graphChanged");
    if (currentBreakpointUID != (unsigned) -1)
    {
        IBreakpointInfo *bp = findBreakpoint(currentBreakpointUID);
        _listBreakpoint(output, *bp, breakpoints.find(*bp));
    }
    if (currentException)
    {
        output->outputBeginNested("Exception", true);
        output->outputCString("Roxie", "Source");
        output->outputInt(currentException->errorCode(), "Code");
        StringBuffer s;
        currentException->errorMessage(s);
        output->outputString(s.length(), s.str(), "Message");
        output->outputEndNested("Exception");
    }
}

void CBaseServerDebugContext::_listBreakpoint(IXmlWriter *output, IBreakpointInfo &bp, unsigned idx) const
{
    if (bp.queryMode() != BreakpointModeNone)
    {
        output->outputBeginNested("break", true);
        output->outputInt(idx, "@idx");
        bp.toXML(output);
        output->outputEndNested("break");
    }
}

void CBaseServerDebugContext::_continue(WatchState watch) 
{
    if (running)
        throw MakeStringException(THORHELPER_INTERNAL_ERROR, "Query already running");
    if (watch==WatchStateNext || watch==WatchStateOver)
    {
        if (!currentActivity)
            throw MakeStringException(THORHELPER_INTERNAL_ERROR, "next: no current activity");
        nextActivity.set(currentActivity);
    }
    else
        nextActivity.clear();
    watchState = watch;
    previousSequence = sequence;
    prevGraphChangeSequence = graphChangeSequence;
    debuggerActive++;
    debugeeSem.signal();
    {
        CriticalUnblock b(debugCrit);
        debuggerSem.wait();         // MORE - is that actually correct, to release the crit while waiting? Think...
    }
}

unsigned CBaseServerDebugContext::checkOption(const char *supplied, const char *name, const char *accepted[])
{
    if (supplied)
    {
        unsigned idx = 0;
        while (accepted[idx])
        {
            if (strcmp(accepted[idx], supplied)==0)
                return idx;
            idx++;
        }
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Invalid parameter value %s='%s'", name, supplied);
    }
    else
        return 0; // first is default
}

IBreakpointInfo *CBaseServerDebugContext::createDummyBreakpoint()
{
    return new CBreakpointInfo(BreakpointModeNone);
}

IBreakpointInfo *CBaseServerDebugContext::_createBreakpoint(const char *modeString, const char *id, const char *action,
                                   const char *fieldName, const char *condition, const char *value, bool caseSensitive,
                                   unsigned hitCount, const char *hitCountMode)
{
    BreakpointMode mode = (BreakpointMode) checkOption(modeString, "mode", BreakpointModes);
    if (!id)
    {
        if (mode==BreakpointModeEdge)
        {
            if (!currentActivity)
                throw MakeStringException(THORHELPER_DEBUG_ERROR, "No current activity");
            id = currentActivity->queryEdgeId();
        }
        else if (mode==BreakpointModeNode)
        {
            if (!currentActivity)
                throw MakeStringException(THORHELPER_DEBUG_ERROR, "No current activity");
            id = currentActivity->querySourceId();
        }
    }
    return new CBreakpointInfo(mode, id, (BreakpointActionMode) checkOption(action, "action", BreakpointActionModes),
        fieldName, (BreakpointConditionMode) checkOption(condition, "condition", BreakpointConditionModes), value, caseSensitive,
        hitCount, (BreakpointCountMode) checkOption(hitCountMode, "hitCountMode", BreakpointCountModes));
}

void CBaseServerDebugContext::waitForDebugger(DebugState state, IActivityDebugContext *probe)
{
    sequence++;
    while (!debugeeSem.wait(DEBUGEE_TIMEOUT))
    {
        if (onDebuggerTimeout())
            break;
    }
    sequence++;
}

CBaseServerDebugContext::CBaseServerDebugContext(const IContextLogger &_logctx, IPropertyTree *_queryXGMML, SafeSocket &_client) 
    : CBaseDebugContext(_logctx), queryXGMML(_queryXGMML), client(_client)
{
    sequence = 0;
    previousSequence = 0;
    breakpoints.append(*createDummyBreakpoint()); // temporary breakpoint always present
}

void CBaseServerDebugContext::serializeBreakpoints(MemoryBuffer &to)
{
    to.append(breakpoints.length());
    ForEachItemIn(idx, breakpoints)
        breakpoints.item(idx).serialize(to);
}

void CBaseServerDebugContext::debugInitialize(const char *id, const char *_queryName, bool _breakAtStart)
{
    if (!_breakAtStart)
        detached = true;
    debugId.set(id);
    queryName.set(_queryName);
    currentState = DebugStateLoading; 
}

void CBaseServerDebugContext::debugTerminate()
{
    CriticalBlock b(debugCrit);
    assertex(running);
    currentState = DebugStateUnloaded;
    running = false;
    if (debuggerActive)
    {
        debuggerSem.signal(debuggerActive);
        debuggerActive = 0;
    }
}

void CBaseServerDebugContext::addBreakpoint(IXmlWriter *output, const char *modeString, const char *id, const char *action, 
                               const char *fieldName, const char *condition, const char *value, bool caseSensitive,
                               unsigned hitCount, const char *hitCountMode)
{
    CriticalBlock b(debugCrit);
    if (running)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available while query is running");

    Owned<IBreakpointInfo> newBreakpoint = _createBreakpoint(modeString, id, action, fieldName, condition, value, caseSensitive, hitCount, hitCountMode);
    // For breakpoints to be efficient we need to set them in the probe, though because the activity probe may not exist yet, we should also 
    // keep a central list (also makes it easier to do things like list breakpoint info...)
    ForEachItemIn(idx, breakpoints)
    {
        IBreakpointInfo &bp = breakpoints.item(idx);
        if (bp.equals(*newBreakpoint))
            throw MakeStringException(THORHELPER_DEBUG_ERROR, "Breakpoint already exists");
    }
    _listBreakpoint(output, *newBreakpoint, breakpoints.ordinality());
    CBaseDebugContext::addBreakpoint(*newBreakpoint.getClear());
}

void CBaseServerDebugContext::removeBreakpoint(IXmlWriter *output, unsigned removeIdx)
{
    CriticalBlock b(debugCrit);
    if (running)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available while query is running");
    assertex(removeIdx);
    if (!breakpoints.isItem(removeIdx))
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Breakpoint %d does not exist", removeIdx);
    IBreakpointInfo &bp = breakpoints.item(removeIdx);
    _listBreakpoint(output, bp, removeIdx);
    if (currentGraph)
        currentGraph->removeBreakpoint(bp);
    breakpoints.replace(*createDummyBreakpoint(), removeIdx);
}

void CBaseServerDebugContext::removeAllBreakpoints(IXmlWriter *output)
{
    ForEachItemIn(idx, breakpoints)
    {
        IBreakpointInfo &bp = breakpoints.item(idx);
        _listBreakpoint(output, bp, idx);
        if (currentGraph)
            currentGraph->removeBreakpoint(bp);
        breakpoints.replace(*createDummyBreakpoint(), idx);
    }
}

void CBaseServerDebugContext::listBreakpoint(IXmlWriter *output, unsigned listIdx) const
{
    // MORE - fair amount could be commoned up with remove...
    CriticalBlock b(debugCrit);
    if (running)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available while query is running");
    assertex(listIdx);
    if (!breakpoints.isItem(listIdx))
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Breakpoint %d does not exist", listIdx);
    IBreakpointInfo &bp = breakpoints.item(listIdx);
    _listBreakpoint(output, bp, listIdx);
}

void CBaseServerDebugContext::listAllBreakpoints(IXmlWriter *output) const
{
    ForEachItemIn(idx, breakpoints)
    {
        IBreakpointInfo &bp = breakpoints.item(idx);
        _listBreakpoint(output, bp, idx);
    }
}

void CBaseServerDebugContext::debugInterrupt(IXmlWriter *output)
{
    CriticalBlock b(debugCrit);
    if (!running)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Query is already paused"); // MORE - or has terminated?
    detached = false;
    nextActivity.clear();
    watchState = WatchStateStep;
    debuggerActive++;
    {
        CriticalUnblock b(debugCrit);
        debuggerSem.wait(); // MORE - should this be inside critsec? Should it be there at all?
    }
    doStandardResult(output);
}

void CBaseServerDebugContext::debugContinue(IXmlWriter *output, const char *modeString, const char *id)
{
    CriticalBlock b(debugCrit);
    Owned<IBreakpointInfo> tempBreakpoint;
    if (id)
    {
        tempBreakpoint.setown(_createBreakpoint(modeString, id));
        if (currentGraph)
            currentGraph->setBreakpoint(*tempBreakpoint);
        breakpoints.replace(*LINK(tempBreakpoint), 0);
    }
    _continue(WatchStateContinue);
    doStandardResult(output);
    if (tempBreakpoint)
    {
        if (currentGraph)
            currentGraph->removeBreakpoint(*tempBreakpoint);
        breakpoints.replace(*createDummyBreakpoint(), 0);
    }
}

void CBaseServerDebugContext::debugRun(IXmlWriter *output) 
{
    CriticalBlock b(debugCrit);
    if (running)
        throw MakeStringException(THORHELPER_INTERNAL_ERROR, "Query already running");
    watchState = WatchStateContinue;
    detached = true;
    if (currentGraph)
        currentGraph->clearHistories();
    currentState = DebugStateDetached;
    currentActivity.clear();
    currentBreakpointUID = (unsigned) -1;
    previousSequence = sequence;
    debugeeSem.signal();
    doStandardResult(output);
}

void CBaseServerDebugContext::debugQuit(IXmlWriter *output) 
{
    CriticalBlock b(debugCrit);
    detached = false;
    nextActivity.clear();
    watchState = WatchStateQuit;
    currentState = DebugStateQuit;
    currentActivity.clear();
    currentBreakpointUID = (unsigned) -1;
    if (!running)
    {
        debugeeSem.signal();
    }
    doStandardResult(output);
}

void CBaseServerDebugContext::debugSkip(IXmlWriter *output)
{
    CriticalBlock b(debugCrit);
    if (running)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available while query is running");
    if (!currentActivity)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "No current activity");
    skipRequested = true;
    doStandardResult(output);
}

void CBaseServerDebugContext::debugStatus(IXmlWriter *output) const
{
    CriticalBlock b(debugCrit);
    doStandardResult(output);
}

void CBaseServerDebugContext::debugStep(IXmlWriter *output, const char *modeString)
{
    CriticalBlock b(debugCrit);
    WatchState state;
    if (modeString)
    {
        if (strcmp(modeString, "graph")==0)
            state = WatchStateGraph;
        else if (strcmp(modeString, "edge")==0)
            state = WatchStateStep;
        else
            throw MakeStringException(THORHELPER_DEBUG_ERROR, "Step mode should be edge or graph");
    }
    else
        state = WatchStateStep;
    _continue(state);
    doStandardResult(output);
}

void CBaseServerDebugContext::debugNext(IXmlWriter *output)
{
    CriticalBlock b(debugCrit);
    _continue(WatchStateNext);
    doStandardResult(output);
}

void CBaseServerDebugContext::debugOver(IXmlWriter *output)
{
    CriticalBlock b(debugCrit);
    _continue(WatchStateOver);
    doStandardResult(output);
}

void CBaseServerDebugContext::debugChanges(IXmlWriter *output, unsigned sinceSequence) const
{
    CriticalBlock b(debugCrit);
    if (running)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available while query is running");
    if (sinceSequence == (unsigned) -1)
        sinceSequence = previousSequence;
    if (!currentGraph)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available when no graph active");

    // MORE - if current graph has changed since specified sequence, then ??
    currentGraph->getXGMML(output, sinceSequence, true);
}

void CBaseServerDebugContext::debugCounts(IXmlWriter *output, unsigned sinceSequence, bool reset)
{
    CriticalBlock b(debugCrit);
    if (running)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available while query is running");
    if (sinceSequence == (unsigned) -1)
        sinceSequence = previousSequence;
    HashIterator edges(globalCounts);
    ForEach(edges)
    {
        IGlobalEdgeRecord *edge = globalCounts.mapToValue(&edges.query());
        if (!sinceSequence || edge->queryLastSequence() > sinceSequence)
        {
            output->outputBeginNested("edge", true);
            output->outputCString((const char *) edges.query().getKey(), "@edgeId");
            output->outputUInt(edge->queryCount(), "@count");
            output->outputEndNested("edge");
        }
        if (reset)
            edge->reset();
    }
}

void CBaseServerDebugContext::debugWhere(IXmlWriter *output) const
{
    CriticalBlock b(debugCrit);
    if (running)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available while query is running");
    if (!currentActivity)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "No current activity");

    output->outputCString(currentGraph->queryGraphName(), "@graphId");
    output->outputCString(currentActivity->queryEdgeId(), "@edgeId");
    IActivityDebugContext *activityCtx = currentActivity;
    while (activityCtx)
    {
        activityCtx->printEdge(output, 0, 1);
        output->outputEndNested("Edge");
        activityCtx = activityCtx->queryInputActivity();
    }
}

void CBaseServerDebugContext::debugPrintVariable(IXmlWriter *output, const char *name, const char *type) const
{
    throwUnexpected(); // must be implemented by platform-specific derived class
}

void CBaseServerDebugContext::debugSearch(IXmlWriter *output, const char *fieldName, const char *condition, const char *value, bool caseSensitive, bool fullRows) const
{
    CriticalBlock b(debugCrit);
    if (running)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available while query is running");
    if (!currentGraph)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available when no graph active");
    Owned<IRowMatcher> searcher = createRowMatcher(fieldName, (BreakpointConditionMode) checkOption(condition, "condition", BreakpointConditionModes), value, caseSensitive);
    currentGraph->searchHistories(output, searcher, fullRows);
}


void CBaseServerDebugContext::debugPrint(IXmlWriter *output, const char *edgeId, unsigned startRow, unsigned numRows) const
{
    CriticalBlock b(debugCrit);
    if (running)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available while query is running");
    if (!currentGraph)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available when no graph active");
    IActivityDebugContext *activityCtx;
    if (edgeId)
    {
        activityCtx = currentGraph->lookupActivityByEdgeId(edgeId);
        if (!activityCtx)
            throw MakeStringException(THORHELPER_DEBUG_ERROR, "Edge %s not found in current graph", edgeId);
    }
    else if (currentActivity)
        activityCtx = currentActivity;
    else
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "An edge id must be specified if there is no current edge");

    output->outputCString(currentGraph->queryGraphName(), "@graphId");
#if 1
    CommonXmlWriter dummyOutput(0, 0);
    activityCtx->printEdge(&dummyOutput, startRow, numRows); // MORE - need to suppress the <Edge> if we want backward compatibility!
    Owned<IPropertyTree> result = createPTreeFromXMLString(dummyOutput.str());
    if (result)
    {
        Owned<IAttributeIterator> attributes = result->getAttributes();
        ForEach(*attributes)
        {
            output->outputCString(attributes->queryValue(),attributes->queryName());
        }
        output->outputString(0, NULL, NULL);
        Owned<IPropertyTreeIterator> elems = result->getElements("*");
        ForEach(*elems)
        {
            StringBuffer e;
            IPropertyTree &elem = elems->query();
            toXML(&elem, e);
            output->outputQuoted(e.str());
        }
    }

#else
    activityCtx->printEdge(output, startRow, numRows); // MORE - need to suppress the <Edge> if we want backward compatibility!
#endif
}

void CBaseServerDebugContext::debugGetConfig(IXmlWriter *output, const char *name, const char *id) const
{
    CriticalBlock b(debugCrit);
    if (running)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available while query is running");
    if (!name)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "configuration setting name must be supplied");
    if (stricmp(name, "executeSequentially")==0)
    {
        output->outputBool(executeSequentially, "@value");
    }
    else if (stricmp(name, "stopOnLimits")==0)
    {
        output->outputInt(stopOnLimits, "@value");
    }
    else if (stricmp(name, "historySize")==0)
    {
        if (id)
        {
            if (!currentGraph)
                throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available when no graph active");
            IActivityDebugContext *activityCtx = currentGraph->lookupActivityByEdgeId(id);
            if (!activityCtx)
                throw MakeStringException(THORHELPER_DEBUG_ERROR, "Edge %s not found in current graph", id);
            output->outputInt(activityCtx->queryHistoryCapacity(), "@value");
        }
        else
        {
            output->outputInt(defaultHistoryCapacity, "@value");
        }
    }
    else
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Unknown configuration setting '%s'", name);
    output->outputCString(name, "@name");
    if (id) output->outputCString(id, "@id");
}

void CBaseServerDebugContext::debugSetConfig(IXmlWriter *output, const char *name, const char *value, const char *id)
{
    CriticalBlock b(debugCrit);
    if (running)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available while query is running");
    if (!name)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "configuration setting name must be supplied");
    unsigned intval = value ? atoi(value) : 0;
    if (stricmp(name, "executeSequentially")==0)
    {
        if (id)
            throw MakeStringException(THORHELPER_DEBUG_ERROR, "id not supported here");
        if (!value || clipStrToBool(value))
            executeSequentially = true;
        else
            executeSequentially = false;
    }
    else if (stricmp(name, "stopOnLimits")==0)
    {
        if (id)
            throw MakeStringException(THORHELPER_DEBUG_ERROR, "id not supported here");
        if (!value || clipStrToBool(value))
            stopOnLimits = true;
        else
            stopOnLimits = false;
    }
    else if (stricmp(name, "historySize")==0)
    {
        if (!value)
            throw MakeStringException(THORHELPER_DEBUG_ERROR, "No value supplied");
        if (id)
        {
            if (!currentGraph)
                throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available when no graph active");
            IActivityDebugContext *activityCtx = currentGraph->lookupActivityByEdgeId(id);
            if (!activityCtx)
                throw MakeStringException(THORHELPER_DEBUG_ERROR, "Edge %s not found in current graph", id);
            activityCtx->setHistoryCapacity(intval);
        }
        else
        {
            defaultHistoryCapacity = intval;
            if (currentGraph)
                currentGraph->setHistoryCapacity(intval);
        }
    }
    else
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Unknown configuration setting '%s'", name);
    if (name) output->outputCString(name, "@name");
    if (value) output->outputCString(value, "@value");
    if (id) output->outputCString(id, "@id");
}

void CBaseServerDebugContext::getCurrentGraphXGMML(IXmlWriter *output, bool original) const
{
    CriticalBlock b(debugCrit);
    if (running)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available while query is running");
    if (!currentGraph)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "No current graph");
    if (original)
        getGraphXGMML(output, currentGraph->queryGraphName());
    else
    {
#if 0
        if (completedGraphs)
        {
            Owned<IPropertyIterator> iterator = completedGraphs->getIterator();
            iterator->first();
            while (iterator->isValid())
            {
                const char *graphName = iterator->getPropKey();
                const char *graphXML = completedGraphs->queryProp(graphName);
                output->outputString(0, 0, NULL);
                output->outputQuoted(graphXML);
                iterator->next();
            }
        }
#endif
        currentGraph->getXGMML(output, 0, true);
    }
}

void CBaseServerDebugContext::getQueryXGMML(IXmlWriter *output) const
{
    CriticalBlock b(debugCrit);
    if (running)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available while query is running");
    StringBuffer s;
    toXML(queryXGMML, s, 2);
    output->outputString(0, NULL, NULL); // closes any open tags....
    output->outputQuoted(s.str());
}

void CBaseServerDebugContext::getGraphXGMML(IXmlWriter *output, const char *graphName) const
{
    CriticalBlock b(debugCrit);
    if (running)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Command not available while query is running");
    StringBuffer xpath;
    xpath.appendf("Graph[@id='%s']", graphName);
    Owned<IPropertyTree> graph = queryXGMML->getPropTree(xpath.str());
    if (!graph)
        throw MakeStringException(THORHELPER_DEBUG_ERROR, "Graph %s not found", graphName);
    StringBuffer s;
    toXML(graph, s, 2);
    output->outputString(0, NULL, NULL); // closes any open tags....
    output->outputQuoted(s.str());
}

const char *CBaseServerDebugContext::queryQueryName() const
{
    return queryName.get();
}
const char *CBaseServerDebugContext::queryDebugId() const
{
    return debugId.get();
}


//=======================================================================================

DebugActivityRecord *CBaseDebugGraphManager::noteActivity(IActivityBase *activity, unsigned iteration, unsigned channel, unsigned sequence)
{
    Linked<DebugActivityRecord> node = allActivities.getValue(activity);
    if (!node)
    {
        node.setown(new DebugActivityRecord(activity, iteration, channel, sequence));
        allActivities.setValue(activity, node);
    }
    else
    {
        node->iteration = iteration;
        node->channel = channel;
        node->sequence = sequence;
    }
    return node; // note - does not link
}

IDebuggableContext *CBaseDebugGraphManager::queryContext() const
{
    return debugContext;
}

DebugActivityRecord *CBaseDebugGraphManager::getNodeByActivityBase(IActivityBase *activity) const
{
    Linked<DebugActivityRecord> node = allActivities.getValue(activity);
    if (!node)
    {
        ForEachItemIn(idx, childGraphs)
        {
            node.setown(childGraphs.item(idx).getNodeByActivityBase(activity));
            if (node)
                break;
        }
    }
    return node.getClear();
}

void CBaseDebugGraphManager::outputLinksForChildGraph(IXmlWriter *output, const char *parentId)
{
    ForEachItemIn(idx, sinks)
    {
        Linked<DebugActivityRecord> childNode = allActivities.getValue(&sinks.item(idx));
        output->outputBeginNested("edge", true);
        output->outputCString("Child", "@label");
        StringBuffer idText;
        idText.append(childNode->idText).append('_').append(parentId);
        output->outputCString(idText.str(), "@id"); // MORE - is this guaranteed to be unique?
        output->outputBeginNested("att", false); output->outputCString("_childGraph", "@name"); output->outputInt(1, "@value"); output->outputEndNested("att");
        output->outputBeginNested("att", false); output->outputCString("_sourceActivity", "@name"); childNode->outputId(output, "@value"); output->outputEndNested("att"); // MORE!!!
        output->outputBeginNested("att", false); output->outputCString("_targetActivity", "@name"); output->outputCString(parentId, "@value"); output->outputEndNested("att");
        output->outputEndNested("edge");
    }
}

void CBaseDebugGraphManager::outputChildGraph(IXmlWriter *output, unsigned sequence)
{
    if (allActivities.count())
    {
        output->outputBeginNested("graph", true);
        HashIterator edges(allProbes);
        ForEach(edges)
        {
            IActivityDebugContext *edge = allProbes.mapToValue(&edges.query());
            if (!sequence || edge->queryLastSequence() > sequence)
                edge->getXGMML(output);
        }
        HashIterator nodes(allActivities);
        ForEach(nodes)
        {
            DebugActivityRecord *node = allActivities.mapToValue(&nodes.query());
            if (!sequence || node->sequence > sequence) // MORE - if we start reporting any stats on nodes (seeks? scans?) this may need to change
            {
                output->outputBeginNested("node", true);
                node->outputId(output, "@id");
                ThorActivityKind kind = node->activity->getKind();
                StringBuffer kindStr(getActivityText(kind));
                output->outputString(kindStr.length(), kindStr.str(), "@shortlabel");
                if (node->activity->isPassThrough())
                {
                    output->outputBeginNested("att", false); output->outputCString("_isPassthrough", "@name"); output->outputInt(1, "@value"); output->outputEndNested("att");
                }
                if (node->totalCycles)
                {
                    output->outputBeginNested("att", false); 
                    output->outputCString("totalTime", "@name"); 
                    output->outputUInt((unsigned) (cycle_to_nanosec(node->totalCycles)/1000), "@value"); 
                    output->outputEndNested("att");
                }
                if (node->localCycles)
                {
                    output->outputBeginNested("att", false); 
                    output->outputCString("localTime", "@name"); 
                    output->outputUInt((unsigned) (cycle_to_nanosec(node->localCycles)/1000), "@value"); 
                    output->outputEndNested("att");
                }
                node->outputProperties(output);
            }
            if (!sequence || node->sequence > sequence) 
                output->outputEndNested("node");
            ForEachItemIn(idx, node->childGraphs)
            {
                output->outputBeginNested("node", true);
                IDebugGraphManager &childGraph = node->childGraphs.item(idx);
                output->outputCString(childGraph.queryIdString(), "@id");
                childGraph.outputChildGraph(output, sequence);
                output->outputEndNested("node");
                childGraph.outputLinksForChildGraph(output, node->queryIdString());
            }
        }
        output->outputEndNested("graph");
    }
    else
    {
        // This is a bit of a hack. Slave-side graphs have no outer-level node, so we have to do the childgraphs instead
        ForEachItemIn(idx, childGraphs)
        {
            childGraphs.item(idx).outputChildGraph(output, sequence);
        }
    }
}

void CBaseDebugGraphManager::Link() const
{
    CInterface::Link(); 
}
bool CBaseDebugGraphManager::Release() const
{
    if (!IsShared())
    {
        if (!id)
            debugContext->releaseManager(const_cast<CBaseDebugGraphManager*> (this));
    }
    return CInterface::Release();
}

CBaseDebugGraphManager::CBaseDebugGraphManager(IDebuggableContext *_debugContext, unsigned _id, const char *_graphName) : debugContext(_debugContext), id(_id), graphName(_graphName)
{
    if (_graphName)
        debugContext->noteManager(this);
    idString.append(_id);
    proxyId = 0;
}

IDebugGraphManager *CBaseDebugGraphManager::queryDebugManager()
{
    return this;
}

const char *CBaseDebugGraphManager::queryIdString() const
{
    return idString;
}

unsigned CBaseDebugGraphManager::queryId() const
{
    return id;
}

void CBaseDebugGraphManager::noteSink(IActivityBase *sink)
{
    CriticalBlock b(crit);
    sinks.append(*LINK(sink));
}

void CBaseDebugGraphManager::noteDependency(IActivityBase *sourceActivity, unsigned sourceIndex, unsigned controlId, const char *edgeId, IActivityBase *targetActivity)
{
    CriticalBlock b(crit);
    dependencies.append(*new DebugDependencyRecord(sourceActivity, sourceIndex, controlId, edgeId, targetActivity, debugContext->querySequence()));
}

void CBaseDebugGraphManager::setNodeProperty(IActivityBase *node, const char *propName, const char *propValue)
{
    CriticalBlock b(crit);
    Linked<DebugActivityRecord> nodeInfo = allActivities.getValue(node);
    if (nodeInfo)
        nodeInfo->setProperty(propName, propValue, debugContext->querySequence());
    ForEachItemIn(idx, childGraphs)
    {
        childGraphs.item(idx).setNodeProperty(node, propName, propValue);
    }
}

void CBaseDebugGraphManager::setNodePropertyInt(IActivityBase *node, const char *propName, unsigned __int64 propValue)
{
    StringBuffer s;
    s.append(propValue);
    setNodeProperty(node, propName, s.str());
}

void CBaseDebugGraphManager::getProbeResponse(IPropertyTree *query)
{
    throwUnexpected();
}

void CBaseDebugGraphManager::setHistoryCapacity(unsigned newCapacity)
{
    HashIterator edges(allProbes);
    ForEach(edges)
    {
        IActivityDebugContext *edge = allProbes.mapToValue(&edges.query());
        edge->setHistoryCapacity(newCapacity);
    }
}

void CBaseDebugGraphManager::clearHistories()
{
    HashIterator edges(allProbes);
    ForEach(edges)
    {
        IActivityDebugContext *edge = allProbes.mapToValue(&edges.query());
        edge->clearHistory();
    }
}

void CBaseDebugGraphManager::setBreakpoint(IBreakpointInfo &bp)
{
    // May want a hash lookup here, but the non-uniqueness makes it slightly non-trivial so bruteforce for now
    HashIterator edges(allProbes);
    ForEach(edges)
    {
        IActivityDebugContext *edge = allProbes.mapToValue(&edges.query());
        if (bp.queryMode()==BreakpointModeGlobal || bp.idMatch(BreakpointModeEdge, edge->queryEdgeId()))
            edge->setBreakpoint(bp);
    }
    ForEachItemIn(idx, childGraphs)
    {
        childGraphs.item(idx).setBreakpoint(bp);
    }
}

void CBaseDebugGraphManager::mergeRemoteCounts(IDebuggableContext *into) const
{
    ForEachItemIn(idx, childGraphs)
    {
        childGraphs.item(idx).mergeRemoteCounts(into);
    }
}

void CBaseDebugGraphManager::removeBreakpoint(IBreakpointInfo &bp)
{
    // May want a hash lookup here, but the non-uniqueness makes it slightly non-trivial so bruteforce for now
    HashIterator edges(allProbes);
    ForEach(edges)
    {
        IActivityDebugContext *edge = allProbes.mapToValue(&edges.query());
        if (bp.queryMode()==BreakpointModeGlobal || bp.idMatch(BreakpointModeEdge, edge->queryEdgeId()))
            edge->removeBreakpoint(bp);
    }
    ForEachItemIn(idx, childGraphs)
    {
        childGraphs.item(idx).removeBreakpoint(bp);
    }
}

IActivityDebugContext *CBaseDebugGraphManager::lookupActivityByEdgeId(const char *edgeId)
{
    // MORE - if structured ID's, behave differently?
    IActivityDebugContext *edge = allProbes.getValue(edgeId);
    if (!edge)
    {
        ForEachItemIn(idx, childGraphs)
        {
            edge = childGraphs.item(idx).lookupActivityByEdgeId(edgeId);
            if (edge)
                break;
        }
    }
    return edge;
}

const char *CBaseDebugGraphManager::queryGraphName() const
{
    return graphName.get();
}

void CBaseDebugGraphManager::getXGMML(IXmlWriter *output, unsigned sequence, bool isActive)
{
    // Build xgmml for this graph... as currently executing...
    CriticalBlock b(crit);
    output->outputBeginNested("Graph", true);
    output->outputString(graphName.length(), graphName.get(), "@id"); // MORE here
    output->outputBool(isActive, "@active");
    output->outputBeginNested("xgmml", true);

    outputChildGraph(output, sequence);

    ForEachItemIn(dependencyIdx, dependencies)
    {
        DebugDependencyRecord &dependency = dependencies.item(dependencyIdx);
        if (!sequence || dependency.sequence > sequence) // MORE - if we start reporting any stats on nodes (seeks? scans?) this may need to change
        {
            DebugActivityRecord *source = allActivities.getValue(dependency.sourceActivity);
            DebugActivityRecord *target = allActivities.getValue(dependency.targetActivity);
            if (source && target)
            {
                output->outputBeginNested("edge", true);
                output->outputCString(dependency.edgeId, "@id");
                output->outputBeginNested("att", false); output->outputCString("_dependsOn", "@name"); output->outputInt(1, "@value"); output->outputEndNested("att");
                output->outputBeginNested("att", false); output->outputCString("_sourceActivity", "@name"); source->outputId(output, "@value"); output->outputEndNested("att");
                output->outputBeginNested("att", false); output->outputCString("_targetActivity", "@name"); target->outputId(output, "@value"); output->outputEndNested("att");
                output->outputEndNested("edge");
            }
            // MORE - work out why sometimes source and/or target is NULL
        }
    }

    output->outputEndNested("xgmml");
    output->outputEndNested("Graph");
}

void CBaseDebugGraphManager::searchHistories(IXmlWriter *output, IRowMatcher *matcher, bool fullRows)
{
    HashIterator edges(allProbes); // MORE - could find ones that have named field
    ForEach(edges)
    {
        IActivityDebugContext *edge = allProbes.mapToValue(&edges.query());
        edge->searchHistories(output, matcher, fullRows);
    }
    ForEachItemIn(idx, childGraphs)
    {
        IDebugGraphManager &graph = childGraphs.item(idx);
        graph.searchHistories(output, matcher, fullRows);
    }
}

void CBaseDebugGraphManager::serializeProxyGraphs(MemoryBuffer &buff)
{
    buff.append(childGraphs.length());
    ForEachItemIn(idx, childGraphs)
    {
        IDebugGraphManager &graph = childGraphs.item(idx);
        buff.append(graph.queryId());
        buff.append((__uint64)graph.queryProxyId());
    }
}

void CBaseDebugGraphManager::endChildGraph(IProbeManager *child, IActivityBase *parent)
{
    CriticalBlock b(crit);
    if (parent)
    {
        Linked<DebugActivityRecord> node = allActivities.getValue(parent);
        if (!node)
        {
            node.setown(new DebugActivityRecord(parent, 0, debugContext->queryChannel(), 0));
            allActivities.setValue(parent, node);
        }
        IDebugGraphManager *childManager = QUERYINTERFACE(child, CBaseDebugGraphManager); // yuk
        node->childGraphs.append(*LINK(childManager));
    }
}

