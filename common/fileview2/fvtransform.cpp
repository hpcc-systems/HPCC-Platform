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
#include "eclrtl.hpp"

#include "fileview.hpp"
#include "fverror.hpp"
#include "fvrelate.ipp"
#include "deftype.hpp"
#include "fvresultset.ipp"
#include "thorplugin.hpp"

#include "eclrtl_imp.hpp"
#include "hqlfold.hpp"
#include "hqlvalid.hpp"
#include "hqlcollect.hpp"
#include "hqlrepository.hpp"
#include "hqlerror.hpp"
#include "dasess.hpp"

static ViewTransformerRegistry * theTransformerRegistry;
static ITypeInfo * stringType;
static ITypeInfo * utf8Type;
static ITypeInfo * unicodeType;
static IAtom * addAtom;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    addAtom = createLowerCaseAtom("add");
    stringType = makeStringType(UNKNOWN_LENGTH, NULL, NULL);
    utf8Type = makeUtf8Type(UNKNOWN_LENGTH, NULL);
    unicodeType = makeUnicodeType(UNKNOWN_LENGTH, NULL);

    theTransformerRegistry = new ViewTransformerRegistry;
    theTransformerRegistry->addTransformer(new ViewFailTransformer);
    theTransformerRegistry->addTransformer(new ViewAddTransformer);
    
    return true;
}
MODULE_EXIT()
{
    delete theTransformerRegistry;
    unicodeType->Release();
    utf8Type->Release();
    stringType->Release();
}

//---------------------------------------------------------------------------

void CardinalityElement::init(unsigned len, const char * _text)
{
    if (len == 0)
    {
        text.set("1");
        min = 1;
        max = 1;
        return;
    }

    text.set(_text, len);
    const char * dotdot = strstr(text, "..");
    if (dotdot)
    {
        min = rtlStrToUInt4(dotdot-text, text);
        if (stricmp(dotdot+2, "M") == 0)
            max = Unbounded;
        else
            max = rtlVStrToUInt4(dotdot+2);
    }
    else
    {
        if (stricmp(text, "M") == 0)
        {
            min = 0;
            max = Unbounded;
        }
        else
        {
            min = rtlVStrToUInt4(text);
            max = min;
        }
    }
}



void CardinalityMapping::init(const char * text)
{
    const char * colon = strchr(text, ':');
    if (colon)
    {
        primary.init(colon-text, text);
        secondary.init(strlen(colon+1), colon+1);
    }
    else
    {
        //May as well try and make some sense of it
        primary.init(0, "");
        secondary.init(strlen(text), text);
    }
}


void getInvertedCardinality(StringBuffer & out, const char * cardinality)
{
    if (cardinality)
    {
        CardinalityMapping mapping(cardinality);
        out.append(mapping.secondary.text).append(":").append(mapping.primary.text);
    }
}

//---------------------------------------------------------------------------

ViewFieldTransformer * ViewFieldTransformer::bind(const HqlExprArray & args)
{
    return LINK(this);
}

void ViewFieldTransformer::transform(MemoryAttr & utfTarget, const MemoryAttr & utfSrc)
{
    //NB: The system utf8 functions typically take a length, whilst MemoryAttr provide a size.
    unsigned lenTarget;
    char * target;
    const char * source = static_cast<const char *>(utfSrc.get());
    unsigned lenSource = rtlUtf8Length((size32_t)utfSrc.length(), source);

    transform(lenTarget, target, lenSource, source);

    unsigned sizeTarget = rtlUtf8Size(lenTarget, target);
    utfTarget.setOwn(sizeTarget, target);
}


ViewFailTransformer::ViewFailTransformer() : ViewFieldTransformer(failAtom)
{
}

void ViewFailTransformer::transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source)
{
    throwError(FVERR_FailTransformation);
}


ViewExceptionTransformer::ViewExceptionTransformer(IException * _e) : ViewFieldTransformer(failAtom), e(_e)
{
}

void ViewExceptionTransformer::transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source)
{
    throw LINK(e);
}


ViewAddTransformer::ViewAddTransformer() : ViewFieldTransformer(addAtom)
{
}

ViewAddTransformer::ViewAddTransformer(const HqlExprArray & _args) : ViewFieldTransformer(addAtom)
{
    appendArray(args, _args);
}


ViewFieldTransformer * ViewAddTransformer::bind(const HqlExprArray & args)
{
    return new ViewAddTransformer(args);
}

void ViewAddTransformer::transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source)
{
    unsigned __int64 value = rtlUtf8ToInt(lenSource, source);
    if (args.ordinality())
        value += args.item(0).queryValue()->getIntValue();
    rtlInt8ToStrX(lenTarget, target, value);
}

//---------------------------------------------------------------------------

void ViewFieldUtf8Transformer::transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source)
{
    (*function)(lenTarget, target, lenSource, source);
}


void ViewFieldUnicodeTransformer::transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source)
{
    unsigned lenUnicodeSrc;
    unsigned lenUnicodeTarget;
    rtlDataAttr unicodeSrc;
    rtlDataAttr unicodeTarget;

    rtlUtf8ToUnicodeX(lenUnicodeSrc, unicodeSrc.refustr(), lenSource, source);
    (*function)(lenUnicodeTarget, unicodeTarget.refustr(), lenUnicodeSrc, unicodeSrc.getustr());
    rtlUnicodeToUtf8X(lenTarget, target, lenUnicodeTarget, unicodeTarget.getustr());
}


void ViewFieldStringTransformer::transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source)
{
    unsigned lenStringSrc;
    unsigned lenStringTarget;
    rtlDataAttr stringSrc;
    rtlDataAttr stringTarget;

    rtlUtf8ToStrX(lenStringSrc, stringSrc.refstr(), lenSource, source);
    (*function)(lenStringTarget, stringTarget.refstr(), lenStringSrc, stringSrc.getstr());
    rtlStrToUtf8X(lenTarget, target, lenStringTarget, stringTarget.getstr());
}


//---------------------------------------------------------------------------

ViewFieldTransformer * ViewFieldECLTransformer::bind(const HqlExprArray & args)
{
    if (args.ordinality() == 0)
        return LINK(this);
    return new ViewFieldBoundECLTransformer(this, args);
}

void ViewFieldECLTransformer::transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source, const HqlExprArray & extraArgs)
{
    Owned<ITypeInfo> sourceType = makeUtf8Type(lenSource, 0);
    IValue * sourceValue = createUtf8Value(source, LINK(sourceType));
    OwnedHqlExpr sourceExpr = createConstant(sourceValue);
    HqlExprArray actuals;
    actuals.append(*LINK(sourceExpr));
    appendArray(actuals, extraArgs);

    Owned<IErrorReceiver> errorReporter = createThrowingErrorReceiver();
    OwnedHqlExpr call = createBoundFunction(errorReporter, function, actuals, NULL, true);
    OwnedHqlExpr castValue = ensureExprType(call, utf8Type);
    OwnedHqlExpr folded = quickFoldExpression(castValue, NULL, 0);
    IValue * foldedValue = folded->queryValue();
    assertex(foldedValue);
    unsigned len = foldedValue->queryType()->getStringLen();
    const char * data = static_cast<const char *>(foldedValue->queryValue());
    unsigned size = rtlUtf8Size(len, data);
    lenTarget = len;
    target = (char *)rtlMalloc(size);
    memcpy(target, data, size);
}

void ViewFieldECLTransformer::transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source)
{
    HqlExprArray extraArgs;
    transform(lenTarget, target, lenSource, source, extraArgs);
}

void ViewFieldBoundECLTransformer::transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source)
{
    transformer->transform(lenTarget, target, lenSource, source, args);
}

//---------------------------------------------------------------------------


void ViewTransformerRegistry::addTransformer(ViewFieldTransformer * ownedTransformer)
{
    transformers.append(*ownedTransformer);
}

void ViewTransformerRegistry::addFieldUtf8Transformer(const char * name, utf8FieldTransformerFunction func)
{
    transformers.append(* new ViewFieldUtf8Transformer(createLowerCaseAtom(name), func));
}

void ViewTransformerRegistry::addFieldStringTransformer(const char * name, stringFieldTransformerFunction func)
{
    transformers.append(* new ViewFieldStringTransformer(createLowerCaseAtom(name), func));
}

void ViewTransformerRegistry::addFieldUnicodeTransformer(const char * name, unicodeFieldTransformerFunction func)
{
    transformers.append(* new ViewFieldUnicodeTransformer(createLowerCaseAtom(name), func));
}

void ViewTransformerRegistry::addPlugins(const char * name)
{
    loadedPlugins.setown(new SafePluginMap(&pluginCtx, true));
    loadedPlugins->loadFromList(name);

    Owned<IErrorReceiver> errorReporter = createThrowingErrorReceiver();
    dataServer.setown(createNewSourceFileEclRepository(errorReporter, name, ESFallowplugins, 0));

    HqlScopeArray scopes;
    HqlParseContext parseCtx(dataServer, NULL, NULL);
    HqlLookupContext ctx(parseCtx, errorReporter);
    getRootScopes(scopes, dataServer, ctx);

    ForEachItemIn(i, scopes)
    {
        IHqlScope * scope = &scopes.item(i);
        HqlExprArray symbols;
        try
        {
            scope->ensureSymbolsDefined(ctx);
            scope->getSymbols(symbols);

            ForEachItemIn(j, symbols)
            {
                IHqlExpression & cur = symbols.item(j);
                if (cur.getOperator() == no_service)
                    addServiceDefinition(&cur);
            }
        }
        catch (IException * e)
        {
            const char * name = str(scope->queryName());
            VStringBuffer msg("Error loading plugin %s", name);
            EXCLOG(e, msg.str());
        }
    }
}

static bool matchesSimpleTransformer(IHqlExpression * definition, ITypeInfo * type)
{
    if (definition->queryBody()->queryType() != type)
        return false;
    if (definition->numChildren() != 2)
        return false;
    if (definition->queryChild(1)->queryType() != type)
        return false;
    return true;
}

void * ViewTransformerRegistry::resolveExternal(IHqlExpression * funcdef)
{
    IHqlExpression *body = funcdef->queryChild(0);

    StringBuffer entry;
    StringBuffer lib;
    getAttribute(body, entrypointAtom, entry);
    getAttribute(body, libraryAtom, lib);
    if (!lib.length())
        getAttribute(body, pluginAtom, lib);

    ensureFileExtension(lib, SharedObjectExtension);

    Owned<ILoadedDllEntry> match = loadedPlugins->getPluginDll(lib.str(), NULL, false); // MORE - shouldn't it check the version????
    if (!match)
        return NULL;

    return GetSharedProcedure(match->getInstance(), entry.str());
}


ViewFieldTransformer * ViewTransformerRegistry::createTransformer(IHqlExpression * funcdef)
{
    IHqlExpression *body = funcdef->queryChild(0);
    if(!body) 
        return NULL;

    StringBuffer entry;
    StringBuffer lib;
    getAttribute(body, entrypointAtom, entry);
    getAttribute(body, libraryAtom, lib);
    if (!lib.length())
        getAttribute(body, pluginAtom, lib);
    if ((entry.length() == 0) || (lib.length() == 0))
        return NULL;

    if(!body->hasAttribute(pureAtom) && !body->hasAttribute(templateAtom))
        return NULL;

    if(!body->hasAttribute(cAtom))
        return NULL;

    if(body->hasAttribute(gctxmethodAtom) || body->hasAttribute(ctxmethodAtom) || body->hasAttribute(omethodAtom))
        return NULL;

    if(body->hasAttribute(contextAtom) || body->hasAttribute(globalContextAtom))
        return NULL;

    //Special case string->string mapping (e.g., uppercase)
    if (matchesSimpleTransformer(funcdef, stringType))
    {
        stringFieldTransformerFunction resolved = (stringFieldTransformerFunction)resolveExternal(funcdef);
        if (resolved)
            return new ViewFieldStringTransformer(funcdef->queryName(), resolved);
    }

    //Special case string->string mapping (e.g., uppercase)
    if (matchesSimpleTransformer(funcdef, unicodeType))
    {
        unicodeFieldTransformerFunction resolved = (unicodeFieldTransformerFunction)resolveExternal(funcdef);
        if (resolved)
            return new ViewFieldUnicodeTransformer(funcdef->queryName(), resolved);
    }

    //MORE: special case string->string etc.
    return new ViewFieldECLTransformer(funcdef);
}

void ViewTransformerRegistry::addServiceDefinition(IHqlExpression * service)
{
    Owned<ViewServiceEntry> entry = new ViewServiceEntry;
    entry->name.set(str(service->queryName()));

    HqlExprArray symbols;
    service->queryScope()->getSymbols(symbols);
    ForEachItemIn(i, symbols)
    {
        IHqlExpression & cur = symbols.item(i);
        if (cur.getOperator() == no_funcdef && cur.queryChild(0)->getOperator() == no_external)
        {
            ViewFieldTransformer * transformer = createTransformer(&cur);
            if (transformer)
                entry->transformers.append(*transformer);
        }
    }
    plugins.append(*entry.getClear());
}


ViewFieldTransformer * find(const ViewFieldTransformerArray & transformers, const char * name, const HqlExprArray & args)
{
    if (!name)
        return NULL;
    IIdAtom * search = createIdAtom(name);
    ForEachItemIn(i, transformers)
    {
        ViewFieldTransformer & cur = transformers.item(i);
        if (cur.matches(search))
            return cur.bind(args);
    }
    return NULL;
}


ViewFieldTransformer * ViewTransformerRegistry::resolve(const char * name, const HqlExprArray & args)
{
    return find(transformers, name, args);
}

ViewFieldTransformer * ViewTransformerRegistry::resolve(const char * servicename, const char * functionName, const HqlExprArray & args)
{
    if (!servicename)
        return NULL;

    ForEachItemIn(i, plugins)
    {
        ViewServiceEntry & cur = plugins.item(i);
        if (stricmp(servicename, cur.name) == 0)
            return find(cur.transformers, functionName, args);
    }
    return NULL;
}

IViewTransformerRegistry & queryTransformerRegistry()
{
    return *theTransformerRegistry;
}



//---------------------------------------------------------------------------

bool containsFail(const ViewFieldTransformerArray & transforms)
{
    ForEachItemIn(i, transforms)
    {
        if (transforms.item(i).matches(failId))
            return true;
    }
    return false;
}

void translateValue(MemoryAttr & result, const MemoryAttr & filterValue, const ViewFieldTransformerArray & transforms)
{
    unsigned numTransforms = transforms.ordinality();
    if (numTransforms)
    {
        MemoryAttr tempValue[2];
        unsigned whichTarget = 0;
        const MemoryAttr * source = &filterValue;
        for (unsigned i=0; i < numTransforms-1; i++)
        {
            MemoryAttr * target = &tempValue[whichTarget];
            transforms.item(i).transform(*target, *source);
            source = target;
            whichTarget = 1-whichTarget;
        }
        transforms.item(numTransforms-1).transform(result, *source);
    }
    else
        result.set(filterValue.length(), filterValue.get());
}


ViewJoinColumn::ViewJoinColumn(unsigned _whichColumn, const ViewFieldTransformerArray & _getTransforms, const ViewFieldTransformerArray & _setTransforms)
{
    whichColumn = _whichColumn;
    appendArray(getTransforms, _getTransforms);
    appendArray(setTransforms, _setTransforms);
    getContainsFail = containsFail(getTransforms);
    setContainsFail = containsFail(setTransforms);
}

void ViewJoinColumn::addFilter(IFilteredResultSet * resultSet, const MemoryAttr & value)
{
    const MemoryAttr * source = &value;
    MemoryAttr tempValue;
    if (setTransforms.ordinality())
    {
        translateValue(tempValue, value, setTransforms);
        source = &tempValue;
    }
    resultSet->addFilter(whichColumn, (size32_t)source->length(), (const char *)source->get());
}

void ViewJoinColumn::clearFilter(IFilteredResultSet * resultSet)
{
    resultSet->clearFilter(whichColumn);
}

void ViewJoinColumn::getValue(MemoryAttr & value, IResultSetCursor * cursor)
{
    if (getTransforms.ordinality())
    {
        MemoryAttr rowValue;
        MemoryAttr2IStringVal adaptor(rowValue);
        cursor->getDisplayText(adaptor, whichColumn);
        translateValue(value, rowValue, getTransforms);
    }
    else
    {
        MemoryAttr2IStringVal adaptor(value);
        cursor->getDisplayText(adaptor, whichColumn);
    }
}

//---------------------------------------------------------------------------

struct TextReference
{
    TextReference() { len =0; text = NULL; }
    TextReference(size32_t _len, const char * _text) : len(_len), text(_text) {}

    inline bool eq(const char * search) const { return (len == strlen(search)) && (memicmp(text, search, len) == 0); }
    inline void get(StringAttr & target) const { target.set(text, len); }
    void set(size32_t _len, const char * _text) { len = _len; text = _text; }

    IHqlExpression * createIntConstant();
    IHqlExpression * createStringConstant();

    size32_t len;
    const char * text;
};

IHqlExpression * TextReference::createIntConstant()
{
    return createConstant(rtlStrToInt8(len, text));
}

IHqlExpression * TextReference::createStringConstant()
{
    return createConstant(createUnicodeValue(text+1, len-2, "", true, true));
}

class MappingParser
{
    enum { TokEof=256, TokId, TokInt, TokString };
public:
    MappingParser(const IResultSetMetaData & _fieldMeta, bool _datasetSelectorAllowed) : fieldMeta(_fieldMeta), datasetSelectorAllowed(_datasetSelectorAllowed)
    {
        tokenType = TokEof;
        lenInput = 0;
        input = NULL;
        offset = 0;
    }

    void parseColumnMappingList(FieldTransformInfoArray & results, unsigned len, const char * text);

protected:
    unsigned lexToken();
    void assertToken(int expected);
    void getTokenText(StringAttr & target) { curToken.get(target); }

    void parseAttribute(FieldTransformInfo & output);
    void parseColumn(FieldTransformInfo & output);
    void parseColumnMapping(FieldTransformInfo & output);
    void parseConstantList(HqlExprArray & args);
    void parseTransformList(ViewFieldTransformerArray & transforms);

protected:
    const IResultSetMetaData & fieldMeta;
    bool datasetSelectorAllowed;
    unsigned tokenType;
    TextReference curToken;
    unsigned offset;
    unsigned lenInput;
    const char * input;
};

inline bool isLeadingIdentChar(byte next)
{
    return isalpha(next) || (next == '_') || (next == '$');
}

inline bool isTrailingIdentChar(byte next)
{
    return isalnum(next) || (next == '_') || (next == '$');
}

//MORE: This should really use the hqllexer - especially if it gets any more complex!
unsigned MappingParser::lexToken()
{
    const byte * buffer = (const byte *)input;
    unsigned cur = offset;

    while ((cur < lenInput) && isspace(buffer[cur]))
        cur++;

    if (cur < lenInput)
    {
        byte next = buffer[cur];
        if (isLeadingIdentChar(next))
        {
            cur++;
            while ((cur < lenInput) && (isTrailingIdentChar(buffer[cur])))
                cur++;
            tokenType = TokId;
        }
        else if (isdigit(next) ||
                 ((next == '-') && (cur+1 < lenInput) && isdigit(buffer[cur+1])))
        {
            cur++;
            while ((cur < lenInput) && (isdigit(buffer[cur])))
                cur++;
            tokenType = TokInt;
        }
        else if (next == '\'')
        {
            cur++;
            while (cur < lenInput)
            {
                byte next = buffer[cur];
                if (next == '\'')
                    break;
                else if (next == '\\')
                {
                    if (cur+1 < lenInput)
                        cur++;
                }
                cur++;
            }

            if (cur == lenInput)
                throwError2(FVERR_BadStringTermination, cur-offset, input+offset);
            cur++;
            tokenType = TokString;
        }
        else
        {
            tokenType = next;
            cur++;
        }
    }
    else
    {
        tokenType = TokEof;
    }

    curToken.set(cur-offset, input+offset);
    offset = cur;
    return tokenType;
}

void MappingParser::assertToken(int expected)
{
    if (tokenType != expected)
    {
        StringBuffer id;
        switch (expected)
        {
        case TokId:
            id.append("identifier");
            break;
        default:
            id.append((char)expected);
            break;
        }
        unsigned len = lenInput-offset;
        if (len>10) len = 10;
        throwError3(FVERR_ExpectedX, id.str(), len, input+offset);
    }
}

void MappingParser::parseColumn(FieldTransformInfo & output)
{
    output.datasetColumn = NotFound;
    output.column = NotFound;

    unsigned firstFieldIndex = 0;
    const IResultSetMetaData * curMeta = &fieldMeta;
    loop
    {
        StringAttr fieldName;
        assertToken(TokId);
        getTokenText(fieldName);
        lexToken();

        //Cheat and cast the meta so the field lookup can be done more efficiently
        const CResultSetMetaData & castFieldMeta = static_cast<const CResultSetMetaData &>(*curMeta);
        unsigned matchColumn = castFieldMeta.queryColumnIndex(firstFieldIndex, fieldName);
        if (matchColumn == NotFound)
            throwError1(FVERR_UnrecognisedFieldX, fieldName.get());

        DisplayType kind = fieldMeta.getColumnDisplayType(matchColumn);
        switch (kind)
        {
        case TypeBoolean:
        case TypeInteger:
        case TypeUnsignedInteger:
        case TypeReal:
        case TypeString:
        case TypeData:
        case TypeUnicode:
        case TypeUnknown:
        case TypeSet:
            output.column = matchColumn;
            break;
        case TypeBeginRecord:
            //Restrict the search fields to the contents of the record.
            firstFieldIndex = matchColumn+1;
            break;
        case TypeDataset:
            {
                if (!datasetSelectorAllowed)
                    throwError1(FVERR_CannotSelectFromDatasetX, fieldName.get());
                if (output.datasetColumn != NotFound)
                    throwError1(FVERR_CannotSelectManyFromDatasetX, fieldName.get());

                firstFieldIndex = 0;
                curMeta = curMeta->getChildMeta(matchColumn);
                output.datasetColumn = matchColumn;
                break;
            }
        default:
            throwUnexpected();
        }

        if (output.column != NotFound)
            return;

        assertToken('.');
        lexToken();
    }
}

void MappingParser::parseConstantList(HqlExprArray & args)
{
    loop
    {
        switch (tokenType)
        {
        case TokInt:
            args.append(*curToken.createIntConstant());
            break;
        case TokString:
            args.append(*curToken.createStringConstant());
            break;
        default:
            unsigned len = lenInput - offset > 10 ? 10 : lenInput - offset;
            throwError3(FVERR_ExpectedX, "int or string constant", len, input+offset);
        }
        lexToken();
        if (tokenType != ',')
            return;
        lexToken();
    }
}

void MappingParser::parseTransformList(ViewFieldTransformerArray & transforms)
{
    loop
    {
        assertToken(TokId);

        StringAttr mappingName, childName, grandName;
        curToken.get(mappingName);
        lexToken();

        if (tokenType == '.')
        {
            lexToken();
            assertToken(TokId);
            curToken.get(childName);
            lexToken();
        }

        if (tokenType == '.')
        {
            lexToken();
            assertToken(TokId);
            curToken.get(grandName);
            lexToken();
        }

        HqlExprArray args;
        if (tokenType == '(')
        {
            lexToken();
            if (tokenType != ')')
                parseConstantList(args);
            assertToken(')');
            lexToken();
        }

        ViewFieldTransformer * transform;
        try
        {
            if (childName)
            {
                transform = theTransformerRegistry->resolve(mappingName, childName, args);
                if (!transform)
                {
                    //Maybe they specified the module name - should provide a 3 valued lookup
                    transform = theTransformerRegistry->resolve(childName, grandName, args);
                }
                if (!transform)
                    throwError2(FVERR_UnrecognisedMappingFunctionXY, mappingName.get(), childName.get());
            }
            else
            {
                transform = theTransformerRegistry->resolve(mappingName, args);
                if (!transform)
                    throwError1(FVERR_UnrecognisedMappingFunctionX, mappingName.get());
            }
        }
        catch (IException * e)
        {
            EXCLOG(e, "Processing field mapping");
            transform = new ViewExceptionTransformer(e);
            e->Release();
        }

        transforms.append(*transform);

        if (tokenType != ',')
            break;

        lexToken();
    }
}

void MappingParser::parseAttribute(FieldTransformInfo & output)
{
    assertToken(TokId);
    if (curToken.eq("get"))
    {
        lexToken();
        assertToken('(');
        lexToken();
        parseTransformList(output.getTransforms);
        assertToken(')');
        lexToken();
    }
    else if (curToken.eq("set"))
    {
        lexToken();
        assertToken('(');
        lexToken();
        parseTransformList(output.setTransforms);
        assertToken(')');
        lexToken();
    }
    else if (curToken.eq("displayname"))
    {
        lexToken();
        assertToken('(');
        lexToken();
        assertToken(TokId); // could allow a string I guess
        curToken.get(output.naturalName);
        lexToken();
        assertToken(')');
        lexToken();
    }
    else
    {
        unsigned len = lenInput-offset;
        if (len>10) len = 10;
        throwError3(FVERR_ExpectedX, "Definition name", len, input+offset);
    }
}

void MappingParser::parseColumnMapping(FieldTransformInfo & output)
{
    parseColumn(output);
    int endToken = '}';
    //be flexible and allow () or {}?
    if (tokenType == '(')
        endToken = ')';
    else if (tokenType != '{')
        return;
    lexToken();

    if (tokenType != endToken)
    {
        loop
        {
            parseAttribute(output);

            if (tokenType != ',')
                break;

            lexToken();
        }
    }

    assertToken(endToken);
    lexToken();
}


void MappingParser::parseColumnMappingList(FieldTransformInfoArray & results, unsigned len, const char * text)
{
    lenInput = len;
    input = text;
    offset = 0;
    lexToken();
    if (tokenType == TokEof)
        return;

    loop
    {
        FieldTransformInfo * next = new FieldTransformInfo;
        results.append(*next);
        parseColumnMapping(*next);

        if (tokenType == TokEof)
            break;

        assertToken(',');
        lexToken();
    }
}


void parseColumnMappingList(FieldTransformInfoArray & results,
                            const IResultSetMetaData & fieldMeta,
                            bool isDatasetAllowed,      // if non null dataset.x is allowed, and column returned via pointer
                            const char * text)
{
    MappingParser parser(fieldMeta, isDatasetAllowed);
    parser.parseColumnMappingList(results, strlen(text), text);
}


void parseFileColumnMapping(FieldTransformInfoArray & results, const char * text, const IResultSetMetaData & fieldMeta)
{
    MappingParser parser(fieldMeta, false);
    parser.parseColumnMappingList(results, strlen(text), text);
}


static void test()
{
    HqlExprArray args;
    MemoryAttr source;
    source.set(26,"Gavin H\303\243lliday !!\316\261\316\221\307\272!!");

    {
        Owned<ViewFieldTransformer> transform = theTransformerRegistry->resolve("stringlib","StringToUpperCase",args);
        MemoryAttr target;
        transform->transform(target, source);
    }

    {
        Owned<ViewFieldTransformer> transform = theTransformerRegistry->resolve("unicodelib","UnicodeToUpperCase",args);
        MemoryAttr target;
        transform->transform(target, source);
    }
    source.get();
}
