/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#include "jstring.hpp"
#include "jdebug.hpp"
#include "jptree.hpp"
#include "jexcept.hpp"
#include "jlog.hpp"

#include <libxml/xmlmemory.h>
#include <libxml/parserInternals.h>
#include <libxml/debugXML.h>
#include <libxml/HTMLtree.h>
#include <libxml/xmlIO.h>
#include <libxml/xinclude.h>
#include <libxml/catalog.h>
#include <libxml/xpathInternals.h>
#include <libxml/xpath.h>
#include <libxml/xmlschemas.h>
#include <libxml/hash.h>

#include "xpathprocessor.hpp"
#include "xmlerror.hpp"

#include "espcontext.hpp"
#include "esdl_script.hpp"
#include "tokenserialization.hpp"
#include "txsummary.hpp"

//only support libxml2 for now

void addFeaturesToAccessMap(MapStringTo<SecAccessFlags> &accessmap, const char *s)
{
    StringArray entries;
    entries.appendList(s, ",");

    ForEachItemIn(i, entries)
    {
        StringArray pair;
        pair.appendList(entries.item(i), ":");
        if (pair.length()==0)
            continue;
        if (pair.length()==1)
            accessmap.setValue(pair.item(0), SecAccess_Read);
        else
        {
            SecAccessFlags required = getSecAccessFlagValue(pair.item(1));
            if (required >= SecAccess_None)
                accessmap.setValue(pair.item(0), required);
        }
    }
}

inline IEsdlScriptContext *queryEsdlScriptContext(xmlXPathParserContextPtr ctxt)
{
    if (!ctxt || !ctxt->context || !ctxt->context->userData)
        return nullptr;

    return reinterpret_cast<IEsdlScriptContext *>(ctxt->context->userData);
}

inline IEspContext *queryEspContext(xmlXPathParserContextPtr ctxt)
{
    IEsdlScriptContext *scriptContext = queryEsdlScriptContext(ctxt);
    if (!scriptContext)
        return nullptr;
    return scriptContext->queryEspContext();
}

/**
 * validateFeaturesAccessFunction:
 * @ctxt:  an XPath parser context
 * @nargs:  the number of arguments
 *
 * Wraps IEspContext::validateFeaturesAccess()
 */
static void validateFeaturesAccessFunction (xmlXPathParserContextPtr ctxt, int nargs)
{
    IEspContext *espContext = queryEspContext(ctxt);
    if (!espContext)
    {
        xmlXPathSetError((ctxt), XPATH_INVALID_CTXT);
        return;
    }

    if (nargs != 1)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *authstring = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt)) //includes null check
        return;

    MapStringTo<SecAccessFlags> accessmap;
    addFeaturesToAccessMap(accessmap, (const char *)authstring);

    bool ok = true;
    if (accessmap.ordinality()!=0)
        ok = espContext->validateFeaturesAccess(accessmap, false);

    if (authstring != nullptr)
        xmlFree(authstring);

    xmlXPathReturnBoolean(ctxt, ok ? 1 : 0);
}

/**
 * evaluateSecAccessFlagsFunction
 * @ctxt:  an XPath parser context
 * @nargs:  the number of arguments
 *
 */
static void secureAccessFlagsFunction (xmlXPathParserContextPtr ctxt, int nargs)
{
    IEspContext *espContext = queryEspContext(ctxt);
    if (!espContext)
    {
        xmlXPathSetError((ctxt), XPATH_INVALID_CTXT);
        return;
    }

    if (nargs == 0)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    unsigned flags = 0;
    while(nargs--)
    {
        xmlChar *s = xmlXPathPopString(ctxt);
        if (xmlXPathCheckError(ctxt)) //includes null check
            return;
        SecAccessFlags f = getSecAccessFlagValue((const char *)s);
        xmlFree(s);
        if (f < SecAccess_None)
        {
            xmlXPathSetArityError(ctxt);
            return;
        }
        flags |= (unsigned)f;
    }

    xmlXPathReturnNumber(ctxt, flags);
}
/**
 * getFeatureSecAccessFlags
 * @ctxt:  an XPath parser context
 * @nargs:  the number of arguments
 *
 */
static void getFeatureSecAccessFlagsFunction (xmlXPathParserContextPtr ctxt, int nargs)
{
    IEspContext *espContext = queryEspContext(ctxt);
    if (!espContext)
    {
        xmlXPathSetError((ctxt), XPATH_INVALID_CTXT);
        return;
    }

    if (nargs != 1)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *authstring = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt)) //includes null check
        return;

    SecAccessFlags access = SecAccess_None;
    espContext->authorizeFeature((const char *)authstring, access);
    xmlFree(authstring);

    xmlXPathReturnNumber(ctxt, access);
}

/**
 * getStoredStringValueFunction
 * @ctxt:  an XPath parser context
 * @nargs:  the number of arguments
 *
 */
static void getStoredStringValueFunction (xmlXPathParserContextPtr ctxt, int nargs)
{
    IEsdlScriptContext *scriptContext = queryEsdlScriptContext(ctxt);
    if (!scriptContext)
    {
        xmlXPathSetError((ctxt), XPATH_INVALID_CTXT);
        return;
    }

    if (nargs != 1)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *namestring = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt)) //includes null check
        return;

    const char *value = scriptContext->queryAttribute(ESDLScriptCtxSection_Store, (const char *)namestring);
    xmlFree(namestring);
    if (!value)
        xmlXPathReturnEmptyString(ctxt);
    else
        xmlXPathReturnString(ctxt, xmlStrdup((const xmlChar *)value));
}

/**
 * scriptGetDataSectionFunctionImpl
 * @ctxt:  an XPath parser context
 * @nargs:  the number of arguments
 *
 */
static void scriptGetDataSectionFunctionImpl (xmlXPathParserContextPtr ctxt, int nargs, bool ensure)
{
    IEsdlScriptContext *scriptContext = queryEsdlScriptContext(ctxt);
    if (!scriptContext)
    {
        xmlXPathSetError((ctxt), XPATH_INVALID_CTXT);
        return;
    }

    if (nargs != 1)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *namestring = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt)) //includes null check
        return;
    const char *sectionName = isEmptyString((const char *) namestring) ? "temporaries" : (const char *) namestring;

    if (ensure)
        scriptContext->appendContent(sectionName, nullptr, nullptr);

    StringBuffer xpath("/esdl_script_context/");
    xpath.append((const char *) sectionName);

    xmlFree(namestring);

    xmlXPathObjectPtr ret = xmlXPathEval((const xmlChar *) xpath.str(), ctxt->context);
    if (ret)
        valuePush(ctxt, ret);
    else
        xmlXPathReturnEmptyNodeSet(ctxt);
}


/**
 * scriptEnsureDataSectionFunction
 * @ctxt:  an XPath parser context
 * @nargs:  the number of arguments
 *
 */
static void scriptEnsureDataSectionFunction (xmlXPathParserContextPtr ctxt, int nargs)
{
    scriptGetDataSectionFunctionImpl (ctxt, nargs, true);
}

/**
 * scriptGetDataSectionFunction
 * @ctxt:  an XPath parser context
 * @nargs:  the number of arguments
 *
 */
static void scriptGetDataSectionFunction (xmlXPathParserContextPtr ctxt, int nargs)
{
    scriptGetDataSectionFunctionImpl (ctxt, nargs, false);
}

/**
 * getLogOptionFunction
 * @ctxt:  an XPath parser context
 * @nargs:  the number of arguments
 *
 */
static void getLogOptionFunction (xmlXPathParserContextPtr ctxt, int nargs)
{
    IEsdlScriptContext *scriptContext = queryEsdlScriptContext(ctxt);
    if (!scriptContext)
    {
        xmlXPathSetError((ctxt), XPATH_INVALID_CTXT);
        return;
    }

    if (nargs != 1)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *namestring = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt)) //includes null check
        return;

    const char *value = scriptContext->queryAttribute(ESDLScriptCtxSection_Logging, (const char *)namestring);
    xmlFree(namestring);
    if (!value)
        xmlXPathReturnEmptyString(ctxt);
    else
        xmlXPathReturnString(ctxt, xmlStrdup((const xmlChar *)value));
}

/**
 * getLogProfileFunction
 * @ctxt:  an XPath parser context
 * @nargs:  the number of arguments
 *
 */
static void getLogProfileFunction (xmlXPathParserContextPtr ctxt, int nargs)
{
    IEsdlScriptContext *scriptContext = queryEsdlScriptContext(ctxt);
    if (!scriptContext)
    {
        xmlXPathSetError((ctxt), XPATH_INVALID_CTXT);
        return;
    }

    if (nargs != 0)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    const char *value = scriptContext->queryAttribute(ESDLScriptCtxSection_Logging, "profile");
    if (!value)
        xmlXPathReturnEmptyString(ctxt);
    else
        xmlXPathReturnString(ctxt, xmlStrdup((const xmlChar *)value));
}

/**
 * logOptionExistsFunction
 * @ctxt:  an XPath parser context
 * @nargs:  the number of arguments
 *
 */
static void logOptionExistsFunction (xmlXPathParserContextPtr ctxt, int nargs)
{
    IEsdlScriptContext *scriptContext = queryEsdlScriptContext(ctxt);
    if (!scriptContext)
    {
        xmlXPathSetError((ctxt), XPATH_INVALID_CTXT);
        return;
    }

    if (nargs != 1)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *namestring = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt)) //includes null check
        return;

    const char *value = scriptContext->queryAttribute(ESDLScriptCtxSection_Logging, (const char *)namestring);
    xmlFree(namestring);

    xmlXPathReturnBoolean(ctxt, (!value) ? 0 : 1);
}

/**
 * storedValueExistsFunction
 * @ctxt:  an XPath parser context
 * @nargs:  the number of arguments
 *
 */
static void storedValueExistsFunction (xmlXPathParserContextPtr ctxt, int nargs)
{
    IEsdlScriptContext *scriptContext = queryEsdlScriptContext(ctxt);
    if (!scriptContext)
    {
        xmlXPathSetError((ctxt), XPATH_INVALID_CTXT);
        return;
    }

    if (nargs != 1)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *namestring = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt)) //includes null check
        return;

    const char *value = scriptContext->queryAttribute(ESDLScriptCtxSection_Store, (const char *)namestring);
    xmlFree(namestring);

    xmlXPathReturnBoolean(ctxt, (!value) ? 0 : 1);
}

//esdl tokenize function will create temporaries in the root/temp section/node of the document
//so this is not a general purpose function in that sense
//this section should be cleared after every script runs
//we may allow overriding the storage location in the future
//
static void strTokenizeFunction(xmlXPathParserContextPtr ctxt, int nargs)
{
    IEsdlScriptContext *scriptContext = queryEsdlScriptContext(ctxt);
    if (!scriptContext)
    {
        xmlXPathSetError((ctxt), XPATH_INVALID_CTXT);
        return;
    }

    if ((nargs < 1) || (nargs > 2))
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *delimiters;
    if (nargs == 2)
    {
        delimiters = xmlXPathPopString(ctxt);
        if (xmlXPathCheckError(ctxt))
            return;
    }
    else
    {
        delimiters = xmlStrdup((const xmlChar *) "\t\r\n ");
    }

    if (delimiters == NULL)
        return;

    xmlChar *str = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt) || (str == NULL))
    {
        if (str)
            xmlFree(str);
        xmlFree(delimiters);
        return;
    }

    StringBuffer resultPath;
    if (!scriptContext->tokenize((const char *)str, (const char *)delimiters, resultPath))
    {
        xmlFree(str);
        xmlFree(delimiters);
        xmlXPathSetError((ctxt), XPATH_EXPR_ERROR);
        return;
    }

    xmlXPathObjectPtr ret = xmlXPathEval((const xmlChar *) resultPath.str(), ctxt->context);
    if (ret != NULL)
        valuePush(ctxt, ret);
    else
        valuePush(ctxt, xmlXPathNewNodeSet(NULL));

    xmlFree(str);
    xmlFree(delimiters);
}

static void deprecatedEncryptString(xmlXPathParserContextPtr ctxt, int nargs)
{
    if (nargs != 1)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *toEncrypt = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt)) //includes null check
        return;

    StringBuffer encrypted;
    try
    {
        encrypt(encrypted, (const char*)toEncrypt);
    }
    catch (IException* e)
    {
        e->Release();
        xmlXPathSetError(ctxt, XPATH_EXPR_ERROR);
    }
    catch (...)
    {
        xmlXPathSetError(ctxt, XPATH_EXPR_ERROR);
    }
    if (!xmlXPathCheckError(ctxt))
        xmlXPathReturnString(ctxt, xmlStrdup((const xmlChar *)encrypted.str()));
    xmlFree(toEncrypt);
}

static void deprecatedDecryptString(xmlXPathParserContextPtr ctxt, int nargs)
{
    if (nargs != 1)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *toDecrypt = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt)) //includes null check
        return;

    StringBuffer decrypted;
    try
    {
        decrypt(decrypted, (const char*)toDecrypt);
    }
    catch (IException* e)
    {
        e->Release();
        xmlXPathSetError(ctxt, XPATH_EXPR_ERROR);
    }
    catch (...)
    {
        xmlXPathSetError(ctxt, XPATH_EXPR_ERROR);
    }
    if (!xmlXPathCheckError(ctxt))
        xmlXPathReturnString(ctxt, xmlStrdup((const xmlChar *)decrypted.str()));
    xmlFree(toDecrypt);
}

static void encodeBase64String(xmlXPathParserContextPtr ctxt, int nargs)
{
    if (0 == nargs || nargs > 2)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *toEncode = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt)) //includes null check
        return;
    bool lineBreaks = false;
    if (2 == nargs)
    {
        lineBreaks = xmlXPathPopBoolean(ctxt);
        if (xmlXPathCheckError(ctxt))
        {
            xmlFree(toEncode);
            return;
        }
    }

    StringBuffer encoded;
    JBASE64_Encode((const char*)toEncode, long(strlen((const char*)toEncode)), encoded, lineBreaks);
    xmlXPathReturnString(ctxt, xmlStrdup((const xmlChar *)encoded.str()));
    xmlFree(toEncode);
}

static void decodeBase64String(xmlXPathParserContextPtr ctxt, int nargs)
{
    if (nargs != 1)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *toDecode = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt)) //includes null check
        return;

    StringBuffer decoded;
    JBASE64_Decode((const char*)toDecode, decoded);
    xmlXPathReturnString(ctxt, xmlStrdup((const xmlChar *)decoded.str()));
    xmlFree(toDecode);
}

static void escapeXmlCharacters(xmlXPathParserContextPtr ctxt, int nargs)
{
    if (nargs != 1)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *toEncode = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt)) //includes null check
        return;

    StringBuffer encoded;
    encodeXML((const char*)toEncode, encoded);
    xmlXPathReturnString(ctxt, xmlStrdup((const xmlChar *)encoded.str()));
    xmlFree(toEncode);
}

static void unescapeXmlCharacters(xmlXPathParserContextPtr ctxt, int nargs)
{
    if (nargs != 1)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *toDecode = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt)) //includes null check
        return;

    StringBuffer decoded;
    decodeXML((const char*)toDecode, decoded);
    xmlXPathReturnString(ctxt, xmlStrdup((const xmlChar *)decoded.str()));
    xmlFree(toDecode);
}

static void compressString(xmlXPathParserContextPtr ctxt, int nargs)
{
    if (nargs != 1)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *toCompress = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt))
        return;

    size32_t len = size32_t(strlen((const char *)toCompress)) + 1;
    MemoryBuffer compressed;
    compressToBuffer(compressed, len, (void *)toCompress);
    StringBuffer encoded;
    JBASE64_Encode((void*)compressed.bufferBase(), compressed.length(), encoded, false);
    xmlXPathReturnString(ctxt, xmlStrdup((const xmlChar *)encoded.str()));

    xmlFree(toCompress);
}

static void decompressString(xmlXPathParserContextPtr ctxt, int nargs)
{
    if (nargs != 1)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlChar *toDecode = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt))
        return;
    if (isEmptyString((const char*)toDecode))
    {
        xmlXPathSetError(ctxt, XPATH_EXPR_ERROR);
        return;
    }

    MemoryBuffer toDecompress;
    JBASE64_Decode((const char *)toDecode, toDecompress);
    if (!toDecompress.length())
    {
        xmlXPathSetError(ctxt, XPATH_EXPR_ERROR);
        return;
    }

    try
    {
        MemoryBuffer decompressed;
        decompressToBuffer(decompressed, toDecompress);
        xmlXPathReturnString(ctxt, xmlStrdup(decompressed.bytes()));
    }
    catch (IException* e)
    {
        e->Release();
        xmlXPathSetError(ctxt, XPATH_EXPR_ERROR);
    }
    catch (...)
    {
        xmlXPathSetError(ctxt, XPATH_EXPR_ERROR);
    }

    xmlFree(toDecode);
}

static void toXmlString(xmlXPathParserContextPtr ctxt, int nargs)
{
    if (nargs != 1)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    xmlNodeSetPtr toSerialize = xmlXPathPopNodeSet(ctxt);
    if (xmlXPathCheckError(ctxt))
        return;

    StringBuffer xml;
    for (int i = 0; i < toSerialize->nodeNr; i++)
    {
        xmlNodePtr node = toSerialize->nodeTab[i];
        if (!node)
            continue;
        xmlOutputBufferPtr xmlOut = xmlAllocOutputBuffer(nullptr);
        xmlNodeDumpOutput(xmlOut, node->doc, node, 0, 1, nullptr);
        xmlOutputBufferFlush(xmlOut);
        xmlBufPtr buf = (xmlOut->conv != nullptr) ? xmlOut->conv : xmlOut->buffer;
        if (xmlBufUse(buf))
            xml.append(xmlBufUse(buf), (const char *)xmlBufContent(buf));
        xmlOutputBufferClose(xmlOut);
    }
    xmlXPathReturnString(ctxt, xmlStrdup((const xmlChar *)xml.str()));

    xmlXPathFreeNodeSet(toSerialize);
}

static void getTxSummary(xmlXPathParserContextPtr ctxt, int nargs)
{
    IEsdlScriptContext *scriptContext = queryEsdlScriptContext(ctxt);
    CTxSummary* txSummary = nullptr;
    if (scriptContext)
    {
        IEspContext* espContext = scriptContext->queryEspContext();
        if (espContext)
            txSummary = espContext->queryTxSummary();
    }
    if (!txSummary)
    {
        xmlXPathSetError((ctxt), XPATH_INVALID_CTXT);
        return;
    }

    if (nargs > 3)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    unsigned style = TXSUMMARY_OUT_TEXT;
    LogLevel level = LogMin;
    unsigned group = TXSUMMARY_GRP_ENTERPRISE;
    xmlChar* tmp;
    if (3 <= nargs)
    {
        tmp = xmlXPathPopString(ctxt);
        if (xmlXPathCheckError(ctxt))
            return;
        if (*tmp)
        {
            if (strieq((const char*)tmp, "core"))
                group = TXSUMMARY_GRP_CORE;
            else if (!streq((const char*)tmp, "enterprise"))
            {
                xmlFree(tmp);
                xmlXPathSetError((ctxt), XPATH_EXPR_ERROR);
                return;
            }
        }
        xmlFree(tmp);
    }
    if (2 <= nargs)
    {
        tmp = xmlXPathPopString(ctxt);
        if (xmlXPathCheckError(ctxt))
            return;
        if (*tmp)
        {
            if (strieq((const char*)tmp, "normal") || streq((const char*)tmp, "5"))
                level = LogNormal;
            else if (strieq((const char*)tmp, "max") || streq((const char*)tmp, "10"))
                level = LogMax;
            else if (!strieq((const char*)tmp, "min"))
            {
                if (TokenDeserializer().deserialize((const char*)tmp, level) != Deserialization_SUCCESS || level < LogMin || level > LogMax)
                {
                    xmlFree(tmp);
                    xmlXPathSetError((ctxt), XPATH_EXPR_ERROR);
                    return;
                }
            }
        }
        xmlFree(tmp);
    }
    if (1 <= nargs)
    {
        tmp = xmlXPathPopString(ctxt);
        if (xmlXPathCheckError(ctxt))
            return;
        if (*tmp)
        {
            if (strieq((const char*)tmp, "json"))
                style = TXSUMMARY_OUT_JSON;
            else if (!strieq((const char*)tmp, "text"))
            {
                xmlFree(tmp);
                xmlXPathSetError((ctxt), XPATH_EXPR_ERROR);
                return;
            }
        }
        xmlFree(tmp);
    }

    StringBuffer output;
    txSummary->serialize(output, level, group, style);
    xmlXPathReturnString(ctxt, xmlStrdup((const xmlChar*)output.str()));
}

static void maskValue(xmlXPathParserContextPtr ctxt, int nargs)
{
    IEsdlScriptContext *scriptContext = queryEsdlScriptContext(ctxt);
    if (!scriptContext)
    {
        xmlXPathSetError((ctxt), XPATH_INVALID_CTXT);
        return;
    }
    Owned<IDataMaskingProfileContext> masker(scriptContext->getMasker());

    if (nargs < 2 || nargs > 3)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    StringBuffer value, valueType, maskStyle;
    xmlChar* tmp;
    if (3 <= nargs)
    {
        tmp = xmlXPathPopString(ctxt);
        if (xmlXPathCheckError(ctxt))
            return;
        maskStyle.append((const char*)tmp);
        xmlFree(tmp);
    }

    tmp = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt))
        return;
    valueType.append((const char*)tmp);
    xmlFree(tmp);

    tmp = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt))
        return;
    value.append((const char*)tmp);
    xmlFree(tmp);

    if (masker)
        masker->maskValue(valueType, maskStyle, const_cast<char*>(value.str()), 0, value.length(), false);

    xmlXPathReturnString(ctxt, xmlStrdup((const xmlChar*)value.str()));
}

static void maskContent(xmlXPathParserContextPtr ctxt, int nargs)
{
    IEsdlScriptContext *scriptContext = queryEsdlScriptContext(ctxt);
    if (!scriptContext)
    {
        xmlXPathSetError((ctxt), XPATH_INVALID_CTXT);
        return;
    }
    Owned<IDataMaskingProfileContext> masker(scriptContext->getMasker());

    if (nargs < 1 || nargs > 2)
    {
        xmlXPathSetArityError(ctxt);
        return;
    }

    StringBuffer content, contentType;
    xmlChar* tmp;
    if (2 <= nargs)
    {
        tmp = xmlXPathPopString(ctxt);
        if (xmlXPathCheckError(ctxt))
            return;
        contentType.append((const char*)tmp);
        xmlFree(tmp);
    }

    tmp = xmlXPathPopString(ctxt);
    if (xmlXPathCheckError(ctxt))
        return;
    content.append((const char*)tmp);
    xmlFree(tmp);

    if (masker)
        masker->maskContent(contentType, const_cast<char*>(content.str()), 0, content.length());

    xmlXPathReturnString(ctxt, xmlStrdup((const xmlChar*)content.str()));
}

void registerEsdlXPathExtensionsForURI(IXpathContext *xpathContext, const char *uri)
{
    xpathContext->registerFunction(uri, "validateFeaturesAccess", (void *)validateFeaturesAccessFunction);
    xpathContext->registerFunction(uri, "secureAccessFlags", (void *)secureAccessFlagsFunction);
    xpathContext->registerFunction(uri, "getFeatureSecAccessFlags", (void *)getFeatureSecAccessFlagsFunction);
    xpathContext->registerFunction(uri, "getStoredStringValue", (void *)getStoredStringValueFunction);
    xpathContext->registerFunction(uri, "getDataSection", (void *)scriptGetDataSectionFunction);
    xpathContext->registerFunction(uri, "ensureDataSection", (void *)scriptEnsureDataSectionFunction);
    xpathContext->registerFunction(uri, "storedValueExists", (void *)storedValueExistsFunction);
    xpathContext->registerFunction(uri, "getLogProfile", (void *)getLogProfileFunction);
    xpathContext->registerFunction(uri, "getLogOption", (void *)getLogOptionFunction);
    xpathContext->registerFunction(uri, "logOptionExists", (void *)logOptionExistsFunction);
    xpathContext->registerFunction(uri, "tokenize", (void *)strTokenizeFunction);
    xpathContext->registerFunction(uri, "deprecatedEncryptString", (void *)deprecatedEncryptString);
    xpathContext->registerFunction(uri, "deprecatedDecryptString", (void *)deprecatedDecryptString);
    xpathContext->registerFunction(uri, "encodeBase64String", (void *)encodeBase64String);
    xpathContext->registerFunction(uri, "decodeBase64String", (void *)decodeBase64String);
    xpathContext->registerFunction(uri, "escapeXmlCharacters", (void *)escapeXmlCharacters);
    xpathContext->registerFunction(uri, "unescapeXmlCharacters", (void *)unescapeXmlCharacters);
    xpathContext->registerFunction(uri, "compressString", (void *)compressString);
    xpathContext->registerFunction(uri, "decompressString", (void *)decompressString);
    xpathContext->registerFunction(uri, "toXmlString", (void *)toXmlString);
    xpathContext->registerFunction(uri, "getTxSummary", (void *)getTxSummary);
    xpathContext->registerFunction(uri, "maskValue", (void *)maskValue);
    xpathContext->registerFunction(uri, "maskContent", (void *)maskContent);
}

void registerEsdlXPathExtensions(IXpathContext *xpathContext, IEsdlScriptContext *context, const StringArray &prefixes)
{
    bool includeDefaultNS = false;
    xpathContext->setUserData(context);
    if (!prefixes.ordinality())
        xpathContext->registerNamespace("esdl", "urn:hpcc:esdl:script");
    else
    {
        ForEachItemIn(i, prefixes)
        {
            if (isEmptyString(prefixes.item(i)))
                includeDefaultNS=true;
            else
                xpathContext->registerNamespace(prefixes.item(i), "urn:hpcc:esdl:script");
        }
    }

    if (includeDefaultNS)
        registerEsdlXPathExtensionsForURI(xpathContext, nullptr);
    registerEsdlXPathExtensionsForURI(xpathContext, "urn:hpcc:esdl:script");
}
