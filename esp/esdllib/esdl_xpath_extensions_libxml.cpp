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

inline IEsdlScriptContext *getEsdlScriptContext(xmlXPathParserContextPtr ctxt)
{
    if (!ctxt || !ctxt->context || !ctxt->context->userData)
        return nullptr;

    return reinterpret_cast<IEsdlScriptContext *>(ctxt->context->userData);
}

inline IEspContext *getEspContext(xmlXPathParserContextPtr ctxt)
{
    IEsdlScriptContext *scriptContext = getEsdlScriptContext(ctxt);
    if (!scriptContext || !scriptContext->queryEspContext())
        return nullptr;
    return reinterpret_cast<IEspContext *>(scriptContext->queryEspContext());
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
    IEspContext *espContext = getEspContext(ctxt);
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
    IEspContext *espContext = getEspContext(ctxt);
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
    IEspContext *espContext = getEspContext(ctxt);
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
    IEsdlScriptContext *scriptContext = getEsdlScriptContext(ctxt);
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
 * getLogOptionFunction
 * @ctxt:  an XPath parser context
 * @nargs:  the number of arguments
 *
 */
static void getLogOptionFunction (xmlXPathParserContextPtr ctxt, int nargs)
{
    IEsdlScriptContext *scriptContext = getEsdlScriptContext(ctxt);
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
    IEsdlScriptContext *scriptContext = getEsdlScriptContext(ctxt);
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
    IEsdlScriptContext *scriptContext = getEsdlScriptContext(ctxt);
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
    IEsdlScriptContext *scriptContext = getEsdlScriptContext(ctxt);
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
    IEsdlScriptContext *scriptContext = getEsdlScriptContext(ctxt);
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

void registerEsdlXPathExtensionsForURI(IXpathContext *xpathContext, const char *uri)
{
    xpathContext->registerFunction(uri, "validateFeaturesAccess", (void *)validateFeaturesAccessFunction);
    xpathContext->registerFunction(uri, "secureAccessFlags", (void *)secureAccessFlagsFunction);
    xpathContext->registerFunction(uri, "getFeatureSecAccessFlags", (void *)getFeatureSecAccessFlagsFunction);
    xpathContext->registerFunction(uri, "getStoredStringValue", (void *)getStoredStringValueFunction);
    xpathContext->registerFunction(uri, "storedValueExists", (void *)storedValueExistsFunction);
    xpathContext->registerFunction(uri, "getLogProfile", (void *)getLogProfileFunction);
    xpathContext->registerFunction(uri, "getLogOption", (void *)getLogOptionFunction);
    xpathContext->registerFunction(uri, "logOptionExists", (void *)logOptionExistsFunction);
    xpathContext->registerFunction(uri, "tokenize", (void *)strTokenizeFunction);
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
