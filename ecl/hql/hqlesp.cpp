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
#include "jliball.hpp"
#include "hql.hpp"
#include "eclrtl.hpp"
#include "workunit.hpp"
#include "junicode.hpp"

#include "hqlerrors.hpp"
#include "hqlexpr.hpp"
#include "hqlesp.hpp"
#include "hqlutil.hpp"
#include "hqlrepository.hpp"

//-------------------------------------------------------------------------------------------------------------------
// Process SOAP header information from an attribute.

class StringPairItem : public CInterface
{
public:
    StringPairItem(const char * _first, const char * _second) : first(_first), second(_second) {}

    StringAttr first;
    StringAttr second;
};

typedef CIArrayOf<StringPairItem> StringPairArray;


static bool extractAttributeReference(StringBuffer & attributeName, const char * text)
{
    const char * dot = NULL;
    const char * cur = text;
    loop
    {
        char next = *cur;
        if (next == '.' || next == '/')
        {
            dot = cur;
            next = '.';
        }
        else if (isalnum((byte)next) || (next=='_'))
        {
        }
        else
            break;
        attributeName.append(next);
        cur++;
    }

    if (dot && (dot != text) && (cur != dot+1))
        return true;
    return false;
}

static bool allowMultipleInstances(const char * tag)
{
    return (strcmp(tag, "USES") == 0);
}

//-------------------------------------------------------------------------------------------------------------------

static void parseAttribute(IPropertyTree *destTree, const char *text, const char * name)
{
    if (!text)
        return;

    while (*text)
    {
        if (text[0]=='/' && text[1]=='*' && text[2]=='-' && text[3]=='-')
        {
            unsigned formLen = 0;
            unsigned markerLen = 0;

            text += 4;
            const char *marker = text;
            while (isalpha(*text))
                text++;
            if (text[0]=='-' && text[1]=='-')
            {
                markerLen = text-marker;
                text += 2;
            }

            while (isspace(*text)) text++;
            const char * form = text;
            loop
            {
                if (text[0]=='*' && text[1]=='/')
                {
                    formLen = text-form;
                    text += 2;
                    break;
                }
                if (!*text)
                    break;

                text++;
            }

            if (formLen && markerLen)
            {
                StringBuffer m(markerLen, marker);
                StringBuffer f(formLen, form);

                const char *ptr = f.str();
                const char *start_xmltag;
                // need to strip off ALL of the <?xml tags
                while ( (start_xmltag = strstr(ptr, "<?xml")) != 0)
                {
                    const char *end_xmltag = strchr(start_xmltag, '>');
                    if (!end_xmltag)
                        break;

                    unsigned startpos = (unsigned)(start_xmltag - ptr);
                    int endpos = (end_xmltag + 1) - start_xmltag;

                    f.remove(startpos, endpos);
                    f.trimLeft();
                    ptr = f.str();
                }

                if (allowMultipleInstances(m))
                    destTree->addProp(m.str(), f.str());
                else if (!destTree->hasProp(m.str()))
                    destTree->setProp(m.str(), f.str());
                else
                    throw MakeStringException(3, "Tag '%s' is defined more than once in attribute %s", m.str(), name);
            }
        }
        else
            text++;
    }
}

//-------------------------------------------------------------------------------------------------------------------

class WebServicesExtractor
{
public:
    WebServicesExtractor(HqlLookupContext & _lookupCtx) : lookupCtx(_lookupCtx) 
    {
        root.setown(createPTree("Cache"));
    }

    void addRootReference(const char * attribute);
    void addRootText(const char * text);
    void getResults(StringPairArray & results);
    unsigned getVersion();
    bool extractWebServiceInfo();

protected:
    IPropertyTree * ensureCacheEntry(const char * name);
    void getAttributeText(StringBuffer & text, const char * attributeName);
    void extractWebServiceInfo(const char * section);
    bool extractWebServiceInfo(IPropertyTree * element, const char * section);
    bool extractWebServiceInfo(const char * attribute, const char * section);
    IPropertyTree * resolveAttribute(const char * name);

protected:
    HqlLookupContext & lookupCtx;
    Owned<IPropertyTree> root;
    Linked<IPropertyTree> archive;
    CopyArray visited;
    StringPairArray results;
};

void WebServicesExtractor::getAttributeText(StringBuffer & text, const char* attributeName)
{
    const char * dot = strrchr(attributeName, '.');
    if(!dot || !dot[1])
        throw MakeStringException(3, "Please specify both module and attribute");

    OwnedHqlExpr symbol = getResolveAttributeFullPath(attributeName, LSFpublic, lookupCtx); 
    if (!symbol || !hasNamedSymbol(symbol) || !symbol->hasText()) 
    {
        StringBuffer txt;
        txt.append("Could not read attribute: ").append(attributeName);
        DBGLOG("%s", txt.toCharArray());
        throw MakeStringException(ERR_NO_ATTRIBUTE_TEXT, "%s", txt.str());
    }

    symbol->getTextBuf(text);

    /* MORE: It would be preferable if this was better integrated with hqlgram2.cpp.  It's a reasonable stopgap */
    if (archive)
    {
        StringAttr moduleName(attributeName, dot-attributeName);
        IPropertyTree * moduleTree = queryEnsureArchiveModule(archive, moduleName, NULL);
        IPropertyTree * attrTree = queryArchiveAttribute(moduleTree, dot+1);
        if (!attrTree)
        {
            attrTree = createArchiveAttribute(moduleTree, dot+1);

            const char * p = text.str();
            if (0 == strncmp(p, UTF8_BOM,3))
                p += 3;
            attrTree->setProp("", p);
        }
    }
}


void WebServicesExtractor::addRootReference(const char * attribute)
{
    root->addProp("USES", attribute);
}

void WebServicesExtractor::addRootText(const char * text)
{
    parseAttribute(root, text, "");
}

IPropertyTree * WebServicesExtractor::ensureCacheEntry(const char * name)
{
    IPropertyTree * match = resolveAttribute(name);
    if (match)
        return match;

    Owned<IPropertyTree> tree = createPTree("attr");
    tree->setProp("@name", name);
    
    StringBuffer lowerName;
    tree->setProp("@key", lowerName.append(name).toLowerCase());

    StringBuffer text;
    getAttributeText(text, name);
    parseAttribute(tree, text, name);
    return root->addPropTree("attr", tree.getClear());
}

IPropertyTree * WebServicesExtractor::resolveAttribute(const char * name)
{
    StringBuffer lowerName, xpath;
    lowerName.append(name).toLowerCase();

    xpath.append("attr[@key='").append(lowerName).append("']");
    return root->queryPropTree(xpath);
}


bool WebServicesExtractor::extractWebServiceInfo(IPropertyTree * attributeTree, const char * section)
{
    if (visited.contains(*attributeTree))
        return false;
    visited.append(*attributeTree);

    const char * result = attributeTree->queryProp(section);
    if (result)
    {
        if (strcmp(section, "INFO")==0 || strcmp(section, "HELP")==0 || strcmp(section, "DFORM")==0 || *result=='<')
        {
            results.append(*new StringPairItem(section, result));
            return true;
        }

        StringBuffer nextAttribute;
        if (extractAttributeReference(nextAttribute, result))
        {
            if (extractWebServiceInfo(nextAttribute, section))
                return true;
        }
    }

    Owned<IPropertyTreeIterator> iter = attributeTree->getElements("USES");
    ForEach(*iter)
    {
        StringBuffer nextAttribute;
        if (extractAttributeReference(nextAttribute, iter->query().queryProp("")))
            if (extractWebServiceInfo(nextAttribute, section))
                return true;
    }
    return false;
}

bool WebServicesExtractor::extractWebServiceInfo(const char * attribute, const char * section)
{
    if (!attribute || !*attribute)
        return false;
    return extractWebServiceInfo(ensureCacheEntry(attribute), section);
}


void WebServicesExtractor::extractWebServiceInfo(const char * section)
{
    visited.kill();
    extractWebServiceInfo(root, section);
}


bool WebServicesExtractor::extractWebServiceInfo()
{
    // start load forms
    static const char *sectionNames[] = { "HTML", "HTMLD", "SOAP", "INFO", "HELP", "ERROR", "RESULT", "OTX", NULL };
    for (unsigned i = 0; sectionNames[i]; i++)
    {
        extractWebServiceInfo(sectionNames[i]);
    }
    return (results.ordinality() != 0);
}

void WebServicesExtractor::getResults(StringPairArray & targetResults)
{
    ForEachItemIn(i, results)
        targetResults.append(OLINK(results.item(i)));
}

unsigned WebServicesExtractor::getVersion()
{
    StringBuffer info;
    toXML(root, info);
    return crc32(info.str(), info.length(), 0);
}

bool retrieveWebServicesInfo(IWorkUnit *workunit, HqlLookupContext & ctx)
{
    Owned<IWUWebServicesInfo> webServicesInfo = workunit->updateWebServicesInfo(false);  // if it doesn't exist - don't do anything
    if (!webServicesInfo)
        return false;

    SCMStringBuffer moduleName, attributeName, defaultName;
    webServicesInfo->getModuleName(moduleName);
    webServicesInfo->getAttributeName(attributeName);
    webServicesInfo->getDefaultName(defaultName);

    StringBuffer fullName;
    fullName.append(moduleName).append(".").append(attributeName);

    WebServicesExtractor extractor(ctx);
    extractor.addRootReference(fullName);
    if (defaultName.length())
    {
        const char *lastsep = strrchr(defaultName.str(), '/');
        if (lastsep)
        {
            StringBuffer mappedDefault;
            mappedDefault.append(defaultName.str()).replace('/', '.');
            extractor.addRootReference(mappedDefault);
        }
    }

    extractor.extractWebServiceInfo();

    StringPairArray results;
    extractor.getResults(results);
    ForEachItemIn(i, results)
    {
        StringPairItem & cur = results.item(i);
        webServicesInfo->setInfo(cur.first, cur.second); 
    }

    //Names are currently stored case insensitively, we want the case sensitive variant.
    OwnedHqlExpr s1 = ctx.queryRepository()->queryRootScope()->lookupSymbol(createIdentifierAtom(moduleName.str()), LSFpublic, ctx);
    if (s1 && s1->queryScope())
    {
        const char *caseSensitiveModuleName = s1->queryScope()->queryFullName();
        webServicesInfo->setModuleName(caseSensitiveModuleName);
    }
    
    unsigned version = extractor.getVersion();
    webServicesInfo->setWebServicesCRC(version);

//  const char *caseSensitiveAttrName = attrCache->queryProp("Module/Attribute/@name");
//  if (caseSensitiveAttrName)
//      webServicesInfo->setAttributeName(caseSensitiveAttrName);

    return true;
}


bool retrieveWebServicesInfo(IWorkUnit *workunit, const char * queryText, HqlLookupContext & ctx)
{
    WebServicesExtractor extractor(ctx);
    extractor.addRootText(queryText);

    StringPairArray results;
    if (!extractor.extractWebServiceInfo())
        return false;

    Owned<IWUWebServicesInfo> webServicesInfo = workunit->updateWebServicesInfo(true);
    extractor.getResults(results);
    ForEachItemIn(i, results)
    {
        StringPairItem & cur = results.item(i);
        webServicesInfo->setInfo(cur.first, cur.second); 
    }

    unsigned version = extractor.getVersion();
    webServicesInfo->setWebServicesCRC(version);
    return true;
}

IPropertyTree * retrieveWebServicesInfo(WebServicesExtractor &extractor, HqlLookupContext & ctx)
{
    if (!extractor.extractWebServiceInfo())
        return NULL;

    StringPairArray results;
    extractor.getResults(results);

    Owned<IPropertyTree> result = createPTree("WebServicesInfo");
    ForEachItemIn(i, results)
    {
        StringPairItem & cur = results.item(i);
        result->setProp(cur.first, cur.second); 
    }

    result->setPropInt("@crc", extractor.getVersion());
    return result.getClear();
}

IPropertyTree * retrieveWebServicesInfo(const char * queryText, HqlLookupContext & ctx)
{
    WebServicesExtractor extractor(ctx);
    extractor.addRootText(queryText);
    return retrieveWebServicesInfo(extractor, ctx);
}

IPropertyTree * retrieveMainWebServicesInfo(const char * mainDefinition, HqlLookupContext & ctx)
{
    WebServicesExtractor extractor(ctx);
    extractor.addRootReference(mainDefinition);
    return retrieveWebServicesInfo(extractor, ctx);
}
