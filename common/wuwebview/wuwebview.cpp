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

#include "jlib.hpp"
#include "jexcept.hpp"
#include "jptree.hpp"
#include "junicode.hpp"
#include "workunit.hpp"
#include "dllserver.hpp"
#include "thorplugin.hpp"
#include "xslprocessor.hpp"
#include "fileview.hpp"

#include "wuwebview.hpp"
#include "wuweberror.hpp"

typedef MapStringTo<bool> BoolHash;

class WuExpandedResultBuffer : implements IPTreeNotifyEvent, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    WuExpandedResultBuffer(const char *queryname, unsigned _flags=0) :
        name(queryname), resultlevel(0), finalized(false), flags(_flags), hasXmlns(false)
    {
        if (flags & (WWV_INCL_NAMESPACES | WWV_INCL_GENERATED_NAMESPACES))
        {
            StringBuffer lower(name);
            ns.append("urn:hpccsystems:ecl:").append(lower.toLowerCase());
        }

        if (!(flags & WWV_OMIT_XML_DECLARATION))
            buffer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        if (flags & WWV_USE_DISPLAY_XSLT)
            buffer.append("<?xml-stylesheet type=\"text/xsl\" href=\"/esp/xslt/xmlformatter.xsl\"?>");
        if (flags & WWV_ADD_SOAP)
            buffer.append(
                "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\""
                  " xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\">"
                    " <soap:Body>"
            );
        if (flags & WWV_ADD_RESPONSE_TAG)
        {
            buffer.append('<');
            if (queryname)
                buffer.append(queryname);
            buffer.append("Response");
            if ((flags & WWV_INCL_NAMESPACES) && ns.length())
                buffer.append(" xmlns=\"").append(ns.str()).append('\"');
            buffer.append('>');
        }
        if (flags & WWV_ADD_RESULTS_TAG)
            buffer.append("<Results>");
        if (!(flags & WWV_OMIT_RESULT_TAG))
            buffer.append("<Result>");

        initResultChildTags();
    }

    void initResultChildTags()
    {
        resultChildTags.setValue("Dataset", true);
        resultChildTags.setValue("Tracing", true);
        resultChildTags.setValue("Exception", true);
        resultChildTags.setValue("Warning", true);
        resultChildTags.setValue("Alert", true);
        resultChildTags.setValue("Info", true);
    }

    void appendResults(IConstWorkUnit *wu, const char *username, const char *pw)
    {
        StringBufferAdaptor resultXML(buffer);
        getFullWorkUnitResultsXML(username, pw, wu, resultXML, WorkUnitXML_NoRoot, SeverityError);
    }

    void appendSingleResult(IConstWorkUnit *wu, const char *resultname, const char *username, const char *pw)
    {
        StringBufferAdaptor resultXML(buffer);
        Owned<IResultSetFactory> factory = getResultSetFactory(username, pw);
        Owned<INewResultSet> nr = factory->createNewResultSet(wu->queryWuid(), 0, resultname);
        getResultXml(resultXML, nr.get(), resultname, 0, 0, NULL);
    }

    void appendDatasetsFromXML(const char *xml)
    {
        assertex(!finalized);
        Owned<IPullPTreeReader> reader = createPullXMLStringReader(xml, *this);
        reader->load();
    }

    void append(const char *value)
    {
        assertex(!finalized);
        buffer.append(value);
    }

    void appendSchemaResource(IPropertyTree &res, ILoadedDllEntry *dll)
    {
        if (!dll || (flags & WWV_OMIT_SCHEMAS))
            return;
        if (res.getPropInt("@seq", -1)>=0 && res.hasProp("@id"))
        {
            int id = res.getPropInt("@id");
            size32_t len = 0;
            const void *data = NULL;
            if (dll->getResource(len, data, "RESULT_XSD", (unsigned) id) && len>0)
            {
                buffer.append("<XmlSchema name=\"").append(res.queryProp("@name")).append("\">");
                if (res.getPropBool("@compressed"))
                {
                    StringBuffer decompressed;
                    decompressResource(len, data, decompressed);
                    if (flags & WWV_CDATA_SCHEMAS)
                        buffer.append("<![CDATA[");
                    buffer.append(decompressed.str());
                    if (flags & WWV_CDATA_SCHEMAS)
                        buffer.append("]]>");
                }
                else
                    buffer.append(len, (const char *)data);
                buffer.append("</XmlSchema>");
            }
        }
    }

    void appendManifestSchemas(IPropertyTree &manifest, ILoadedDllEntry *dll)
    {
        if (flags & WWV_OMIT_SCHEMAS)
            return;
        assertex(!finalized);
        if (!dll)
            return;
        BoolHash uniqueResultNames;
        Owned<IPropertyTreeIterator> iter = manifest.getElements("Resource[@type='RESULT_XSD']");
        ForEach(*iter)
        {
            IPropertyTree& res = iter->query();
            const char* name = res.queryProp("@name");
            if (name && *name)
            {
                bool* found = uniqueResultNames.getValue(name);
                if (found && *found)
                    continue;
                uniqueResultNames.setValue(name, true);
            }
            appendSchemaResource(res, dll);
        }
    }

    void appendManifestResultSchema(IPropertyTree &manifest, const char *resultname, ILoadedDllEntry *dll)
    {
        assertex(!finalized);
        if (!dll)
            return;
        VStringBuffer xpath("Resource[@name='%s'][@type='RESULT_XSD']", resultname);
        IPropertyTree *res=manifest.queryPropTree(xpath.str());
        if (res)
            appendSchemaResource(*res, dll);
    }

    void appendXML(IPropertyTree *xml, const char *tag=NULL)
    {
        assertex(!finalized);
        if (tag)
            buffer.append('<').append(tag).append('>');
        if (xml)
            toXML(xml, buffer);
        if (tag)
            buffer.append("</").append(tag).append('>');
    }

    virtual void beginNode(const char *tag, bool arrayitem, offset_t startOffset)
    {
        bool *pIsResultTag = resultChildTags.getValue(tag);
        if (pIsResultTag && *pIsResultTag)
            resultlevel++;
        if (resultlevel)
            buffer.append('<').append(tag);
    }

    virtual void newAttribute(const char *name, const char *value)
    {
        if (resultlevel)
        {
            if (streq(name, "@xmlns"))
            {
                if (!(flags & WWV_INCL_NAMESPACES))
                    return;
                if (resultlevel==1)
                    hasXmlns=true;
            }
            if (resultlevel==1 && streq(name, "@name"))
                dsname.set(value).toLowerCase().replace(' ', '_');;
            buffer.append(' ').append(name+1).append("=\"");
            encodeUtf8XML(value, buffer);
            buffer.append('\"');
        }
    }
    virtual void beginNodeContent(const char *tag)
    {
        if (resultlevel==1 && streq("Dataset", tag))
        {
            if (!hasXmlns && dsname.length() && (flags & WWV_INCL_GENERATED_NAMESPACES))
            {
                StringBuffer s(ns);
                s.append(":result:").append(dsname.str());
                buffer.append(" xmlns=\"").append(s).append('\"');
            }
            dsname.clear();
            hasXmlns=false;
        }
        if (resultlevel)
            buffer.append('>');
    }
    virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
    {
        if (resultlevel)
        {
            if (length)
            {
                if (binary)
                    JBASE64_Encode(value, length, buffer, true);
                else
                    encodeUtf8XML((const char *)value, buffer);
            }
            buffer.append("</").append(tag).append('>');
            bool *pIsResultTag = resultChildTags.getValue(tag);
            if (pIsResultTag && *pIsResultTag)
                resultlevel--;
        }
    }
    StringBuffer &finalize()
    {
        if (!finalized)
        {
            if (!(flags & WWV_OMIT_RESULT_TAG))
                buffer.append("</Result>");
            if (flags & WWV_ADD_RESULTS_TAG)
                buffer.append("</Results>");
            if (flags & WWV_ADD_RESPONSE_TAG)
                buffer.appendf("</%sResponse>", name.str());
            if (flags & WWV_ADD_SOAP)
                buffer.append("</soap:Body></soap:Envelope>");
            finalized=true;
        }
        return buffer;
    }

    const char *str()
    {
        finalize();
        return buffer.str();
    }

    size32_t length()
    {
        finalize();
        return buffer.length();
    }
public:
    StringBuffer buffer;
    StringAttr name;
    StringBuffer dsname;
    StringBuffer ns;
    bool hasXmlns;
    bool finalized;
    unsigned flags;
    MapStringTo<bool> resultChildTags;
private:
    int resultlevel;
};

class WuWebView : public CInterface,
    implements IWuWebView,
    implements IIncludeHandler
{
public:
    IMPLEMENT_IINTERFACE

    WuWebView(IConstWorkUnit &wu, const char *_target, const char *queryname, const char *wdir, bool mapEspDir, bool delay, IPropertyTree *xsltcfg) :
        manifestIncludePathsSet(false), dir(wdir), mapEspDirectories(mapEspDir), delayedDll(delay), target(_target), xsltConfig(xsltcfg)
    {
        name.set(queryname);
        setWorkunit(wu);
    }

    WuWebView(const char *wuid, const char *_target, const char *queryname, const char *wdir, bool mapEspDir, bool delay, IPropertyTree *xsltcfg) :
        manifestIncludePathsSet(false), dir(wdir), mapEspDirectories(mapEspDir), delayedDll(delay), target(_target), xsltConfig(xsltcfg)
    {
        name.set(queryname);
        setWorkunit(wuid);
    }

    void setWorkunit(IConstWorkUnit &wu);
    void setWorkunit(const char *wuid);
    ILoadedDllEntry *loadDll(bool force=false);

    IPropertyTree *ensureManifest();

    virtual void getResultViewNames(StringArray &names);
    virtual void getResourceURLs(StringArray &urls, const char *prefix);
    virtual unsigned getResourceURLCount();
    virtual void renderResults(const char *viewName, const char *xml, StringBuffer &html);
    virtual void renderResults(const char *viewName, StringBuffer &html);
    virtual void renderSingleResult(const char *viewName, const char *resultname, StringBuffer &html);
    virtual void renderResultsJSON(StringBuffer &out, const char *jsonp);

    virtual void expandResults(const char *xml, StringBuffer &out, unsigned flags);
    virtual void expandResults(StringBuffer &out, unsigned flags);
    virtual void createWuidResponse(StringBuffer &out, unsigned flags);

    virtual void applyResultsXSLT(const char *filename, const char *xml, StringBuffer &out);
    virtual void applyResultsXSLT(const char *filename, StringBuffer &out);
    virtual StringBuffer &aggregateResources(const char *type, StringBuffer &content);

    void renderExpandedResults(const char *viewName, WuExpandedResultBuffer &expanded, StringBuffer &out);

    void appendResultSchemas(WuExpandedResultBuffer &buffer);
    void getResultXSLT(const char *viewName, StringBuffer &xslt, StringBuffer &abspath);
    bool getResource(IPropertyTree *res, StringBuffer &content);
    bool getResource(IPropertyTree *res, MemoryBuffer &content);
    bool getResource(IPropertyTree *res, size32_t & len, const void * & data);
    void getResource(const char *name, StringBuffer &content, StringBuffer &abspath, const char *type);
    bool getResourceByPath(const char *path, MemoryBuffer &mb);
    StringBuffer &getManifest(StringBuffer &mf){return toXML(ensureManifest(), mf);}
    bool getEmbeddedArchive(StringBuffer &ret);

    void calculateResourceIncludePaths();
    virtual bool getInclude(const char *includename, MemoryBuffer &includebuf, bool &pathOnly);
    bool getEspInclude(const char *includename, MemoryBuffer &includebuf, bool &pathOnly);


    void addVariableFromPTree(IWorkUnit *w, IConstWUResult &vardef, IResultSetMetaData &metadef, const char *varname, IPropertyTree *valtree);
    void addInputsFromPTree(IPropertyTree *pt);
    void addInputsFromXml(const char *xml);


protected:
    SCMStringBuffer dllname;
    StringBuffer name;
    StringBuffer manifestDir;
    Owned<IConstWorkUnit> cw;
    Owned<ILoadedDllEntry> dll;
    Owned<IPropertyTree> manifest;
    Linked<IPropertyTree> xsltConfig;
    StringAttr target;
    StringAttr dir;
    StringAttr username;
    StringAttr pw;
    bool mapEspDirectories;
    bool manifestIncludePathsSet;
    bool delayedDll;
};

IPropertyTree *WuWebView::ensureManifest()
{
    if (!manifest)
    {
        StringBuffer xml;
        // Is this threadsafe? Does it need to be? Should it use getEmbeddedManifestPTree ?
        // It looks like WuWebView classes are created when needed, so not shared between threads.
        manifest.setown((loadDll() && getEmbeddedManifestXML(dll, xml)) ? createPTreeFromXMLString(xml.str()) : createPTree());
    }
    return manifest.get();
}

StringBuffer &makeResourcePath(const char *path, const char *basedir, StringBuffer &respath)
{
    StringBuffer abspath;
    makeAbsolutePath(path, basedir, abspath);

    return makePathUniversal(abspath, respath);
}

void WuWebView::calculateResourceIncludePaths()
{
    if (!manifestIncludePathsSet)
    {
        manifestDir.set(ensureManifest()->queryProp("@manifestDir"));
        Owned<IPropertyTreeIterator> iter = manifest->getElements("Resource[@filename]");
        ForEach(*iter)
        {
            if (!iter->query().hasProp("@resourcePath")) //backward compatible
            {
                StringBuffer respath;
                makeResourcePath(iter->query().queryProp("@filename"), dir.get(), respath);
                iter->query().setProp("@resourcePath", respath.str());
            }
        }
        manifestIncludePathsSet=true;
    }
}

bool WuWebView::getEspInclude(const char *includename, MemoryBuffer &includebuf, bool &pathOnly)
{
    StringBuffer absPath;
    makeAbsolutePath(includename, dir.get(), absPath);
    if (checkFileExists(absPath.str()))
    {
        Owned <IFile> f = createIFile(absPath.str());
        Owned <IFileIO> fio = f->open(IFOread);
        read(fio, 0, (size32_t) f->size(), includebuf);
    }
    //esp looks in two places for path starting with "xslt/"
    else if (mapEspDirectories && !strncmp(includename, "xslt/", 5))
    {
        absPath.clear().append(dir.get());
        absPath.append("smc_").append(includename);;
        makeAbsolutePath(absPath);
        if (checkFileExists(absPath.str()))
        {
            Owned <IFile> f = createIFile(absPath.str());
            Owned <IFileIO> fio = f->open(IFOread);
            read(fio, 0, (size32_t) f->size(), includebuf);
        }
    }
    return true;
}

bool WuWebView::getInclude(const char *includename, MemoryBuffer &includebuf, bool &pathOnly)
{
    //eliminate "file://"
    if (strncmp(includename, "file:", 5)==0)
        includename+=5;
    if (*includename=='/')
    {
        while (includename[1]=='/')
            includename++;
        //eliminate extra '/' for windows absolute paths
        if (includename[1] && includename[2]==':')
            includename++;
    }
    if (mapEspDirectories && !strnicmp(includename, "/esp/", 5))
        return getEspInclude(includename+5, includebuf, pathOnly);

    IPropertyTree *res = NULL;
    if (manifest)
    {
        if (strieq(includename, "/EmbeddedView"))
            res = manifest->queryPropTree("Resource[@name='Results'][@type='XSLT']");
        else
        {
            VStringBuffer xpath("Resource[@resourcePath='%s']", includename);
            res = manifest->queryPropTree(xpath.str());
        }
    }
    if (res)
    {
        StringBuffer xslt;
        getResource(res, xslt);
        includebuf.append(xslt.str());
    }
    else if (checkFileExists(includename))
    {
        Owned <IFile> f = createIFile(includename);
        Owned <IFileIO> fio = f->open(IFOread);
        read(fio, 0, (size32_t) f->size(), includebuf);
    }
    return true;
}

bool WuWebView::getResourceByPath(const char *path, MemoryBuffer &mb)
{
    calculateResourceIncludePaths();

    StringBuffer xpath;
    if (!manifestDir.length())
        xpath.setf("Resource[@filename='%s'][1]", path);
    else
    {
        StringBuffer respath;
        makeResourcePath(path, manifestDir.str(), respath);
        xpath.setf("Resource[@resourcePath='%s'][1]", respath.str());
    }

    IPropertyTree *res = ensureManifest()->queryPropTree(xpath.str());
    if (!res)
        return false;
    return getResource(res, mb);
}

unsigned WuWebView::getResourceURLCount()
{
    unsigned urlCount = 1;
    Owned<IPropertyTreeIterator> iter = ensureManifest()->getElements("Resource");
    ForEach(*iter)
    {
        IPropertyTree &res = iter->query();
        if (res.hasProp("@ResourcePath") || res.hasProp("@filename"))
            urlCount++;
    }
    return urlCount;
}

void WuWebView::getResourceURLs(StringArray &urls, const char *prefix)
{
    const char *wuid = cw->queryWuid();
    StringBuffer url(prefix);
    url.append("manifest/");
    if (target.length() && name.length())
        url.appendf("query/%s/%s", target.get(), name.str());
    else
        url.append(wuid);
    urls.append(url);

    Owned<IPropertyTreeIterator> iter = ensureManifest()->getElements("Resource");
    ForEach(*iter)
    {
        IPropertyTree &res = iter->query();
        url.set(prefix).append("res/");
        if (target.length() && name.length())
            url.appendf("query/%s/%s", target.get(), name.str());
        else
            url.append(wuid);
        if (res.hasProp("@ResourcePath"))
            urls.append(url.append(res.queryProp("@ResourcePath")));
        else if (res.hasProp("@filename"))
            urls.append(url.append('/').append(res.queryProp("@filename")));
    }
}

void WuWebView::getResultViewNames(StringArray &names)
{
    Owned<IPropertyTreeIterator> iter = ensureManifest()->getElements("Views/Results[@name]");
    ForEach(*iter)
        names.append(iter->query().queryProp("@name"));
    if (manifest->hasProp("Views/XSLT/RESULTS[@resource='Results']"))
        names.append("EmbeddedView");
}

bool WuWebView::getResource(IPropertyTree *res, size32_t & len, const void * & data)
{
    if (!loadDll())
        return false;
    if (res->hasProp("@id") && (res->hasProp("@header")||res->hasProp("@compressed")))
    {
        int id = res->getPropInt("@id");
        return (dll->getResource(len, data, res->queryProp("@type"), (unsigned) id) && len>0);
    }
    return false;
}

bool WuWebView::getResource(IPropertyTree *res, MemoryBuffer &content)
{
    size32_t len = 0;
    const void *data = NULL;
    if (getResource(res, len, data))
    {
        if (res->getPropBool("@compressed"))
            decompressResource(len, data, content);
        else
            content.append(len, (const char *)data);
        return true;
    }
    return false;
}

bool WuWebView::getResource(IPropertyTree *res, StringBuffer &content)
{
    size32_t len = 0;
    const void *data = NULL;
    if (getResource(res, len, data))
    {
        if (res->getPropBool("@compressed"))
            decompressResource(len, data, content);
        else
            content.append(len, (const char *)data);
        return true;
    }
    return false;
}

void WuWebView::getResource(const char *name, StringBuffer &content, StringBuffer &includepath, const char *type)
{
    VStringBuffer xpath("Resource[@name='%s']", name);
    if (type)
        xpath.append("[@type='").append(type).append("']");
    IPropertyTree *res = ensureManifest()->queryPropTree(xpath.str());
    calculateResourceIncludePaths();
    includepath.append(res->queryProp("@resourcePath"));
    if (res)
        getResource(res, content);
}

StringBuffer &WuWebView::aggregateResources(const char *type, StringBuffer &content)
{
    VStringBuffer xpath("Resource[@type='%s']", type);
    Owned<IPropertyTreeIterator> iter = ensureManifest()->getElements(xpath.str());
    ForEach(*iter)
        getResource(&iter->query(), content);
    return content;
}

void WuWebView::getResultXSLT(const char *viewName, StringBuffer &xslt, StringBuffer &abspath)
{
    if (!viewName || !*viewName)
        return;
    if (strieq("EmbeddedView", viewName))
    {
        getResource("Results", xslt, abspath, "XSLT");
        return;
    }
    VStringBuffer xpath("Views/Results[@name='%s']/@resource", viewName);
    const char *resource = ensureManifest()->queryProp(xpath.str());
    if (resource)
        getResource(resource, xslt, abspath, "XSLT");
}

void WuWebView::renderExpandedResults(const char *viewName, WuExpandedResultBuffer &expanded, StringBuffer &out)
{
    IPropertyTree *mf = ensureManifest();
    calculateResourceIncludePaths();

    IPropertyTree *view;
    const char *type = NULL;
    const char *respath = NULL;
    if (strieq("EmbeddedView", viewName))
    {
        view = mf->queryPropTree("Views/XSLT/RESULTS[@resource='Results']");
        if (!view)
            throw MakeStringException(WUWEBERR_ViewResourceNotFound, "EmbeddedView not found");
        type="xslt";
        respath="/EmbeddedView";
    }
    else
    {
        VStringBuffer xpath("Views/Results[@name='%s']", viewName);
        view = mf->queryPropTree(xpath.str());
        if (!view)
            throw MakeStringException(WUWEBERR_ViewResourceNotFound, "Result view %s not found", viewName);
        type=view->queryProp("@type");
        if (!type)
            throw MakeStringException(WUWEBERR_UnknownViewType, "No type defined for view %s", viewName);
        if (strieq(type, "xslt"))
        {
            const char *resname = view->queryProp("@resource");
            if (!resname || !*resname)
                throw MakeStringException(WUWEBERR_ViewResourceNotFound, "resource for %s view not defined", viewName);
            xpath.set("Resource[@name='").append(resname).append("']/@resourcePath");
            respath = mf->queryProp(xpath.str());
            if (!respath || !*respath)
                throw MakeStringException(WUWEBERR_ViewResourceNotFound, "resource %s not resolved", resname);
        }
        else if (!strieq(type, "xml"))
            throw MakeStringException(WUWEBERR_UnknownViewType, "View %s has an unknown type of %s", viewName, type);
    }

    expanded.appendXML(view, "view");
    expanded.appendManifestSchemas(*mf, loadDll());
    expanded.finalize();
    if (strieq(type, "xml"))
        return out.swapWith(expanded.buffer);

    Owned<IXslTransform> t = getXslProcessor()->createXslTransform(xsltConfig);
    StringBuffer cacheId(viewName);
    cacheId.append('@').append(dllname.str()); //using dllname, cloned workunits can share cache entry
    t->setIncludeHandler(this);
    t->loadXslFromEmbedded(respath, cacheId.str());
    t->setXmlSource(expanded.buffer.str(), expanded.buffer.length());
    t->transform(out);
}

void WuWebView::renderResults(const char *viewName, const char *xml, StringBuffer &out)
{
    WuExpandedResultBuffer buffer(name.str(), WWV_ADD_RESPONSE_TAG | WWV_ADD_RESULTS_TAG);
    buffer.appendDatasetsFromXML(xml);
    renderExpandedResults(viewName, buffer, out);
}

void WuWebView::renderResults(const char *viewName, StringBuffer &out)
{
    WuExpandedResultBuffer buffer(name.str(), WWV_ADD_RESPONSE_TAG | WWV_ADD_RESULTS_TAG);
    buffer.appendResults(cw, username.get(), pw.get());
    renderExpandedResults(viewName, buffer, out);
}

void WuWebView::renderResultsJSON(StringBuffer &out, const char *jsonp)
{
    if (jsonp && *jsonp)
        out.append(jsonp).append('(');
    out.append('{');
    StringBuffer responseName(name.str());
    responseName.append("Response");
    appendJSONName(out, responseName);
    StringBufferAdaptor json(out);
    getFullWorkUnitResultsJSON(username, pw, cw, json, 0, SeverityError);
    out.append("}");
    if (jsonp && *jsonp)
        out.append(");");
}


void WuWebView::renderSingleResult(const char *viewName, const char *resultname, StringBuffer &out)
{
    WuExpandedResultBuffer buffer(name.str(), WWV_ADD_RESPONSE_TAG | WWV_ADD_RESULTS_TAG);
    buffer.appendSingleResult(cw, resultname, username.get(), pw.get());
    renderExpandedResults(viewName, buffer, out);
}

void expandWuXmlResults(StringBuffer &out, const char *name, const char *xml, unsigned flags, IPropertyTree *manifest, ILoadedDllEntry *dll)
{
    WuExpandedResultBuffer expander(name, flags);
    expander.appendDatasetsFromXML(xml);
    if (!(flags & WWV_OMIT_SCHEMAS) && manifest && dll)
        expander.appendManifestSchemas(*manifest, dll);
    expander.finalize();
    out.append(expander.buffer);
}

extern WUWEBVIEW_API void expandWuXmlResults(StringBuffer &out, const char *name, const char *xml, unsigned flags)
{
    expandWuXmlResults(out, name, xml, flags, NULL, NULL);
}

void WuWebView::expandResults(const char *xml, StringBuffer &out, unsigned flags)
{
    IPropertyTree *manifest = NULL;
    ILoadedDllEntry *dll = NULL;
    if (!(flags & WWV_OMIT_SCHEMAS))
    {
        manifest = ensureManifest();
        dll = loadDll();
    }
    expandWuXmlResults(out, name.str(), xml, flags, manifest, dll);
}

void WuWebView::createWuidResponse(StringBuffer &out, unsigned flags)
{
    flags &= ~WWV_ADD_RESULTS_TAG;
    flags |= WWV_OMIT_RESULT_TAG;

    WuExpandedResultBuffer expander(name.str(), flags);
    appendXMLTag(expander.buffer, "Wuid", cw->queryWuid());
    expander.finalize();
    out.append(expander.buffer);
}

void WuWebView::expandResults(StringBuffer &out, unsigned flags)
{
    SCMStringBuffer xml;
    getFullWorkUnitResultsXML(username.get(), pw.get(), cw, xml, WorkUnitXML_SeverityTags, SeverityInformation);
    expandResults(xml.str(), out, flags);
}

void WuWebView::applyResultsXSLT(const char *filename, const char *xml, StringBuffer &out)
{
    WuExpandedResultBuffer buffer(name.str(), WWV_ADD_RESPONSE_TAG | WWV_ADD_RESULTS_TAG);
    buffer.appendDatasetsFromXML(xml);
    buffer.appendManifestSchemas(*ensureManifest(), loadDll());

    Owned<IXslTransform> t = getXslProcessor()->createXslTransform(xsltConfig);
    t->setIncludeHandler(this);
    //override default behavior using filename as cache identifier, there's a chance includes are
    //mapped to resources and need to be distinguished in cache
    StringBuffer cacheId(filename);
    cacheId.append('@').append(dllname.str()); //cloned workunits have same dll and resources
    t->loadXslFromFile(filename, cacheId.str());
    t->setXmlSource(buffer.str(), buffer.length());
    t->transform(out);
}

void WuWebView::applyResultsXSLT(const char *filename, StringBuffer &out)
{
    SCMStringBuffer xml;
    getFullWorkUnitResultsXML(username.get(), pw.get(), cw, xml);
    applyResultsXSLT(filename, xml.str(), out);
}

ILoadedDllEntry *WuWebView::loadDll(bool force)
{
    if (!dll && dllname.length() && (force || delayedDll))
    {
        try
        {
            dll.setown(queryDllServer().loadDllResources(dllname.str(), DllLocationAnywhere));
        }
        catch (IException *e)
        {
            VStringBuffer msg("Failed to load %s", dllname.str());
            EXCLOG(e, msg.str());
            e->Release();
        }
        catch(...)
        {
            DBGLOG("Failed to load %s", dllname.str());
        }
        delayedDll=false;
    }
    return dll.get();
}
void WuWebView::setWorkunit(IConstWorkUnit &_cw)
{
    cw.set(&_cw);
    if (!name.length())
    {
        name.set(cw->queryJobName());
        name.replace(' ','_');
    }
    Owned<IConstWUQuery> q = cw->getQuery();
    if (q)
    {
        q->getQueryDllName(dllname);
        if (!delayedDll)
            loadDll(true);
    }
}

void WuWebView::setWorkunit(const char *wuid)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuid);
    if (!wu)
        throw MakeStringException(WUWEBERR_WorkUnitNotFound, "Workunit not found %s", wuid);
    setWorkunit(*wu);
}

void WuWebView::addVariableFromPTree(IWorkUnit *w, IConstWUResult &vardef, IResultSetMetaData &metadef, const char *varname, IPropertyTree *valtree)
{
    if (!varname || !*varname)
        return;

    Owned<IWUResult> var = w->updateVariableByName(varname);
    if (!vardef.isResultScalar())
    {
        StringBuffer ds;
        if (valtree->hasChildren())
            toXML(valtree, ds);
        else
        {
            const char *val = valtree->queryProp(NULL);
            if (val)
                decodeXML(val, ds);
        }
        if (ds.length())
            var->setResultRaw(ds.length(), ds.str(), ResultFormatXml);
    }
    else
    {
        const char *val = valtree->queryProp(NULL);
        if (val && *val)
        {
            switch (metadef.getColumnDisplayType(0))
            {
                case TypeBoolean:
                    var->setResultBool(strieq(val, "1") || strieq(val, "true") || strieq(val, "on"));
                    break;
                case TypeInteger:
                    var->setResultInt(_atoi64(val));
                    break;
                case TypeUnsignedInteger:
                    var->setResultInt(_atoi64(val));
                    break;
                case TypeReal:
                    var->setResultReal(atof(val));
                    break;
                case TypeSet:
                case TypeDataset:
                case TypeData:
                    var->setResultRaw(strlen(val), val, ResultFormatRaw);
                    break;
                case TypeUnicode: {
                    MemoryBuffer target;
                    convertUtf(target, UtfReader::Utf16le, strlen(val), val, UtfReader::Utf8);
                    var->setResultUnicode(target.toByteArray(), (target.length()>1) ? target.length()/2 : 0);
                    }
                    break;
                case TypeString:
                case TypeUnknown:
                default:
                    var->setResultString(val, strlen(val));
                    break;
                    break;
            }

            var->setResultStatus(ResultStatusSupplied);
        }
    }
}

void WuWebView::addInputsFromPTree(IPropertyTree *pt)
{
    IPropertyTree *start = pt;
    if (start->hasProp("Envelope"))
        start=start->queryPropTree("Envelope");
    if (start->hasProp("Body"))
        start=start->queryPropTree("Body/*[1]");

    Owned<IResultSetFactory> resultSetFactory(getResultSetFactory(username.get(), pw.get()));
    Owned<IPropertyTreeIterator> it = start->getElements("*");

    WorkunitUpdate wu(&cw->lock());

    ForEach(*it)
    {
        IPropertyTree &eclparm=it->query();
        const char *varname = eclparm.queryName();

        IConstWUResult *vardef = wu->getVariableByName(varname);
        if (vardef)
        {
            Owned<IResultSetMetaData> metadef = resultSetFactory->createResultSetMeta(vardef);
            if (metadef)
                addVariableFromPTree(wu.get(), *vardef, *metadef, varname, &eclparm);
        }
    }
}

void WuWebView::addInputsFromXml(const char *xml)
{
    Owned<IPropertyTree> pt = createPTreeFromXMLString(xml, ipt_none, (PTreeReaderOptions)(ptr_ignoreWhiteSpace|ptr_ignoreNameSpaces));
    addInputsFromPTree(pt.get());
}

bool WuWebView::getEmbeddedArchive(StringBuffer &ret)
{
    if (!loadDll())
        return false;
    if (getEmbeddedArchiveXML(dll, ret))
        return true;
    // Try the old way, in case it's an older dll
    StringBuffer dllXML;
    if (!getEmbeddedWorkUnitXML(dll, dllXML))
        return false;

    Owned<ILocalWorkUnit> embeddedWU = createLocalWorkUnit(dllXML.str());
    Owned<IConstWUQuery> embeddedQuery = embeddedWU->getQuery();
    if (!embeddedQuery)
        return false;
    StringBufferAdaptor iret(ret);
    embeddedQuery->getQueryText(iret);
    return true;
}


extern WUWEBVIEW_API IWuWebView *createWuWebView(IConstWorkUnit &wu, const char *target, const char *queryname, const char *dir, bool mapEspDirectories, IPropertyTree *xsltcfg)
{
    try
    {
        return new WuWebView(wu, target, queryname, dir, mapEspDirectories, true, xsltcfg);
    }
    catch (IException *e)
    {
        VStringBuffer msg("ERROR loading workunit %s shared object.", wu.queryWuid());
        EXCLOG(e, msg.str());
        e->Release();
    }
    catch (...)
    {
        DBGLOG("ERROR loading workunit %s shared object.", wu.queryWuid());
    }
    return NULL;
}

extern WUWEBVIEW_API IWuWebView *createWuWebView(const char *wuid, const char *target, const char *queryname, const char *dir, bool mapEspDirectories, IPropertyTree *xsltcfg)
{
    try
    {
        return new WuWebView(wuid, target, queryname, dir, mapEspDirectories, true, xsltcfg);
    }
    catch (IException *e)
    {
        VStringBuffer msg("ERROR loading workunit %s shared object.", wuid);
        EXCLOG(e, msg.str());
        e->Release();
    }
    catch (...)
    {
        DBGLOG("ERROR loading workunit %s shared object.", wuid);
    }
    return NULL;
}

const char *mimeTypeFromFileExt(const char *ext)
{
    if (!ext)
        return "application/octet-stream";
    if (*ext=='.')
        ext++;
    if (strieq(ext, "html") || strieq(ext, "htm"))
        return "text/html";
    if (strieq(ext, "xml") || strieq(ext, "xsl") || strieq(ext, "xslt"))
       return "application/xml";
    if (strieq(ext, "js"))
       return "text/javascript";
    if (strieq(ext, "css"))
       return "text/css";
    if (strieq(ext, "jpeg") || strieq(ext, "jpg"))
       return "image/jpeg";
    if (strieq(ext, "gif"))
       return "image/gif";
    if (strieq(ext, "png"))
       return "image/png";
    if (strieq(ext, "svg"))
       return "image/svg+xml";
    if (strieq(ext, "txt") || strieq(ext, "text"))
       return "text/plain";
    if (strieq(ext, "zip"))
       return "application/zip";
    if (strieq(ext, "pdf"))
       return "application/pdf";
    if (strieq(ext, "xpi"))
       return "application/x-xpinstall";
    if (strieq(ext, "exe") || strieq(ext, "class"))
       return "application/octet-stream";
    return "application/octet-stream";
}

static void getQueryInfoFromPath(const char *&path, const char *op, StringBuffer &target, StringBuffer &queryname, StringBuffer &wuid)
{
    StringBuffer s;
    nextPathNode(path, s);
    if (op && strieq(s, op))
        nextPathNode(path, s.clear());
    if (strieq(s, "query"))
    {
        nextPathNode(path, target);
        if (!target.length())
            throw MakeStringException(WUWEBERR_TargetNotFound, "Target cluster required");
        nextPathNode(path, queryname);
        Owned<IPropertyTree> query = resolveQueryAlias(target, queryname, true);
        if (!query)
            throw MakeStringException(WUWEBERR_QueryNotFound, "Query not found");
        wuid.set(query->queryProp("@wuid"));
    }
    else
        wuid.swapWith(s);
}
extern WUWEBVIEW_API void getWuResourceByPath(const char *path, MemoryBuffer &mb, StringBuffer &mimetype)
{
    StringBuffer wuid, target, queryname;
    getQueryInfoFromPath(path, "res", target, queryname, wuid);

    Owned<IWuWebView> web = createWuWebView(wuid, target, queryname, NULL, true, nullptr);
    if (!web)
        throw MakeStringException(WUWEBERR_WorkUnitNotFound, "Cannot open workunit");
    mimetype.append(mimeTypeFromFileExt(strrchr(path, '.')));
    if (!web->getResourceByPath(path, mb))
        throw MakeStringException(WUWEBERR_ViewResourceNotFound, "Cannot open resource");
}

extern WUWEBVIEW_API void getWuManifestByPath(const char *path, StringBuffer &mf)
{
    StringBuffer wuid, target, queryname;
    getQueryInfoFromPath(path, "manifest", target, queryname, wuid);

    Owned<IWuWebView> web = createWuWebView(wuid, target, queryname, NULL, true, nullptr);
    if (!web)
        throw MakeStringException(WUWEBERR_WorkUnitNotFound, "Cannot open workunit");
    if (!web->getManifest(mf).length())
        throw MakeStringException(WUWEBERR_ViewResourceNotFound, "Cannot open manifest");
}

extern WUWEBVIEW_API void getWuResourceUrlListByPath(const char *path, StringBuffer &fmt, StringBuffer &content, const char *prefix)
{
    StringBuffer wuid, target, queryname;
    getQueryInfoFromPath(path, "resurls", target, queryname, wuid);
    nextPathNode(path, fmt);
    if (!fmt.length())
        fmt.set("xml");

    Owned<IWuWebView> web = createWuWebView(wuid, target, queryname, NULL, true, nullptr);
    if (!web)
        throw MakeStringException(WUWEBERR_WorkUnitNotFound, "Cannot open workunit");
    StringArray urls;
    web->getResourceURLs(urls, prefix);

    bool json = strieq(fmt, "json");
    content.append(json ? "{\"url\": [" : "<ResourceUrls>");
    ForEachItemIn(i, urls)
    {
        if (json)
            appendJSONValue(content, NULL, urls.item(i));
        else
            appendXMLTag(content, "url", urls.item(i));
    }
    content.append(json ? "]}" : "</ResourceUrls>");
}
