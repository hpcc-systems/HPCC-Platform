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

class WuExpandedResultBuffer : public CInterface, implements IPTreeNotifyEvent
{
public:
    IMPLEMENT_IINTERFACE;

    WuExpandedResultBuffer(const char *queryname, unsigned _flags=0) :
        name(queryname), datasetLevel(0), finalized(false), flags(_flags), hasXmlns(false)
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
            if (flags & WWV_INCL_NAMESPACES && ns.length())
                buffer.append(" xmlns=\"").append(ns.str()).append('\"');
            buffer.append('>');
        }
        if (flags & WWV_ADD_RESULTS_TAG)
            buffer.append("<Results>");
        if (!(flags & WWV_OMIT_RESULT_TAG))
            buffer.append("<Result>");
    }

    void appendResults(IConstWorkUnit *wu, const char *username, const char *pw)
    {
        StringBufferAdaptor resultXML(buffer);
        getFullWorkUnitResultsXML(username, pw, wu, resultXML, WorkUnitXML_NoRoot, ExceptionSeverityError);
    }

    void appendSingleResult(IConstWorkUnit *wu, const char *resultname, const char *username, const char *pw)
    {
        SCMStringBuffer wuid;
        StringBufferAdaptor resultXML(buffer);
        Owned<IResultSetFactory> factory = getResultSetFactory(username, pw);
        Owned<INewResultSet> nr = factory->createNewResultSet(wu->getWuid(wuid).str(), 0, resultname);
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
        assertex(!finalized);
        if (!dll)
            return;
        Owned<IPropertyTreeIterator> iter = manifest.getElements("Resource[@type='RESULT_XSD']");
        ForEach(*iter)
            appendSchemaResource(iter->query(), dll);
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

    virtual void beginNode(const char *tag, offset_t startOffset)
    {
        if (streq("Dataset", tag) || streq("Exception", tag))
            datasetLevel++;
        if (datasetLevel)
            buffer.append('<').append(tag);
    }

    virtual void newAttribute(const char *name, const char *value)
    {
        if (datasetLevel)
        {
            if (streq(name, "@xmlns"))
            {
                if (!(flags & WWV_INCL_NAMESPACES))
                    return;
                if (datasetLevel==1)
                    hasXmlns=true;
            }
            if (datasetLevel==1 && streq(name, "@name"))
                dsname.set(value).toLowerCase().replace(' ', '_');;
            buffer.append(' ').append(name+1).append("=\"");
            encodeUtf8XML(value, buffer);
            buffer.append('\"');
        }
    }
    virtual void beginNodeContent(const char *tag)
    {
        if (datasetLevel==1 && streq("Dataset", tag))
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
        if (datasetLevel)
            buffer.append('>');
    }
    virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
    {
        if (datasetLevel)
        {
            if (length)
            {
                if (binary)
                    JBASE64_Encode(value, length, buffer);
                else
                    encodeUtf8XML((const char *)value, buffer);
            }
            buffer.append("</").append(tag).append('>');
            if (streq("Dataset", tag) || streq("Exception", tag))
                datasetLevel--;
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
                buffer.appendf("</%sResponse>", name.sget());
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
private:
    int datasetLevel;
};

class WuWebView : public CInterface,
    implements IWuWebView,
    implements IIncludeHandler
{
public:
    IMPLEMENT_IINTERFACE;

    WuWebView(IConstWorkUnit &wu, const char *queryname, const char *wdir, bool mapEspDir, bool delay=true) :
        manifestIncludePathsSet(false), dir(wdir), mapEspDirectories(mapEspDir), delayedDll(delay)
    {
        name.set(queryname);
        setWorkunit(wu);
    }

    WuWebView(const char *wuid, const char *queryname, const char *wdir, bool mapEspDir, bool delay=true) :
        manifestIncludePathsSet(false), dir(wdir), mapEspDirectories(mapEspDir), delayedDll(delay)
    {
        name.set(queryname);
        setWorkunit(wuid);
    }

    void setWorkunit(IConstWorkUnit &wu);
    void setWorkunit(const char *wuid);
    ILoadedDllEntry *loadDll(bool force=false);

    IPropertyTree *ensureManifest();

    virtual void getResultViewNames(StringArray &names);
    virtual void renderResults(const char *viewName, const char *xml, StringBuffer &html);
    virtual void renderResults(const char *viewName, StringBuffer &html);
    virtual void renderSingleResult(const char *viewName, const char *resultname, StringBuffer &html);
    virtual void expandResults(const char *xml, StringBuffer &out, unsigned flags);
    virtual void expandResults(StringBuffer &out, unsigned flags);
    virtual void applyResultsXSLT(const char *filename, const char *xml, StringBuffer &out);
    virtual void applyResultsXSLT(const char *filename, StringBuffer &out);
    virtual StringBuffer &aggregateResources(const char *type, StringBuffer &content);

    void renderExpandedResults(const char *viewName, WuExpandedResultBuffer &expanded, StringBuffer &out);

    void appendResultSchemas(WuExpandedResultBuffer &buffer);
    void getResultXSLT(const char *viewName, StringBuffer &xslt, StringBuffer &abspath);
    void getResource(IPropertyTree *res, StringBuffer &content);
    void getResource(const char *name, StringBuffer &content, StringBuffer &abspath, const char *type);

    void calculateResourceIncludePaths();
    virtual bool getInclude(const char *includename, MemoryBuffer &includebuf, bool &pathOnly);
    bool getEspInclude(const char *includename, MemoryBuffer &includebuf, bool &pathOnly);


    void addVariableFromPTree(IWorkUnit *w, IConstWUResult &vardef, IResultSetMetaData &metadef, const char *varname, IPropertyTree *valtree);
    void addInputsFromPTree(IPropertyTree *pt);
    void addInputsFromXml(const char *xml);


protected:
    SCMStringBuffer dllname;
    Owned<IConstWorkUnit> cw;
    Owned<ILoadedDllEntry> dll;
    bool delayedDll;
    Owned<IPropertyTree> manifest;
    SCMStringBuffer name;
    bool mapEspDirectories;
    bool manifestIncludePathsSet;
    StringAttr dir;
    StringAttr username;
    StringAttr pw;
};

IPropertyTree *WuWebView::ensureManifest()
{
    if (!manifest)
    {
        StringBuffer xml;
        manifest.setown((loadDll() && getEmbeddedManifestXML(dll, xml)) ? createPTreeFromXMLString(xml.str()) : createPTree());
    }
    return manifest.get();
}

void WuWebView::calculateResourceIncludePaths()
{
    if (!manifestIncludePathsSet)
    {
        Owned<IPropertyTreeIterator> iter = ensureManifest()->getElements("Resource[@filename]");
        ForEach(*iter)
        {
            if (!iter->query().hasProp("@resourcePath")) //backward compatible
            {
                StringBuffer abspath;
                makeAbsolutePath(iter->query().queryProp("@filename"), dir.get(), abspath);

                StringBuffer respath;
                makePathUniversal(abspath.str(), respath);
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
    int len=strlen(includename);
    if (len<8)
        return false;
    //eliminate "file://"
    if (strncmp(includename, "file://", 7)==0)
        includename+=7;
    //eliminate extra '/' for windows absolute paths
    if (len>9 && includename[2]==':')
        includename++;
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

void WuWebView::getResultViewNames(StringArray &names)
{
    Owned<IPropertyTreeIterator> iter = ensureManifest()->getElements("Views/Results[@name]");
    ForEach(*iter)
        names.append(iter->query().queryProp("@name"));
    if (manifest->hasProp("Views/XSLT/RESULTS[@resource='Results']"))
        names.append("EmbeddedView");
}

void WuWebView::getResource(IPropertyTree *res, StringBuffer &content)
{
    if (!loadDll())
        return;
    if (res->hasProp("@id"))
    {
        int id = res->getPropInt("@id");
        size32_t len = 0;
        const void *data = NULL;
        if (dll->getResource(len, data, res->queryProp("@type"), (unsigned) id) && len>0)
        {
            if (res->getPropBool("@compressed"))
            {
                StringBuffer decompressed;
                decompressResource(len, data, content);
                content.append(decompressed.str());
            }
            else
                content.append(len, (const char *)data);
        }
    }
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

    Owned<IXslTransform> t = getXslProcessor()->createXslTransform();
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

void WuWebView::renderSingleResult(const char *viewName, const char *resultname, StringBuffer &out)
{
    WuExpandedResultBuffer buffer(name.str(), WWV_ADD_RESPONSE_TAG | WWV_ADD_RESULTS_TAG);
    buffer.appendSingleResult(cw, resultname, username.get(), pw.get());
    renderExpandedResults(viewName, buffer, out);
}

void WuWebView::expandResults(const char *xml, StringBuffer &out, unsigned flags)
{
    WuExpandedResultBuffer expander(name.str(), flags);
    expander.appendDatasetsFromXML(xml);
    expander.appendManifestSchemas(*ensureManifest(), loadDll());
    expander.finalize();
    out.append(expander.buffer);
}

void WuWebView::expandResults(StringBuffer &out, unsigned flags)
{
    SCMStringBuffer xml;
    getFullWorkUnitResultsXML(username.get(), pw.get(), cw, xml);
    expandResults(xml.str(), out, flags);
}

void WuWebView::applyResultsXSLT(const char *filename, const char *xml, StringBuffer &out)
{
    WuExpandedResultBuffer buffer(name.str(), WWV_ADD_RESPONSE_TAG | WWV_ADD_RESULTS_TAG);
    buffer.appendDatasetsFromXML(xml);
    buffer.appendManifestSchemas(*ensureManifest(), loadDll());

    Owned<IXslTransform> t = getXslProcessor()->createXslTransform();
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
    if (!dll && (force || delayedDll))
    {
        try
        {
            dll.setown(queryDllServer().loadDll(dllname.str(), DllLocationAnywhere));
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
        cw->getJobName(name);
        name.s.replace(' ','_');
    }
    Owned<IConstWUQuery> q = cw->getQuery();
    q->getQueryDllName(dllname);
    if (!delayedDll)
        loadDll(true);
}

void WuWebView::setWorkunit(const char *wuid)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuid, false);
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

extern WUWEBVIEW_API IWuWebView *createWuWebView(IConstWorkUnit &wu, const char *queryname, const char *dir, bool mapEspDirectories)
{
    try
    {
        return new WuWebView(wu, queryname, dir, mapEspDirectories);
    }
    catch (...)
    {
        SCMStringBuffer wuid;
        DBGLOG("ERROR loading workunit %s shared object.", wu.getWuid(wuid).str());
    }
    return NULL;
}

extern WUWEBVIEW_API IWuWebView *createWuWebView(const char *wuid, const char *queryname, const char *dir, bool mapEspDirectories)
{
    try
    {
        return new WuWebView(wuid, queryname, dir, mapEspDirectories);
    }
    catch (...)
    {
        DBGLOG("ERROR loading workunit %s shared object.", wuid);
    }
    return NULL;
}

