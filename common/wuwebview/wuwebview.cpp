/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#include "jlib.hpp"
#include "jexcept.hpp"
#include "jptree.hpp"
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

    WuExpandedResultBuffer(const char *queryname) : name(queryname), datasetLevel(0), finalized(false)
    {
        buffer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><");
        if (queryname)
            buffer.append(queryname);
        buffer.append("Response><Results><Result>");
    }

    void appendResults(IConstWorkUnit *wu, const char *username, const char *pw)
    {
        StringBufferAdaptor resultXML(buffer);
        getFullWorkUnitResultsXML(username, pw, wu, resultXML, false, ExceptionSeverityError, true);
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
        Owned<IPullXMLReader> reader = createPullXMLStringReader(xml, *this);
        reader->load();
    }

    void appendDatasetXML(const char *xml)
    {
        assertex(!finalized);
        buffer.append(xml);
    }

    void append(const char *value)
    {
        assertex(!finalized);
        buffer.append(value);
    }

    void appendSchemaResource(IPropertyTree &res, ILoadedDllEntry &loadedDll)
    {
        if (res.getPropInt("@seq", -1)>=0 && res.hasProp("@id"))
        {
            int id = res.getPropInt("@id");
            size32_t len = 0;
            const void *data = NULL;
            if (loadedDll.getResource(len, data, "RESULT_XSD", (unsigned) id) && len>0)
            {
                StringBuffer decompressed;
                decompressResource(len, data, decompressed);
                buffer.append("<XmlSchema name=\"").append(res.queryProp("@name")).append("\">");
                buffer.append(decompressed.str());
                buffer.append("</XmlSchema>");
            }
        }
    }

    void appendManifestSchemas(IPropertyTree &manifest, ILoadedDllEntry &loadedDll)
    {
        assertex(!finalized);
        Owned<IPropertyTreeIterator> iter = manifest.getElements("Resource[@type='RESULT_XSD']");
        ForEach(*iter)
            appendSchemaResource(iter->query(), loadedDll);
    }

    void appendManifestResultSchema(IPropertyTree &manifest, const char *resultname, ILoadedDllEntry &loadedDll)
    {
        assertex(!finalized);
        VStringBuffer xpath("Resource[@name='%s']", resultname);
        Owned<IPropertyTreeIterator> iter = manifest.getElements(xpath.str());
        ForEach(*iter)
        {
            const char *type=iter->query().queryProp("@type");
            if (type && strieq(type, "RESULT_XSD"))
                appendSchemaResource(iter->query(), loadedDll);
        }
    }

    virtual void beginNode(const char *tag, offset_t startOffset)
    {
        if (streq("Dataset", tag))
            datasetLevel++;
        if (datasetLevel)
            buffer.append('<').append(tag);
    }

    virtual void newAttribute(const char *name, const char *value)
    {
        if (datasetLevel)
            buffer.append(' ').append(name+1).append("=\"").append(value).append('\"');
    }
    virtual void beginNodeContent(const char *tag)
    {
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
                    buffer.append((const char *)value);
            }
            buffer.append("</").append(tag).append('>');
            if (streq("Dataset", tag))
                datasetLevel--;
        }
    }
    StringBuffer &finalize()
    {
        if (!finalized)
        {
            buffer.appendf("</Result></Results></%sResponse>", name.sget());
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
    bool finalized;
private:
    int datasetLevel;
};

inline bool isAbsoluteXalanPath(const char *path)
{
    if (!path||!*path)
        return false;
    return isPathSepChar(path[0])||((path[1]==':')&&(isPathSepChar(path[2])));
}

class WuWebView : public CInterface,
    implements IWuWebView,
    implements IIncludeHandler
{
public:
    IMPLEMENT_IINTERFACE;
    WuWebView(IConstWorkUnit &wu, const char *queryname, const char *wdir, bool mapEspDir) :
        manifestIncludePathsSet(false), dir(wdir), mapEspDirectories(mapEspDir)
    {
        name.set(queryname);
        load(wu);
    }

    WuWebView(const char *wuid, const char *queryname, const char *wdir, bool mapEspDir) :
        manifestIncludePathsSet(false), dir(wdir), mapEspDirectories(mapEspDir)
    {
        name.set(queryname);
        load(wuid);
    }

    void load(IConstWorkUnit &wu);
    void load(const char *wuid);
    IPropertyTree *ensureManifest();

    virtual void getResultViewNames(StringArray &names);
    virtual void renderResults(const char *viewName, const char *xml, StringBuffer &html);
    virtual void renderResults(const char *viewName, StringBuffer &html);
    virtual void renderSingleResult(const char *viewName, const char *resultname, StringBuffer &html);
    virtual void applyResultsXSLT(const char *filename, const char *xml, StringBuffer &out);
    virtual void applyResultsXSLT(const char *filename, StringBuffer &out);
    virtual StringBuffer &aggregateResources(const char *type, StringBuffer &content);

    void renderExpandedResults(const char *viewName, StringBuffer &expanded, StringBuffer &out);

    void appendResultSchemas(WuExpandedResultBuffer &buffer);
    void getResultXSLT(const char *viewName, StringBuffer &xslt, StringBuffer &abspath);
    void getResource(IPropertyTree *res, StringBuffer &content);
    void getResource(const char *name, StringBuffer &content, StringBuffer &abspath);

    void calculateResourceIncludePaths();
    virtual bool getInclude(const char *includename, MemoryBuffer &includebuf, bool &pathOnly);

protected:
    Owned<IConstWorkUnit> wu;
    Owned<ILoadedDllEntry> loadedDll;
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
        manifest.setown(getEmbeddedManifestXML(loadedDll, xml) ? createPTreeFromXMLString(xml.str()) : createPTree());
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
            const char *filename = iter->query().queryProp("@filename");
            StringBuffer abspath;
            if (isAbsoluteXalanPath(filename))
                abspath.append(filename);
            else
            {
                StringBuffer relpath(dir.get());
                relpath.append(filename);
                makeAbsolutePath(relpath.str(), abspath);
            }
            iter->query().setProp("@res_include_path", abspath.str());
        }
        manifestIncludePathsSet=true;
    }
}

bool WuWebView::getInclude(const char *includename, MemoryBuffer &includebuf, bool &pathOnly)
{
    int len=strlen(includename);
    if (len<8)
        return false;
    //eliminate "file://"
    includename+=7;
    //eliminate extra '/' for windows absolute paths
    if (len>9 && includename[2]==':')
        includename++;
    StringBuffer relpath;
    if (mapEspDirectories && !strnicmp(includename, "/esp/", 5))
        relpath.append(dir.get()).append(includename+=5);
    else
        relpath.append(includename); //still correct for OS
    StringBuffer abspath;
    makeAbsolutePath(relpath.str(), abspath);

    VStringBuffer xpath("Resource[@res_include_path='%s']", abspath.str());
    IPropertyTree *res = manifest->queryPropTree(xpath.str());
    if (res)
    {
        StringBuffer xslt;
        getResource(res, xslt);
        includebuf.append(xslt.str());
    }
    else if (checkFileExists(abspath.str()))
    {
        Owned <IFile> f = createIFile(abspath.str());
        Owned <IFileIO> fio = f->open(IFOread);
        read(fio, 0, (size32_t) f->size(), includebuf);
    }
    //esp looks in two places for path starting with "xslt/"
    else if (mapEspDirectories && !strncmp(includename, "xslt/", 5))
    {
        StringBuffer relpath(dir.get());
        relpath.append("smc_").append(includename);;
        makeAbsolutePath(relpath.str(), abspath.clear());
        if (checkFileExists(abspath.str()))
        {
            Owned <IFile> f = createIFile(abspath.str());
            Owned <IFileIO> fio = f->open(IFOread);
            read(fio, 0, (size32_t) f->size(), includebuf);
        }
    }
    return true;
}

void WuWebView::getResultViewNames(StringArray &names)
{
    Owned<IPropertyTreeIterator> iter = ensureManifest()->getElements("Views/Results[@name]");
    ForEach(*iter)
    names.append(iter->query().queryProp("@name"));
}

void WuWebView::getResource(IPropertyTree *res, StringBuffer &content)
{
    if (res->hasProp("@id"))
    {
        int id = res->getPropInt("@id");
        size32_t len = 0;
        const void *data = NULL;
        if (loadedDll->getResource(len, data, res->queryProp("@type"), (unsigned) id) && len>0)
            decompressResource(len, data, content);
    }
}

void WuWebView::getResource(const char *name, StringBuffer &content, StringBuffer &includepath)
{
    VStringBuffer xpath("Resource[@name='%s']", name);
    IPropertyTree *res = ensureManifest()->queryPropTree(xpath.str());
    calculateResourceIncludePaths();
    includepath.append(res->queryProp("@res_include_path"));
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
    VStringBuffer xpath("Views/Results[@name='%s']/@resource", viewName);
    const char *resource = ensureManifest()->queryProp(xpath.str());
    if (resource)
        getResource(resource, xslt, abspath);
}

void WuWebView::renderExpandedResults(const char *viewName, StringBuffer &expanded, StringBuffer &out)
{
    StringBuffer xslt;
    StringBuffer rootpath;
    getResultXSLT(viewName, xslt, rootpath);
    if (!xslt.length())
        throw MakeStringException(WUWEBERR_ViewResourceNotFound, "Result view %s not found", viewName);
    Owned<IXslTransform> t = getXslProcessor()->createXslTransform();
    t->setIncludeHandler(this);
    t->setXslSource(xslt.str(), xslt.length(), rootpath.str());
    t->setXmlSource(expanded.str(), expanded.length());
    t->transform(out);
}

void WuWebView::renderResults(const char *viewName, const char *xml, StringBuffer &out)
{
    WuExpandedResultBuffer buffer(name.str());
    buffer.appendDatasetsFromXML(xml);
    buffer.appendManifestSchemas(*ensureManifest(), *loadedDll);
    renderExpandedResults(viewName, buffer.finalize(), out);
}

void WuWebView::renderResults(const char *viewName, StringBuffer &out)
{
    WuExpandedResultBuffer buffer(name.str());
    buffer.appendResults(wu, username.get(), pw.get());
    buffer.appendManifestSchemas(*ensureManifest(), *loadedDll);
    renderExpandedResults(viewName, buffer.finalize(), out);
}

void WuWebView::renderSingleResult(const char *viewName, const char *resultname, StringBuffer &out)
{
    WuExpandedResultBuffer buffer(name.str());
    buffer.appendManifestResultSchema(*ensureManifest(), resultname, *loadedDll);
    buffer.appendSingleResult(wu, resultname, username.get(), pw.get());
    renderExpandedResults(viewName, buffer.finalize(), out);
}

void WuWebView::applyResultsXSLT(const char *filename, const char *xml, StringBuffer &out)
{
    WuExpandedResultBuffer buffer(name.str());
    buffer.appendDatasetsFromXML(xml);
    buffer.appendManifestSchemas(*ensureManifest(), *loadedDll);

    Owned<IXslTransform> t = getXslProcessor()->createXslTransform();
    t->setIncludeHandler(this);
    t->setXslSource(filename);
    t->setXmlSource(buffer.str(), buffer.length());
    t->transform(out);
}

void WuWebView::applyResultsXSLT(const char *filename, StringBuffer &out)
{
    SCMStringBuffer xml;
    getFullWorkUnitResultsXML(username.get(), pw.get(), wu, xml, false, ExceptionSeverityError);
    applyResultsXSLT(filename, xml.str(), out);
}

void WuWebView::load(IConstWorkUnit &cwu)
{
    wu.set(&cwu);
    if (!name.length())
    {
        wu->getJobName(name);
        name.s.replace(' ','_');
    }
    Owned<IConstWUQuery> q = wu->getQuery();
    SCMStringBuffer dllname;
    q->getQueryDllName(dllname);
    loadedDll.setown(queryDllServer().loadDll(dllname.str(), DllLocationAnywhere));
}

void WuWebView::load(const char *wuid)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuid, false);
    if (!wu)
        throw MakeStringException(WUWEBERR_WorkUnitNotFound, "Workunit not found %s", wuid);
    load(*wu);
}

extern WUWEBVIEW_API IWuWebView *createWuWebView(IConstWorkUnit &wu, const char *queryname, const char *dir, bool mapEspDirectories)
{
    return new WuWebView(wu, queryname, dir, mapEspDirectories);
}

extern WUWEBVIEW_API IWuWebView *createWuWebView(const char *wuid, const char *queryname, const char *dir, bool mapEspDirectories)
{
    return new WuWebView(wuid, queryname, dir, mapEspDirectories);
}

