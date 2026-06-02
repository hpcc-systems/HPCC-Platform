#include "jliball.hpp"
#include <mutex>
#include <string.h>

#if defined(__clang__) || defined(__GNUC__)
//Disable the warning for the whole file.  It does not work if it only surrounds the #includes
#pragma GCC diagnostic ignored "-Wparentheses"
#endif

#include <libxml/xmlmemory.h>
#include <libxml/parserInternals.h>
#include <libxml/debugXML.h>
#include <libxml/HTMLtree.h>
#include <libxml/xmlIO.h>
#include <libxml/xinclude.h>
#include <libxml/catalog.h>
#include <libxml/xpathInternals.h>
#include <libxml/xmlsave.h>
#include <libxslt/xslt.h>
#include <libxslt/xsltInternals.h>
#include <libxslt/transform.h>
#include <libxslt/documents.h>
#include <libxslt/xsltutils.h>
#include <libxslt/extensions.h>
#include <libxslt/variables.h>
#include <libexslt/exslt.h>

#ifdef _WIN32
#undef new
#undef delete
#endif

#include "xslprocessor.hpp"
#include "xslcache.hpp"
#include "xmlerror.hpp"

xsltDocLoaderFunc originalLibXsltIncludeHandler = NULL;

xmlDocPtr libXsltIncludeHandler(const xmlChar * URI, xmlDictPtr dict, int options, IIncludeHandler* handler, xsltLoadType type);
xmlDocPtr globalLibXsltIncludeHandler(const xmlChar * URI, xmlDictPtr dict, int options, void *ctx, xsltLoadType type);
static void libxsltErrorMsgHandler(void *ctx, const char *format, ...) __attribute__((format(printf,2,3)));

namespace
{
std::once_flag xmlLibraryInitFlag;

// Allow internal entity substitution only, no external DTD or entities.
constexpr int hpccXsltStylesheetParseOptions = XML_PARSE_NOENT | XML_PARSE_NO_XXE;

// For parsing documents loaded at transform runtime via document():
// Continues to allow that CDATA sections in loaded documents are merged into
// text nodes, preserving text() matching behavior.
constexpr int hpccXsltDocumentParseOptions = XML_PARSE_NOENT | XML_PARSE_NOCDATA | XML_PARSE_NO_XXE;

xmlDocPtr readXmlMemoryWithOptions(const char *buffer, int size, const char *url, xmlDictPtr dict, int options)
{
    xmlParserCtxtPtr context = xmlNewParserCtxt();
    if (!context)
        return NULL;

    if (dict)
        xmlCtxtSetDict(context, dict);

    xmlDocPtr document = xmlCtxtReadMemory(context, buffer, size, url, NULL, options);
    xmlFreeParserCtxt(context);
    return document;
}

void initializeXmlLibraries()
{
    std::call_once(xmlLibraryInitFlag, []()
    {
        // libxml2/libxslt initialization is process-global state. Initialize it
        // once on first use and leave process teardown to the runtime.
        xmlInitParser();

        xsltMaxDepth = 100000;
        xsltSetLoaderFunc(NULL);
        originalLibXsltIncludeHandler = xsltDocDefaultLoader;
        xsltSetLoaderFunc(globalLibXsltIncludeHandler);
        exsltRegisterAll();
        xsltSetGenericErrorFunc(NULL, libxsltErrorMsgHandler);
    });
}
}

void libxsltCustomMessageHandler(StringBuffer& out, const char* in, IXslTransform* transform);

class CLibXsltSource : implements IXslBuffer, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CLibXsltSource(const char* fname) : filename(fname), cacheId(fname), compiledXslt(NULL)
    {
        srcType = IO_TYPE_FILE;
    }

    CLibXsltSource(IIncludeHandler *handler, const char* rootpath, const char *_cacheId) : filename(rootpath), cacheId(_cacheId), compiledXslt(NULL)
    {
        srcType = IO_TYPE_BUFFER;
        bool pathOnly=false;
        MemoryBuffer mb;
        if (!handler->getInclude(rootpath, mb, pathOnly) || pathOnly || !mb.length())
            throw MakeStringException(XSLERR_InvalidSource, "Failed to load XSLT resource path %s\n", rootpath);
        text.set(mb.toByteArray(), mb.length());
        StringBuffer s("file://");
        if (*rootpath!='/')
            s.append('/');
        filename.set(s.append(rootpath).str());
    }

    CLibXsltSource(const char* s, int len, const char *_cacheId) : cacheId(_cacheId), compiledXslt(NULL)
    {
        srcType = IO_TYPE_BUFFER;
        text.set(s, len);
        filename.set("buffer.xslt");
    }

    virtual ~CLibXsltSource()
    {
        if (compiledXslt)
            xsltFreeStylesheet(compiledXslt);
    }

    virtual void compile();
    virtual bool isCompiled() const {return compiledXslt!=NULL;}
    virtual IO_Type getType() {return srcType;}
    virtual const char* getFileName(){return filename.get();}
    virtual char* getBuf(){return (char*)text.get();}
    virtual int getLen(){return text.length();}
    virtual StringArray& getIncludes(){return includes;}
    virtual const char *getCacheId(){return cacheId.get();}

    void setIncludeHandler(IIncludeHandler* handler){includeHandler.set(handler);}

    xsltStylesheetPtr getCompiledXslt();
    xsltStylesheetPtr parseXsltFile();

public:
    IO_Type srcType;
    StringAttr filename;
    StringAttr cacheId;
    StringAttr text;

    Owned<IIncludeHandler> includeHandler;
    StringArray includes;

    xsltStylesheetPtr compiledXslt;
};

xsltStylesheetPtr CLibXsltSource::getCompiledXslt()
{
    if (compiledXslt)
        return compiledXslt;

    IXslCache* xslcache = getXslCache();
    if (xslcache)
    {
        IXslBuffer* xslbuffer = xslcache->getCompiledXsl(this, false);
        if (xslbuffer)
            return ((CLibXsltSource*)xslbuffer)->compiledXslt;
    }

    compile();
    return compiledXslt;
}

xsltStylesheetPtr CLibXsltSource::parseXsltFile()
{
    if (filename.isEmpty())
        return NULL;

    xmlDocPtr doc = xsltDocDefaultLoader((xmlChar *)filename.get(), NULL, hpccXsltStylesheetParseOptions, NULL, XSLT_LOAD_START);
    if (!doc)
        throw MakeStringException(XSLERR_InvalidSource, "Failed to parse XSLT source\n");
    doc->_private = static_cast<void *>(this);

    xsltStylesheetPtr style = xsltParseStylesheetDoc(doc);
    if (!style)
    {
        xmlFreeDoc(doc);
        throw MakeStringException(XSLERR_InvalidSource, "Failed to parse XSLT source\n");
    }

    return style;
}

void CLibXsltSource::compile()
{
    if (!compiledXslt)
    {
        if ((srcType == IO_TYPE_FILE && filename.isEmpty()) || (srcType == IO_TYPE_BUFFER && text.isEmpty()))
            throw MakeStringException(XSLERR_MissingSource, "xslt source not set");

        if (compiledXslt != NULL)
        {
            xsltFreeStylesheet(compiledXslt);
            compiledXslt = NULL;
        }

        try
        {
            if (srcType == IO_TYPE_FILE)
                compiledXslt = parseXsltFile();
            else if (srcType == IO_TYPE_BUFFER)
            {
                xmlDocPtr xsldoc = xmlReadMemory(text.get(), text.length(), filename.get(), NULL, hpccXsltStylesheetParseOptions);
                if (!xsldoc)
                    throw MakeStringException(XSLERR_InvalidSource, "XSLT source contains invalid XML\n");
                xsldoc->_private=(void*)this;
                compiledXslt = xsltParseStylesheetDoc(xsldoc);
                if (!compiledXslt)
                    throw MakeStringException(XSLERR_InvalidSource, "Failed to parse XSLT source\n");

            }
            if (compiledXslt)
                compiledXslt->_private = (void*)this;
        }
        catch(...)
        {
            compiledXslt = NULL;
            throw;
        }
    }
}

class CLibXmlSource : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CLibXmlSource(const char* fname)
    {
        filename.set(fname);
        srcType = IO_TYPE_FILE;
        parsedXml = NULL;
    }

    CLibXmlSource(const char* buf, int len)
    {
        text.set(buf, len);
        srcType = IO_TYPE_BUFFER;
        parsedXml = NULL;
    }

    virtual ~CLibXmlSource()
    {
        if (parsedXml)
            xmlFreeDoc(parsedXml);
    }

    xmlDocPtr getParsedXml();

    bool isCompiled() const {return parsedXml!=NULL;}

public:
    IO_Type srcType;
    StringAttr filename;
    StringAttr text;
    xmlDocPtr parsedXml;
};

xmlDocPtr CLibXmlSource::getParsedXml()
{
    if (!parsedXml)
    {
        if ((srcType == IO_TYPE_FILE && filename.isEmpty()) ||
            (srcType == IO_TYPE_BUFFER && text.isEmpty()))
            throw MakeStringException(XSLERR_MissingXml, "xml source not set");

        if (parsedXml)
        {
            xmlFreeDoc(parsedXml);
            parsedXml = NULL;
        }

        try
        {
            if (srcType == IO_TYPE_FILE)
                parsedXml = xmlReadFile(filename.get(), NULL, 0);
            else if (srcType == IO_TYPE_BUFFER)
                parsedXml = xmlReadMemory(text.get(), text.length(), "source.xml", NULL, 0);
        }
        catch(...)
        {
            parsedXml = NULL;
            throw;
        }
    }

    return parsedXml;
}

class CLibXsltResultTarget : public CInterface, implements IInterface
{
private:
    IO_Type destType;
    StringAttr filename;
    char* dest;
    int maxLen;

public:
    IMPLEMENT_IINTERFACE;

    CLibXsltResultTarget(const char* _filename) : filename(_filename)
    {
        destType = IO_TYPE_FILE;
    }

    CLibXsltResultTarget(char* _buffer, int _len) : dest(_buffer), maxLen(_len)
    {
        destType = IO_TYPE_BUFFER;
    }

    void process(const char* content, int contLen)
    {
        if (destType == IO_TYPE_FILE)
        {
            Owned<IFile> f = createIFile(filename.get());
            Owned<IFileIO> fio = f->open(IFOwrite);
            fio->write(0, contLen, content);
        }
        else
        {
            if (contLen>maxLen)
                throw MakeStringException(XSLERR_TargetBufferToSmall, "XSLT Output greater than target buffer size");
            memcpy((void*)dest, content, contLen);
            dest[contLen] = '\0';
        }
    }
};

typedef void TextFunctionType(StringBuffer& out, const char* pszIn, IXslTransform* pTransform);

class CLibXslFunction : public CInterface, implements IXslFunction
{
public:
    IMPLEMENT_IINTERFACE;

    CLibXslFunction(const char* _name, TextFunctionType* _fn, IXslTransform* trans)
        : name(_name), fn(_fn), assigned(false), parent(trans) {}

    virtual ~CLibXslFunction(){}

    virtual const char* getName() const
    {
        return name.get();
    }

    virtual bool isAssigned ()
    {
        return assigned;
    }

    virtual void setAssigned(bool bAssigned)
    {
        assigned = bAssigned;
    }

    virtual void setURI(const char *_uri)
    {
        if (uri.isEmpty())
            uri.set(_uri);
        else if (!streq(uri.get(), _uri))
            throw MakeStringException(XSLERR_ExtenrnalFunctionMultipleURIs, "The same function cannot be assigned to multiple URIs");
    }

public:
    StringAttr name;
    StringAttr uri;
    TextFunctionType* fn;
    bool assigned;

    IXslTransform* parent;
};


class CLibXslTransform : public CInterface, implements IXslTransform
{
public:
    IMPLEMENT_IINTERFACE;

    CLibXslTransform(IPropertyTree *cfg);
    ~CLibXslTransform();
    virtual int transform();
    virtual int transform(StringBuffer &s);
    virtual int transform(ISocket* targetSocket);
    int transform(xmlChar **xmlbuff, int &len);

    virtual int setXmlSource(const char *pszFileName);
    virtual int setXmlSource(const char *pszBuffer, unsigned int nSize);

    virtual int setXslSource(const char *pszBuffer, unsigned int nSize, const char *cacheId, const char *rootpath);
    virtual int setXslNoCache(const char *pszBuffer, unsigned int nSize, const char *rootpath=NULL);
    virtual int loadXslFromFile(const char *pszFileName, const char *altCacheId=NULL);
    virtual int loadXslFromEmbedded(const char *path, const char *cacheId);

    virtual int setResultTarget(char *pszBuffer, unsigned int nSize);
    virtual int setResultTarget(const char *pszFileName);
    virtual int closeResultTarget();
    virtual int setParameter(const char *pszName, const char *pszExpression);
    virtual void copyParameters(IProperties *params);
    virtual int setStringParameter(const char *pszName, const char* pszString);

    bool checkSanity();

    virtual int setIncludeHandler(IIncludeHandler* handler)
    {
        includeHandler.set(handler);
        return 0;
    }

    virtual IXslFunction* createExternalFunction( const char* name, TextFunctionType *fn)
    {
        return new CLibXslFunction(name, fn, this);
    }

    virtual int setExternalFunction(const char* ns, IXslFunction* xfn, bool set);

    IXslFunction* queryExternalFunction(const char *ns, const char* name)
    {
        StringBuffer s(ns);
        return functions.getValue(s.append(':').append(name).str());
    }

    virtual const char* getLastError() const
    {
        return ""; //was xalan specific pass through
    }

    virtual const char* getMessages() const
    {
        return messages.str();
    }
    virtual void setUserData(void* _userData)
    {
        userData = _userData;
    }
    virtual void* getUserData() const
    {
        return userData;
    }
    IProperties *ensureParameters()
    {
        if (!xslParameters)
            xslParameters.setown(createProperties());
        return xslParameters.get();
    }
    IMultiException *ensureExceptions()
    {
        if (!exceptions)
            exceptions.setown(MakeMultiException());
        return exceptions.get();
    }
    void clearExceptions(){exceptions.clear();}
    void clearMessages(){messages.clear();}

public:
    Owned<IPropertyTree> xslConfig;
    Owned<IProperties> xslParameters;
    Owned<CLibXsltSource> xslSrc;
    Owned<CLibXmlSource> xmlSrc;
    Owned<CLibXsltResultTarget> target;
    Owned<IIncludeHandler> includeHandler;
    Owned<IXslFunction> msgfn;
    MapStringToMyClass<IXslFunction> functions;
    Owned<IMultiException> exceptions;
    StringBuffer messages;
    void *userData;
};

CLibXslTransform::CLibXslTransform(IPropertyTree *cfg)
{
    userData = NULL;
    msgfn.setown(createExternalFunction("message", libxsltCustomMessageHandler));
    setExternalFunction(SEISINT_XSLTEXT_NAMESPACE, msgfn, true);
    if (cfg)
        xslConfig.set(cfg);
}

CLibXslTransform::~CLibXslTransform()
{
}

void CLibXslTransform::copyParameters(IProperties *params)
{
    if (params)
    {
        Owned<IPropertyIterator> it = params->getIterator();
        ForEach(*it.get())
        {
            const char *name = it->getPropKey();
            const char *value = params->queryProp(name);
            setParameter(name, value);
        }
    }
}

int CLibXslTransform::setExternalFunction(const char* ns, IXslFunction* xfn, bool set)
{
    if (!xfn)
        return 0;
    CLibXslFunction *cfn = dynamic_cast<CLibXslFunction*>(xfn);
    if (!cfn)
        throw MakeStringException(XSLERR_ExternalFunctionIncompatible, "External Funciton not created for LIBXSLT");
    cfn->setURI(ns);
    if (set)
    {
        StringBuffer s(ns);
        s.append(':').append(xfn->getName());
        if (!functions.getValue(s.str()))
            functions.setValue(s.str(), xfn);
    }
    return 0;
}

bool CLibXslTransform::checkSanity()
{
    return (xslSrc.get() != NULL && xmlSrc.get() != NULL);
}

CLibXslTransform *getXsltTransformObject(xsltTransformContextPtr x)
{
    if (!x)
        return NULL;
    if (!x->_private)
        return NULL;
    return static_cast<CLibXslTransform *>(x->_private);
}

void globalLibXsltExtensionHandler(xmlXPathParserContextPtr ctxt, int nargs);

static void libxsltErrorMsgHandler(void *ctx, const char *format, ...) __attribute__((format(printf,2,3)));
static void libxsltErrorMsgHandler(void *ctx, const char *format, ...)
{
    if (!ctx)
        return;
    if (format && *format == '\n')
        return;

    CLibXslTransform *ctrans = (CLibXslTransform*)ctx;//getXsltTransformObject((xsltTransformContextPtr)ctx);
    if (!ctrans)
        return;
    va_list args;
    va_start(args, format);
    ctrans->ensureExceptions()->append(*MakeStringExceptionVA(XSLERR_TransformError, format, args));
    va_end(args);
}

int CLibXslTransform::transform(xmlChar **xmlbuff, int &len)
{
    clearExceptions();
    clearMessages();

    xsltSetGenericErrorFunc(this, libxsltErrorMsgHandler);
    if (!xmlSrc)
        throw MakeStringException(XSLERR_MissingXml, "XSLT Transform missing XML");
    else if (!xslSrc)
        throw MakeStringException(XSLERR_MissingSource, "XSL source not set");

    StringArray params;
    if (xslParameters)
    {
        Owned<IPropertyIterator> it = xslParameters->getIterator();
        ForEach(*it)
        {
            const char *name = it->getPropKey();
            if (!name || !*name)
                continue;
            const char *val = xslParameters->queryProp(name);
            if (!val || !*val)
                continue;
            params.append(name);
            params.append(val);
        }
    }
    xmlDocPtr xmldoc = xmlSrc->getParsedXml();
    xslSrc->setIncludeHandler(includeHandler.get());
    xsltStylesheetPtr xsldoc = xslSrc->getCompiledXslt();

    xsltTransformContextPtr ctxt = xsltNewTransformContext(xsldoc, xmldoc);
    if (!ctxt)
        throw MakeStringException(XSLERR_CouldNotCreateTransform, "Failed creating libxslt Transform Context");
    ctxt->_private = this;

#if LIBXSLT_VERSION >= 10127  //the context variables maxTemplateDepth and maxTemplateVars were added in libxslt version 1.1.27
    if (xslConfig)
    {
        ctxt->maxTemplateDepth = xslConfig->getPropInt("xsltMaxDepth", 100000);
        ctxt->maxTemplateVars = xslConfig->getPropInt("xsltMaxVars", 1000000);
    }
    else
    {
        ctxt->maxTemplateDepth = 100000; //we use some very highly nested stylesheets
        ctxt->maxTemplateVars = 1000000;
    }
#endif

    HashIterator h(functions);
    ForEach (h)
    {
        IXslFunction *fn = functions.mapToValue(&h.query());
        CLibXslFunction *cfn = dynamic_cast<CLibXslFunction*>(fn);
        if (cfn && cfn->name.length())
            xsltRegisterExtFunction(ctxt, (const xmlChar *) cfn->name.get(), (const xmlChar *) cfn->uri.str(), globalLibXsltExtensionHandler);
    }
    xsltSetCtxtParseOptions(ctxt, hpccXsltDocumentParseOptions);
    MemoryBuffer mp;
    if (params.length())
        mp.append(sizeof(const char *) * params.length(), params.getArray()).append((unsigned __int64)0);

    xmlDocPtr res = xsltApplyStylesheetUser(xsldoc, xmldoc, (mp.length()) ? (const char**)mp.toByteArray() : NULL, NULL, NULL, ctxt);
    if (!res)
    {
        if (exceptions && exceptions->ordinality())
            throw exceptions.getClear();
        throw MakeStringException(XSLERR_TransformFailed, "Failed running xlst using libxslt.");
    }

    xsltTransformState stateAfterTransform = ctxt->state;

    try
    {
        xsltSaveResultToString(xmlbuff, &len, res, xsldoc);
        xsltFreeTransformContext(ctxt);
    }
    catch(...)
    {
        xsltFreeTransformContext(ctxt);
        xmlFreeDoc(res);
        xmlFree(xmlbuff);
        throw MakeStringException(XSLERR_TransformFailed, "Failed processing libxslt transform output");
    }
    xmlFreeDoc(res);

    if (exceptions && exceptions->ordinality())
    {
        if (stateAfterTransform != XSLT_STATE_OK)
        {
            throw exceptions.getClear();
        }
        else
        {
            StringBuffer strErrMsg;
            exceptions.get()->errorMessage(strErrMsg);
            messages.set(strErrMsg.str());
        }
    }

    return 0;
}

int CLibXslTransform::transform(StringBuffer &s)
{
    int len = 0;
    xmlChar *xmlbuff = NULL;
    transform(&xmlbuff, len);
    if (len && xmlbuff)
    {
        s.append(len, (char *) xmlbuff);
        xmlFree(xmlbuff);
    }
    return 0;
}

int CLibXslTransform::transform()
{
    int len = 0;
    xmlChar *xmlbuff = NULL;
    transform(&xmlbuff, len);
    if (len && xmlbuff)
    {
        target->process((const char *) xmlbuff, len);
        xmlFree(xmlbuff);
    }
    return 0;
}

int CLibXslTransform::transform(ISocket* targetSocket)
{
    int len = 0;
    xmlChar *xmlbuff = NULL;
    transform(&xmlbuff, len);
    if (len && xmlbuff)
    {
        targetSocket->write((const void *) xmlbuff, (size32_t) len);
        xmlFree(xmlbuff);
    }
    return 0;
}

int CLibXslTransform::setXmlSource(const char *pszFileName)
{
    xmlSrc.setown(new CLibXmlSource(pszFileName));
    return 0;
}

int CLibXslTransform::setXmlSource(const char *pszBuffer, unsigned int nSize)
{
    xmlSrc.setown(new CLibXmlSource(pszBuffer, nSize));
    return 0;
}

int CLibXslTransform::setXslSource(const char *pszBuffer, unsigned int nSize, const char *cacheId, const char *rootpath)
{
    xslSrc.setown(new CLibXsltSource(pszBuffer, nSize, cacheId));
    return 0;
}

int CLibXslTransform::loadXslFromEmbedded(const char *path, const char *cacheId)
{
    xslSrc.setown(new CLibXsltSource(includeHandler.get(), path, cacheId));
    return 0;
}

int CLibXslTransform::setXslNoCache(const char *pszBuffer, unsigned int nSize, const char *rootpath)
{
    xslSrc.setown(new CLibXsltSource(pszBuffer, nSize, NULL));
    return 0;
}

int CLibXslTransform::loadXslFromFile(const char *pszFileName, const char *altCacheId)
{
    xslSrc.setown(new CLibXsltSource(pszFileName));
    return 0;
}

int CLibXslTransform::setResultTarget(const char *pszFileName)
{
    target.setown(new CLibXsltResultTarget(pszFileName));
    return 0;
}

int CLibXslTransform::setResultTarget(char *pszBuffer, unsigned int nSize)
{
    target.setown(new CLibXsltResultTarget(pszBuffer, nSize));
    return 0;
}

int CLibXslTransform::closeResultTarget()
{
    return 0;
}

int CLibXslTransform::setParameter(const char *pszName, const char *pszExpression)
{
    if (pszName && *pszName)
        ensureParameters()->setProp(pszName, pszExpression);
    return 0;
}

int CLibXslTransform::setStringParameter(const char *pszName, const char* pszString)
{
    if (pszName && *pszName)
        ensureParameters()->setProp(pszName, StringBuffer("'").append(pszString).append("'").str());
    return 0;
}

class CLibXslProcessor : public CInterface, implements IXslProcessor
{
public:
    IMPLEMENT_IINTERFACE;

    CLibXslProcessor();
    ~CLibXslProcessor();
    virtual IXslTransform *createXslTransform(IPropertyTree *cfg);
    virtual int execute(IXslTransform *pITransform);

    virtual int setDefIncludeHandler(IIncludeHandler* handler){includeHandler.set(handler); return 0;}
    IIncludeHandler* queryDefIncludeHandler(){return includeHandler.get();}

    virtual void setCacheTimeout(int timeout);
    virtual int getCacheTimeout();
public:
    Owned<IIncludeHandler> includeHandler;
    int m_cachetimeout;
};

CLibXslProcessor::CLibXslProcessor()
{
    m_cachetimeout = XSLT_DEFAULT_CACHETIMEOUT;

    initializeXmlLibraries();
}

CLibXslProcessor::~CLibXslProcessor()
{
    // libxml2 documents xmlCleanupParser() as not thread-safe, says no library
    // calls may be made after it, and recommends calling it only right before
    // the whole process exits. It also notes there is generally no need for
    // manual cleanup when automatic runtime teardown is available. Do not try
    // to mirror initialization here by tearing down global XML/XSLT state
    // during static destruction.
}

static CLibXslProcessor xslProcessor;

extern IXslProcessor* getXslProcessor()
{
    return LINK(&xslProcessor);
}

IXslTransform *CLibXslProcessor::createXslTransform(IPropertyTree *cfg)
{
    return new CLibXslTransform(cfg);
}

int CLibXslProcessor::execute(IXslTransform *pITransform)
{
    return ((CLibXslTransform*)pITransform)->transform();
}

void CLibXslProcessor::setCacheTimeout(int timeout)
{
    m_cachetimeout = timeout;

    IXslCache* xslcache = getXslCache2();
    if (xslcache)
        xslcache->setCacheTimeout(timeout);
}

int CLibXslProcessor::getCacheTimeout()
{
    return m_cachetimeout;
}

CLibXsltSource *getXsltStylesheetSourceObject(xsltStylesheetPtr x)
{
    if (!x)
        return NULL;
    if (x->_private)
        return static_cast<CLibXsltSource *>(x->_private);
    if (!x->doc || !x->doc->_private)
        return NULL;
    x->_private = x->doc->_private; //initialy stored in root xstl xml document
    return static_cast<CLibXsltSource *>(x->_private);
}

xmlDocPtr libXsltIncludeHandler(const xmlChar * URI, xmlDictPtr dict, int options, IIncludeHandler* handler, xsltLoadType type)
{
    bool mbContainsPath=false;
    MemoryBuffer mb;
    StringBuffer decodedURI;
    appendDecodedURL(decodedURI, (const char *)URI);
    if (strchr(decodedURI.str(), '%')) //libxstl seems to double encode
    {
        StringBuffer s;
        appendDecodedURL(s, decodedURI.str());
        decodedURI.swapWith(s);
    }
    if (!handler->getInclude(decodedURI, mb, mbContainsPath))
        return originalLibXsltIncludeHandler(URI, dict, options, NULL, type);
    if (mbContainsPath)
        return originalLibXsltIncludeHandler((const xmlChar *)mb.append((char)0).toByteArray(), dict, options, NULL, type);
    if (mb.length())
        return readXmlMemoryWithOptions((const char *)mb.toByteArray(), mb.length(), (const char *)URI, dict, options);
    return NULL;
}

IIncludeHandler* getXsltStylesheetIncludeHandler(xsltStylesheetPtr x, IIncludeHandler *def)
{
    CLibXsltSource *src = getXsltStylesheetSourceObject(x);
    return (src && src->includeHandler) ? src->includeHandler.get() : def;
}

xmlDocPtr globalLibXsltIncludeHandler(const xmlChar * URI, xmlDictPtr dict, int options, void *ctx, xsltLoadType type)
{
    xsltStylesheetPtr x = (xsltStylesheetPtr)ctx;
    if (type == XSLT_LOAD_DOCUMENT)
        x = ((xsltTransformContextPtr)ctx)->style;

    IIncludeHandler* handler = getXsltStylesheetIncludeHandler(x, xslProcessor.queryDefIncludeHandler());
    if (handler)
        return libXsltIncludeHandler(URI, dict, options, handler, type);
    return originalLibXsltIncludeHandler(URI, dict, options, ctx, type);
}

void libxsltCustomMessageHandler(StringBuffer& out, const char* in, IXslTransform* trans)
{
    CLibXslTransform *ctrans = dynamic_cast<CLibXslTransform *>(trans);
    if (!ctrans)
        return;
    ctrans->messages.append(in).append(';');
    out.append(in);
}

void globalLibXsltExtensionHandler(xmlXPathParserContextPtr ctxt, int nargs)
{
    const xmlChar *uri = ctxt->context->functionURI;
    const xmlChar *name = ctxt->context->function;

    xsltTransformContextPtr tctxt = xsltXPathGetTransformContext(ctxt);
    if (!tctxt)
    {
        xsltGenericError(xsltGenericErrorContext, "failed to get the transformation context\n");
        return;
    }

    if (nargs!=1)
    {
        VStringBuffer msg("Extension %s:%s - called", uri, name);
        if (!nargs)
            msg.append(" without any arguments\n");
        else
            msg.append(" with too many arguments\n");
        xsltGenericError(xsltGenericErrorContext, "%s", msg.str());
        ctxt->error = XPATH_INVALID_ARITY;
        return;
    }

    xmlChar *text =xmlXPathPopString(ctxt);
    CLibXslTransform *trns = getXsltTransformObject(tctxt);
    if (!trns)
    {
        xsltGenericError(xsltGenericErrorContext, "{%s}%s: IXslTransform not found\n", uri, name);
        return;
    }

    StringBuffer out;
    IXslFunction *xslfn = trns->queryExternalFunction((const char *)uri, (const char *)name);
    if (!xslfn)
    {
        xsltGenericError(xsltGenericErrorContext, "{%s}%s: IXslFuntionTransform not found\n", uri, name);
        return;
    }

    CLibXslFunction *cfn = dynamic_cast<CLibXslFunction *>(xslfn);
    cfn->fn(out, (const char *)text, trns);

    valuePush(ctxt, xmlXPathNewCString(out.str()));
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"

// Security regression tests: verify that XML_PARSE_DTDLOAD is not active when parsing
// XSLT stylesheets or XML documents loaded at transform time, preventing XXE attacks.
//
// Each test crafts an XXE payload that would exfiltrate the contents of a canary file
// if external entity loading were enabled.
class LibXsltXXESecurityTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(LibXsltXXESecurityTests);
        CPPUNIT_TEST(testXXE_StylesheetFromBuffer);
        CPPUNIT_TEST(testXXE_StylesheetFromFile);
        CPPUNIT_TEST(testXXE_DocumentFunction);
    CPPUNIT_TEST_SUITE_END();

    static constexpr const char * canaryFilename  = "xmllib_xxe_canary.txt";
    static constexpr const char * canaryContent   = "XXECANARY";
    static constexpr const char * xsltFilename    = "xmllib_xxe_stylesheet.xslt";
    static constexpr const char * extDocFilename  = "xmllib_xxe_external.xml";

    static void removeIfExists(const char * filename)
    {
        Owned<IFile> f = createIFile(filename);
        if (f->exists())
            f->remove();
    }

public:
    void setUp() override
    {
        Owned<IFile> f = createIFile(canaryFilename);
        Owned<IFileIO> io = f->open(IFOcreate);
        CPPUNIT_ASSERT_MESSAGE("Failed to create canary file for XXE tests", io != nullptr);
        io->write(0, strlen(canaryContent), canaryContent);
    }

    void tearDown() override
    {
        removeIfExists(canaryFilename);
        removeIfExists(xsltFilename);
        removeIfExists(extDocFilename);
    }

    // Test 1: XSLT loaded from an in-memory buffer (covers the setXslSource / WuWebView path).
    // An attacker-controlled embedded XSLT with a DOCTYPE SYSTEM entity pointing at the canary.
    void testXXE_StylesheetFromBuffer()
    {
        // Build the absolute path to the canary so the SYSTEM URI is unambiguous
        StringBuffer canaryPath;
        makeAbsolutePath(canaryFilename, canaryPath);

        VStringBuffer xslt(
            "<?xml version=\"1.0\"?>"
            "<!DOCTYPE xsl:stylesheet ["
            "  <!ENTITY xxe SYSTEM \"file://%s\">"
            "]>"
            "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">"
            "  <xsl:output method=\"text\"/>"
            "  <xsl:template match=\"/\"><xsl:text>&xxe;</xsl:text></xsl:template>"
            "</xsl:stylesheet>",
            canaryPath.str());

        Owned<IXslProcessor> proc = getXslProcessor();
        Owned<IXslTransform> trans = proc->createXslTransform();
        trans->setXmlSource("<root/>", 7);
        trans->setXslSource(xslt.str(), xslt.length(), "xxe_test_buffer", ".");

        StringBuffer output;
        trans->transform(output);

        CPPUNIT_ASSERT_MESSAGE(
            "XXE vulnerability (stylesheet from buffer): canary content appeared in transform output - XML_PARSE_DTDLOAD must be removed",
            strstr(output.str(), canaryContent) == nullptr);
    }

    // Test 2: XSLT loaded from a file (covers the loadXslFromFile / parseXsltFile path).
    // Write the crafted XSLT to a temp file then load it via the file path.
    void testXXE_StylesheetFromFile()
    {
        StringBuffer canaryPath;
        makeAbsolutePath(canaryFilename, canaryPath);

        VStringBuffer xslt(
            "<?xml version=\"1.0\"?>"
            "<!DOCTYPE xsl:stylesheet ["
            "  <!ENTITY xxe SYSTEM \"file://%s\">"
            "]>"
            "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">"
            "  <xsl:output method=\"text\"/>"
            "  <xsl:template match=\"/\"><xsl:text>&xxe;</xsl:text></xsl:template>"
            "</xsl:stylesheet>",
            canaryPath.str());

        {
            Owned<IFile> xsltFile = createIFile(xsltFilename);
            Owned<IFileIO> io = xsltFile->open(IFOcreate);
            CPPUNIT_ASSERT_MESSAGE("Failed to create stylesheet file for XXE tests", io != nullptr);
            io->write(0, xslt.length(), xslt.str());
        }

        StringBuffer xsltPath;
        makeAbsolutePath(xsltFilename, xsltPath);

        Owned<IXslProcessor> proc = getXslProcessor();
        Owned<IXslTransform> trans = proc->createXslTransform();
        trans->setXmlSource("<root/>", 7);

        try
        {
            trans->loadXslFromFile(xsltPath.str());

            StringBuffer output;
            trans->transform(output);

            CPPUNIT_ASSERT_MESSAGE(
                "XXE vulnerability (stylesheet from file): canary content appeared in transform output - XML_PARSE_DTDLOAD must be removed",
                strstr(output.str(), canaryContent) == nullptr);
        }
        catch (IException *e)
        {
            // XSLERR_InvalidSource means the parse was aborted because the entity load was denied
            // — also a secure outcome, so treat as pass. Any other exception re-throws to fail the test.
            int code = e->errorCode();
            if (code != XSLERR_InvalidSource)  
                throw;  
            e->Release();  
        }
    }

    // Test 3: document() function call at transform time (covers xsltSetCtxtParseOptions path).
    // The XSLT itself is clean; the external XML loaded via document() contains the SYSTEM entity.
    void testXXE_DocumentFunction()
    {
        StringBuffer canaryPath;
        makeAbsolutePath(canaryFilename, canaryPath);

        // Write the external XML document with a SYSTEM entity referencing the canary
        VStringBuffer extDoc(
            "<?xml version=\"1.0\"?>"
            "<!DOCTYPE root ["
            "  <!ENTITY xxe SYSTEM \"file://%s\">"
            "]>"
            "<root>&xxe;</root>",
            canaryPath.str());

        StringBuffer extDocAbsPath;
        {
            Owned<IFile> docFile = createIFile(extDocFilename);
            Owned<IFileIO> io = docFile->open(IFOcreate);
            CPPUNIT_ASSERT_MESSAGE("Failed to create external XML document for XXE tests", io != nullptr);
            io->write(0, extDoc.length(), extDoc.str());
            makeAbsolutePath(extDocFilename, extDocAbsPath);
        }

        // Clean XSLT - uses document() to load the external XML and emit its text content
        VStringBuffer xslt(
            "<?xml version=\"1.0\"?>"
            "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">"
            "  <xsl:output method=\"text\"/>"
            "  <xsl:template match=\"/\">"
            "    <xsl:value-of select=\"document('file://%s')/root\"/>"
            "  </xsl:template>"
            "</xsl:stylesheet>",
            extDocAbsPath.str());

        Owned<IXslProcessor> proc = getXslProcessor();
        Owned<IXslTransform> trans = proc->createXslTransform();
        trans->setXmlSource("<input/>", 8);
        trans->setXslSource(xslt.str(), xslt.length(), "xxe_test_document", ".");

        StringBuffer output;
        trans->transform(output);

        CPPUNIT_ASSERT_MESSAGE(
            "XXE vulnerability (document() function): canary content appeared in transform output - XML_PARSE_DTDLOAD must be removed from xsltSetCtxtParseOptions",
            strstr(output.str(), canaryContent) == nullptr);
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(LibXsltXXESecurityTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(LibXsltXXESecurityTests, "LibXsltXXESecurity");

// Tests for XML_PARSE_NOENT | XML_PARSE_NO_XXE (libxml2 >= 2.13.0) as the preferred
// option combination. Verifies three properties:
//   1. Internal entity substitution still works.
//   2. External SYSTEM entity references are NOT fetched.
//   3. External DTD SYSTEM URIs are NOT fetched.
//
// These tests operate directly on xmlReadMemory so they are independent of the
// XSLT layer and confirm the libxml2 option semantics in isolation.

class LibXml2ParseOptionsTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(LibXml2ParseOptionsTests);
        CPPUNIT_TEST(initParser);
        CPPUNIT_TEST(testInternalEntitySubstituted);
        CPPUNIT_TEST(testExternalSystemEntityBlocked);
        CPPUNIT_TEST(testExternalDtdBlocked);
    CPPUNIT_TEST_SUITE_END();

    static constexpr const char * canaryFilename = "xmllib_opts_canary.txt";
    static constexpr const char * dtdFilename    = "xmllib_opts_test.dtd";
    static constexpr const char * canaryContent  = "PARSE_OPTIONS_CANARY";

    static void removeIfExists(const char * filename)
    {
        Owned<IFile> f = createIFile(filename);
        if (f->exists())
            f->remove();
    }

    static void writeFile(const char * filename, const char * content)
    {
        Owned<IFile> f = createIFile(filename);
        Owned<IFileIO> io = f->open(IFOcreate);
        CPPUNIT_ASSERT_MESSAGE("Failed to create file for XXE tests", io != nullptr);
        io->write(0, strlen(content), content);
    }

    // Returns the root element's full text content as a StringBuffer.
    // Returns empty if the document is NULL or the root has no text children.
    static void getRootText(xmlDocPtr doc, StringBuffer &result)
    {
        if (!doc)
            return;
        xmlNodePtr root = xmlDocGetRootElement(doc);
        if (!root)
            return;
        xmlChar * content = xmlNodeGetContent(root);
        if (content)
        {
            result.append((const char *)content);
            xmlFree(content);
        }
        return;
    }

public:
    void setUp() override
    {
        writeFile(canaryFilename, canaryContent);

        // DTD file defines an entity that would be visible if the external DTD were loaded.
        writeFile(dtdFilename, "<!ENTITY dtdent \"DTD_ENTITY_CONTENT\">");
    }

    void tearDown() override
    {
        removeIfExists(canaryFilename);
        removeIfExists(dtdFilename);
    }


    void initParser()
    {
        // Not an actual test, just for setup to initialize parser
        // in case run independently of or prior to XSLT tests.
        xmlInitParser();
    }

    // Test 1: Internal entity declared in internal subset is substituted.
    // This confirms XML_PARSE_NO_XXE does not disable internal entity substitution —
    // only external loading is suppressed.
    void testInternalEntitySubstituted()
    {
        constexpr const char * xml =
            "<?xml version=\"1.0\"?>"
            "<!DOCTYPE root ["
            "  <!ENTITY greeting \"hello\">"
            "]>"
            "<root>&greeting;</root>";

        xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "internal_ent.xml", NULL,
                                      XML_PARSE_NOENT | XML_PARSE_NO_XXE);
        std::unique_ptr<xmlDoc, decltype(&xmlFreeDoc)> docGuard(doc, xmlFreeDoc);

        CPPUNIT_ASSERT_MESSAGE("Document must parse successfully", doc != nullptr);
        StringBuffer text;
        getRootText(doc, text);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
            "Internal entity &greeting; must be substituted to 'hello'",
            std::string("hello"), std::string(text.str()));
    }

    // Test 2: External SYSTEM entity reference is blocked.
    // The entity declaration references the canary file via a SYSTEM URI.
    // If XML_PARSE_NO_XXE is effective, the canary content must not appear in the tree.
    void testExternalSystemEntityBlocked()
    {
        StringBuffer canaryPath;
        makeAbsolutePath(canaryFilename, canaryPath);

        VStringBuffer xml(
            "<?xml version=\"1.0\"?>"
            "<!DOCTYPE root ["
            "  <!ENTITY xxe SYSTEM \"file://%s\">"
            "]>"
            "<root>&xxe;</root>",
            canaryPath.str());

        xmlDocPtr doc = xmlReadMemory(xml.str(), xml.length(), "ext_ent.xml", NULL,
                                      XML_PARSE_NOENT | XML_PARSE_NO_XXE);
        std::unique_ptr<xmlDoc, decltype(&xmlFreeDoc)> docGuard(doc, xmlFreeDoc);

        // Document may be NULL (parse aborted) or present with unresolved entity.
        // Either outcome is acceptable; what must NOT happen is canary content in the tree.
        if (doc)
        {
            StringBuffer text;
            getRootText(doc, text);
            CPPUNIT_ASSERT_MESSAGE(
                "External SYSTEM entity must not be loaded: canary content must be absent",
                strstr(text.str(), canaryContent) == nullptr);
        }
        // doc == NULL means the parser rejected it entirely — also a passing outcome.
    }

    // Test 3: External DTD SYSTEM URI is not fetched.
    // The DTD file defines an entity. If the DTD were loaded and XML_PARSE_DTDATTR were
    // active, default attributes would appear; here we use a direct entity reference to
    // the DTD-defined entity and confirm it is not resolved.
    void testExternalDtdBlocked()
    {
        StringBuffer dtdPath;
        makeAbsolutePath(dtdFilename, dtdPath);

        VStringBuffer xml(
            "<?xml version=\"1.0\"?>"
            "<!DOCTYPE root SYSTEM \"file://%s\">"
            "<root>&dtdent;</root>",
            dtdPath.str());

        xmlDocPtr doc = xmlReadMemory(xml.str(), xml.length(), "ext_dtd.xml", NULL,
                                      XML_PARSE_NOENT | XML_PARSE_NO_XXE);
        std::unique_ptr<xmlDoc, decltype(&xmlFreeDoc)> docGuard(doc, xmlFreeDoc);

        // As with Test 2: NULL doc or present doc with unresolved entity both pass.
        if (doc)
        {
            StringBuffer text;
            getRootText(doc, text);
            CPPUNIT_ASSERT_MESSAGE(
                "External DTD must not be loaded: DTD entity content must be absent",
                strstr(text.str(), "DTD_ENTITY_CONTENT") == nullptr);
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(LibXml2ParseOptionsTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(LibXml2ParseOptionsTests, "LibXml2ParseOptions");

#endif // _USE_CPPUNIT
