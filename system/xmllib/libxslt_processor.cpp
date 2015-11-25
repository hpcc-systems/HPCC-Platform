#include "jliball.hpp"
#include <string.h>

#include <libxml/xmlmemory.h>
#include <libxml/parserInternals.h>
#include <libxml/debugXML.h>
#include <libxml/HTMLtree.h>
#include <libxml/xmlIO.h>
#include <libxml/xinclude.h>
#include <libxml/catalog.h>
#include <libxml/xpathInternals.h>
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

extern int xmlLoadExtDtdDefaultValue;
xsltDocLoaderFunc originalLibXsltIncludeHandler = NULL;

xmlDocPtr libXsltIncludeHandler(const xmlChar * URI, xmlDictPtr dict, int options, IIncludeHandler* handler, xsltLoadType type);
xmlDocPtr globalLibXsltIncludeHandler(const xmlChar * URI, xmlDictPtr dict, int options, void *ctx, xsltLoadType type);

void libxsltCustomMessageHandler(StringBuffer& out, const char* in, IXslTransform* transform);

class CLibXsltSource : public CInterface, implements IXslBuffer
{
public:
    IMPLEMENT_IINTERFACE;

    CLibXsltSource(const char* fname) : filename(fname), cacheId(fname), compiledXslt(NULL)
    {
        srcType = IO_TYPE_FILE;
    }

    CLibXsltSource(IIncludeHandler *handler, const char* rootpath, const char *_cacheId) : cacheId(_cacheId), compiledXslt(NULL), filename(rootpath)
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

    xmlDocPtr doc = xsltDocDefaultLoader((xmlChar *)filename.get(), NULL, XSLT_PARSE_OPTIONS, NULL, XSLT_LOAD_START);
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
                xmlDocPtr xsldoc = xmlReadMemory(text.get(), text.length(), filename.get(), NULL, 0);
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
                parsedXml = xmlParseFile(filename.get());
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
        : name(_name), fn(_fn), parent(trans), assigned(false) {}

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

    CLibXslTransform();
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

CLibXslTransform::CLibXslTransform()
{
    userData = NULL;
    msgfn.setown(createExternalFunction("message", libxsltCustomMessageHandler));
    setExternalFunction(SEISINT_XSLTEXT_NAMESPACE, msgfn, true);
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

    HashIterator h(functions);
    ForEach (h)
    {
        IXslFunction *fn = functions.mapToValue(&h.query());
        CLibXslFunction *cfn = dynamic_cast<CLibXslFunction*>(fn);
        if (cfn && cfn->name.length())
            xsltRegisterExtFunction(ctxt, (const xmlChar *) cfn->name.get(), (const xmlChar *) cfn->uri.str(), globalLibXsltExtensionHandler);
    }
    xsltSetCtxtParseOptions(ctxt, XSLT_PARSE_OPTIONS);
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
        xsltFreeTransformContext(ctxt);
        xsltSaveResultToString(xmlbuff, &len, res, xsldoc);
    }
    catch(...)
    {
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
    virtual IXslTransform *createXslTransform();
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

    xmlInitMemory();
    xmlInitParser();

    xmlSubstituteEntitiesDefault(1);
    xmlThrDefSaveNoEmptyTags(1);
    xmlLoadExtDtdDefaultValue = 1;
    xsltMaxDepth = 20000;
    xsltSetLoaderFunc(NULL);
    originalLibXsltIncludeHandler = xsltDocDefaultLoader;
    xsltSetLoaderFunc(globalLibXsltIncludeHandler);
    exsltRegisterAll();
    xsltSetGenericErrorFunc(NULL, libxsltErrorMsgHandler);
}

CLibXslProcessor::~CLibXslProcessor()
{
    xsltCleanupGlobals();
    xmlCleanupParser();
    xmlCleanupMemory();
}

static CLibXslProcessor xslProcessor;

extern IXslProcessor* getXslProcessor()
{
    return LINK(&xslProcessor);
}

IXslTransform *CLibXslProcessor::createXslTransform()
{
    return new CLibXslTransform();
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
        return xmlReadMemory(mb.toByteArray(), mb.length(), (const char *)URI, NULL, 0);
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
