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

#include "xalan_processor.ipp"
#include "xpathprocessor.hpp"
#include "jencrypt.hpp"
#include "jexcept.hpp"
#include "xalanc/XPath/XObjectFactory.hpp"
#include "xalanc/PlatformSupport/Writer.hpp"

/*
class StringBufferOStreamBuf : public std::basic_streambuf<char, std::char_traits<char> >
{
    StringBuffer & _inputbuffer;
    typedef std::char_traits<char> _Tr;
protected:
    virtual int overflow(int = _Tr::eof());
public:
    StringBufferOStreamBuf(StringBuffer &_str);
};

StringBufferOStreamBuf::StringBufferOStreamBuf(StringBuffer &_str) : _inputbuffer(_str)
{
}

int StringBufferOStreamBuf::overflow(int c)
{
     if(c == _Tr::eof()) return _Tr::not_eof(c);
     _inputbuffer.append((char) c);
     return c;
}

class StringBufferOStream : public std::basic_ostream<char, std::char_traits<char> >
{
     StringBufferOStreamBuf _streambuffer;
public:
     StringBufferOStream(StringBuffer &buf)
         : _streambuffer(buf),
          std::basic_ostream<char, std::char_traits<char> >(&_streambuffer)
     { clear(); }
};
*/

//-------------------------------------------------

XALAN_USING_XALAN(Writer)
XALAN_USING_XALAN(XalanOutputStream)
XALAN_USING_XALAN(XalanDOMChar)

class ISocketOutputStream : public IInterface
{
public:
    virtual Writer* getWriter() = 0;
};

//XALAN_CPP_NAMESPACE_BEGIN

class SocketOutputStream : public CInterface, public ISocketOutputStream, public Writer
{
private:
    ISocket *m_socket;

public:
    IMPLEMENT_IINTERFACE;

    SocketOutputStream(ISocket *s);
    ~SocketOutputStream() { close(); }

    virtual Writer* getWriter() { return this; }

    // Close the stream
    virtual void close();

    // Flush the stream
    virtual void flush();

    // Get the stream associated with the writer...
    virtual XalanOutputStream* getStream();

    // Get the stream associated with the writer...
    virtual const XalanOutputStream* getStream() const;

    // Writes a string
    virtual void write(const char* s, size_t theOffset = 0, size_t theLength = ~0u);

    // Writes a string
    virtual void write(const XalanDOMChar* s, XalanDOMString::size_type theOffset=0,
        XalanDOMString::size_type   theLength = XalanDOMString::npos);

    // Writes a character
    virtual void write(XalanDOMChar c);

    // Writes a string
    virtual void write(const XalanDOMString& s, XalanDOMString::size_type   theOffset = 0,
            XalanDOMString::size_type   theLength = XalanDOMString::npos);
};

SocketOutputStream::SocketOutputStream(ISocket* s)
{
    m_socket = s;
}

void SocketOutputStream::close()
{
    //fprintf(stderr,"close()\n");
    //m_socket->shutdown();
    //m_socket->close();
}

void SocketOutputStream::flush()
{
    //fprintf(stderr,"flush()\n");
}

XalanOutputStream* SocketOutputStream::getStream()
{
    fprintf(stderr, "Unsupported getStream()!");
    return NULL;
}

const XalanOutputStream* SocketOutputStream::getStream() const
{
    fprintf(stderr, "Unsupported getStream()!");
    return NULL;
}

void SocketOutputStream::write(const char* s, size_t offset, size_t length)
{
    //wprintf(stderr,TEXT("write(char*='%s',offset=%d,length=%d)\n"),s,offset,length);
    m_socket->write(s + offset, length);
}

//XalanDOMChar: utf-8 or wchar_t
void SocketOutputStream::write(const XalanDOMChar* s, XalanDOMString::size_type offset,
        XalanDOMString::size_type length)
{
    //printf(stderr,"write(DOMChar='%s',offset=%d,length=%d)\n",s,offset,length);
    m_socket->write((const char* )(s+offset), length * sizeof(XalanDOMChar));
}

void SocketOutputStream::write(XalanDOMChar c)
{
    //printf(stderr, "write(%c)", c);
    m_socket->write((const char*)&c, sizeof(XalanDOMChar));
}

void SocketOutputStream::write(const XalanDOMString& s, XalanDOMString::size_type offset,
        XalanDOMString::size_type length)
{
    //printf(stderr,"write(DOMString='%s',offset=%d,length=%s", s.c_str(), offset, length);
    m_socket->write((const char*)(s.c_str()+offset), length*sizeof(XalanDOMChar));
}

extern ISocketOutputStream* createSocketOutputStream(ISocket* s)
{
    return new SocketOutputStream(s);
}

//-------------------------------------------------

class XalanStringBufferOutputHandler
{
    StringBuffer & _inputbuffer;
public:
    XalanStringBufferOutputHandler(StringBuffer &_str) : _inputbuffer(_str)
    {
    }

    static unsigned long callback(const char *data, unsigned long len, void *ctx)
    {
        XalanStringBufferOutputHandler *self = (XalanStringBufferOutputHandler *) ctx;
        self->_inputbuffer.append(len, data);
        return len;
    }
};


//----------------------------------------------------------------------------
//                            CExternalFunction
//----------------------------------------------------------------------------

XObjectPtr CExternalFunction::execute( XPathExecutionContext&   executionContext,
                                    XalanNode*              /* context */,
                                    const XObjectPtr        arg1,
                                    const Locator*          /* locator */) const
{
    assert(arg1.null() == false);
    const XalanDOMString& arg = arg1->str();

    //convert XalanDOMString (implemented as unsigned short*) into StringBuffer
    StringBuffer sbInput;
    sbInput.ensureCapacity(arg.length()+1);

    size32_t len = arg.length();
    for (size32_t i=0; i < len; i++)
        sbInput.append( (char) arg[i]);

    StringBuffer sbOutput;

    try
    {
        (*m_userFunction)(sbOutput, sbInput.str(), m_pTransform);
    }
    catch (IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        e->Release();
    }
    catch (...)
    {
    }

    XalanDOMString xdsOutput( sbOutput.str() );
    return executionContext.getXObjectFactory().createString( xdsOutput );
}

//----------------------------------------------------------------------------
//                            CXslProcessor
//----------------------------------------------------------------------------

extern IXslProcessor* getXslProcessor()
{
    static CXslProcessor s_pXslProcessor;
    return LINK(&s_pXslProcessor);
}

// Should be called only once per-process
class XslProcessorInitializer
{
public:
    XslProcessorInitializer()
    {
        // Call the static initializer for Xerces.
        XMLPlatformUtils::Initialize();
        // Initialize Xalan.
        XalanTransformer::initialize();
    }
    ~XslProcessorInitializer()
    {
        // Terminate Xalan.
        XalanTransformer::terminate();
        // Call the static terminator for Xerces.
        XMLPlatformUtils::Terminate();
    }
};

CXslProcessor::CXslProcessor()
{
    static XslProcessorInitializer initializer;
    m_cachetimeout = XSLT_DEFAULT_CACHETIMEOUT;
}

// Should be called only once per-process
CXslProcessor::~CXslProcessor()
{
}

IXslTransform *CXslProcessor::createXslTransform(IPropertyTree *cfg)
{
    return new CXslTransform(inch.get());
}

int CXslProcessor::execute(IXslTransform *pITransform)
{
    return ((CXslTransform*)pITransform)->transform();
}

void CXslProcessor::setCacheTimeout(int timeout)
{
    m_cachetimeout = timeout;

    IXslCache* xslcache = getXslCache();
    if(xslcache)
        xslcache->setCacheTimeout(timeout);
}

int CXslProcessor::getCacheTimeout()
{
    return m_cachetimeout;
}

//----------------------------------------------------------------------------
//                            CXslTransform
//----------------------------------------------------------------------------
/*static*/ const char* CXslTransform::SEISINT_NAMESPACE = "http://seisint.com";

CXslTransform::CXslTransform(IIncludeHandler* handler) : m_XalanTransformer()
{
    m_ParsedSource = 0;
    m_resultTarget = 0;
    m_ostrstream = 0;
    m_sourceResolver = NULL;
    m_pUserData = NULL;

#ifdef _WIN32
    m_normalizeLinefeed = true; //default for Xalan
#endif

    if (handler)
        setIncludeHandler(handler);

    //set an external function to handle non-fatal XSL messages
    m_fnMessage.setown(createExternalFunction("message", message));
    setExternalFunction(SEISINT_NAMESPACE, m_fnMessage.get(), true);
}

CXslTransform::~CXslTransform()
{
    setExternalFunction(SEISINT_NAMESPACE, m_fnMessage.get(), false);

    if(m_sourceResolver != NULL)
        delete m_sourceResolver;

    if (m_xslsource)
        m_xslsource->clearIncludeHandler();

    if(m_ParsedSource)
    {
        m_XalanTransformer.destroyParsedSource(m_ParsedSource);
        m_ParsedSource = NULL;
    }

    closeResultTarget();
}

bool CXslTransform::checkSanity()
{
    return (m_xslsource && m_ParsedSource);
}

int CXslTransform::transform(StringBuffer &target)
{
    if(!m_ParsedSource)
        throw MakeStringException(1, "[XML source not set]");
    else if(!m_xslsource)
        throw MakeStringException(2, "[XSL stylesheet not set]");


    XalanCompiledStylesheet* pCompiledStylesheet = NULL;
    pCompiledStylesheet = m_xslsource->getStylesheet();

    if (!pCompiledStylesheet)
    {
        DBGLOG("[failed to compile XSLT stylesheet]");
        throw MakeStringException(2, "[failed to compile XSLT stylesheet]");
    }

    int rc=0;
    m_sMessages.clear();
    try
    {
        XalanStringBufferOutputHandler output(target);
        rc = m_XalanTransformer.transform(*m_ParsedSource, pCompiledStylesheet,
            (void*)&output, (XalanOutputHandlerType)output.callback, (XalanFlushHandlerType)0);
    }
    catch(...)
    {
        StringBuffer estr("[Exception running XSLT stylesheet]");
        estr.appendf("[%s]", m_XalanTransformer.getLastError());
        DBGLOG("%s", estr.str());
        throw MakeStringException(2, "%s", estr.str());
    }

    if (rc < 0)
    {
        StringBuffer estr;
        estr.appendf("[%s]", m_XalanTransformer.getLastError());
        DBGLOG("%s", estr.str());
        throw MakeStringException(2, "%s", estr.str());
    }

    return rc;
}

int CXslTransform::transform()
{
    if(!m_ParsedSource)
        throw MakeStringException(1, "[XML source not set for XSLT[");
    else if(!m_xslsource)
        throw MakeStringException(2, "[XSL stylesheet not set]");
    else if(!m_resultTarget)
        throw MakeStringException(2, "[XSLT target file/buffer not set]");

    XalanCompiledStylesheet* pCompiledStylesheet = NULL;
    pCompiledStylesheet = m_xslsource->getStylesheet();

    if (!pCompiledStylesheet)
    {
        DBGLOG("[failed to compile XSLT stylesheet]");
        throw MakeStringException(2, "[failed to compile XSLT stylesheet]");
    }

    int rc=0;
    m_sMessages.clear();
    try
    {
        rc = m_XalanTransformer.transform(*m_ParsedSource, pCompiledStylesheet, *m_resultTarget);
    }
    catch(...)
    {
        StringBuffer estr("[Exception running XSLT stylesheet]");
        estr.appendf("[%s]", m_XalanTransformer.getLastError());
        DBGLOG("%s", estr.str());
        throw MakeStringException(2, "%s", estr.str());
    }
    if (rc < 0)
    {
        StringBuffer estr;
        estr.appendf("[%s]", m_XalanTransformer.getLastError());
        DBGLOG("%s", estr.str());
        throw MakeStringException(2, "%s", estr.str());
    }

    return rc;
}

int CXslTransform::transform(ISocket* targetSocket)
{
    if(!m_ParsedSource)
        throw MakeStringException(1, "[XML source not set for XSLT[");
    else if(!m_xslsource)
        throw MakeStringException(2, "[XSL stylesheet not set]");

    XalanCompiledStylesheet* pCompiledStylesheet = NULL;
    pCompiledStylesheet = m_xslsource->getStylesheet();

    if (!pCompiledStylesheet)
    {
        DBGLOG("[failed to compile XSLT stylesheet]");
        throw MakeStringException(2, "[failed to compile XSLT stylesheet]");
    }

    int rc=0;
    m_sMessages.clear();
    try
    {
        m_resultTarget = new XSLTResultTarget();
        Owned<ISocketOutputStream> stream = createSocketOutputStream(targetSocket);
        m_resultTarget->setCharacterStream(stream->getWriter());
        rc = m_XalanTransformer.transform(*m_ParsedSource, pCompiledStylesheet, *m_resultTarget);
    }
    catch(...)
    {
        StringBuffer estr("[Exception running XSLT stylesheet]");
        estr.appendf("[%s]", m_XalanTransformer.getLastError());
        DBGLOG("%s", estr.str());
        throw MakeStringException(2, "%s", estr.str());
    }
    if (rc < 0)
    {
        StringBuffer estr;
        estr.appendf("[%s]", m_XalanTransformer.getLastError());
        DBGLOG("%s", estr.str());
        throw MakeStringException(2, "%s", estr.str());
    }

    return rc;
}


int CXslTransform::setXmlSource(const char *pszFileName)
{
    if(m_ParsedSource != NULL)
    {
        m_XalanTransformer.destroyParsedSource(m_ParsedSource);
        m_ParsedSource = NULL;
    }

    int theResult = 0;

    try
    {
        std::ifstream theXMLStream(pszFileName);
        const XSLTInputSource xmlinput(&theXMLStream);
        theResult = m_XalanTransformer.parseSource(xmlinput, (const XalanParsedSource*&)m_ParsedSource);
    }
    catch(...)
    {
        m_ParsedSource = NULL;
        StringBuffer estr("[Exception compiling xml]");
        estr.appendf("[%s]", m_XalanTransformer.getLastError());
        DBGLOG("%s", estr.str());
        throw MakeStringException(2, "%s", estr.str());
    }

    if (!m_ParsedSource)
    {
        StringBuffer estr("[failed to compile xml]");
        estr.appendf("[%s]", m_XalanTransformer.getLastError());
        DBGLOG("%s", estr.str());
        throw MakeStringException(2, "%s", estr.str());
    }

    return theResult;
}

int CXslTransform::setXmlSource(const char *pszBuffer, unsigned int nSize)
{
    if(m_ParsedSource != NULL)
    {
        m_XalanTransformer.destroyParsedSource(m_ParsedSource);
        m_ParsedSource = NULL;
    }

    int theResult = 0;
    try
    {
        //std::istringstream theXMLStream(pszBuffer, nSize);
        std::istringstream theXMLStream(pszBuffer);
        const XSLTInputSource xmlinput(&theXMLStream);
        theResult = m_XalanTransformer.parseSource(xmlinput, (const XalanParsedSource*&)m_ParsedSource);
    }
    catch(...)
    {
        m_ParsedSource = NULL;
        StringBuffer estr("[Exception compiling xml]");
        estr.appendf("[%s]", m_XalanTransformer.getLastError());
        DBGLOG("%s", estr.str());
        throw MakeStringException(2, "%s", estr.str());
    }

    if (!m_ParsedSource)
    {
        StringBuffer estr("[failed to compile xml]");
        estr.appendf("[%s]", m_XalanTransformer.getLastError());
        DBGLOG("%s", estr.str());
        throw MakeStringException(2, "%s", estr.str());
    }

    return theResult;
}

int CXslTransform::loadXslFromFile(const char *pszFileName, const char *cacheId)
{
    m_xslsource.setown(new CXslSource(pszFileName, m_sourceResolver?m_sourceResolver->getIncludeHandler():NULL, cacheId));

    return 0;
}

int CXslTransform::loadXslFromEmbedded(const char *path, const char *cacheId)
{
    m_xslsource.setown(new CXslSource(m_sourceResolver?m_sourceResolver->getIncludeHandler():NULL, cacheId, path));

    return 0;
}

int CXslTransform::setXslSource(const char *pszBuffer, unsigned int nSize, const char *cacheId, const char *rootpath)
{
    assertex(cacheId && *cacheId);
    m_xslsource.setown(new CXslSource(pszBuffer, nSize, m_sourceResolver?m_sourceResolver->getIncludeHandler():NULL, cacheId, rootpath));

    return 0;
}

int CXslTransform::setXslNoCache(const char *pszBuffer, unsigned int nSize, const char *rootpath)
{
    m_xslsource.setown(new CXslSource(pszBuffer, nSize, m_sourceResolver?m_sourceResolver->getIncludeHandler():NULL, NULL, rootpath));

    return 0;
}

int CXslTransform::setResultTarget(const char *pszFileName)
{
    closeResultTarget();

    try
    {
        m_resultTargetFile.clear().append(pszFileName);
        m_resultTarget = new XSLTResultTarget(XalanDOMString(pszFileName));
    }
    catch(...)
    {
        throw MakeStringException(1, "Exception opening file %s", pszFileName);
    }
    return 0;
}

int CXslTransform::setResultTarget(char *pszBuffer, unsigned int nSize)
{
    closeResultTarget();

    // Our output target that uses an ostrstream that will use the buffer

    try
    {
        //m_ostrstream = new std::ostringstream(pszBuffer, nSize);
        m_ostrstream = new std::ostringstream(pszBuffer);
        m_resultTarget = new XSLTResultTarget(m_ostrstream);
    }
    catch(...)
    {
        throw MakeStringException(1, "Exception in setting character buffer as XSLT result target.");
    }
    return 0;
}

int CXslTransform::closeResultTarget()
{
    if (m_resultTarget)
    {
        delete m_resultTarget;
        m_resultTarget = 0;
    }

    if(m_ostrstream)
    {
        delete m_ostrstream;
        m_ostrstream = 0;
    }

    return 0;
}

int CXslTransform::setParameter(const char *pszName, const char *pszExpression)
{
    m_XalanTransformer.setStylesheetParam(XalanDOMString(pszName), XalanDOMString(pszExpression));
    return 0;
}

int CXslTransform::setStringParameter(const char *pszName, const char *pszString)
{
    m_XalanTransformer.setStylesheetParam(XalanDOMString(pszName), XalanDOMString(StringBuffer("'").append(pszString).append("'").str()));
    return 0;
}

int CXslTransform::setIncludeHandler(IIncludeHandler* handler)
{
    if(handler == NULL)
    {
        throw MakeStringException(-1, "From CXslTransform::setIncludeHandler: a NULL handler is passed in");
    }

    if(m_sourceResolver == NULL)
    {
        m_sourceResolver = new MemSourceResolver();
    }

    m_sourceResolver->setHandler(handler);
    m_XalanTransformer.setEntityResolver(m_sourceResolver);

    if(m_xslsource.get() != NULL)
    {
        m_xslsource->setIncludeHandler(handler);
    }

    return 0;
}


int CXslTransform::setExternalFunction(const char* pszNameSpace, IXslFunction* pXslFunction, bool set)
{
    CXslFunction* pFn = (CXslFunction*) pXslFunction;

    if (pFn == NULL || pFn->get() == NULL)
        throw MakeStringException(-1, "Null pointer violation in CXslTransform::setExternalFunction.");

    XalanDOMString nameSpace(pszNameSpace);
    XalanDOMString functionName(pFn->getName());
    bool bAssigned = pFn->isAssigned();

    if (set && !bAssigned)
        m_XalanTransformer.installExternalFunction(nameSpace, functionName, *pFn->get());
    else
    {
        if (!set && bAssigned)
            m_XalanTransformer.uninstallExternalFunction(nameSpace, functionName);
        else
            throw MakeStringException(-1, "XSLT external function assignment error!");
    }

    pFn->setAssigned(set);
    return 0;
}

/*static*/
void CXslTransform::message(StringBuffer& out, const char* in, IXslTransform* pTransform)
{
    CXslTransform* pTrans = dynamic_cast<CXslTransform*>(pTransform);
    pTrans->m_sMessages.append(in).append('\n');
}

extern ICompiledXpath* compileXpath(const char * xpath)
{
    UNIMPLEMENTED;
}

extern IXpathContext* getXpathContext(const char * xmldoc, bool strictParameterDeclaration, bool removeDocNamespaces)
{
    UNIMPLEMENTED;
}
