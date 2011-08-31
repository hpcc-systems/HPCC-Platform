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

#ifndef XSLPROCESSOR_IPP_INCL
#define XSLPROCESSOR_IPP_INCL

#ifndef _WIN32
//undefine the stricmp and strnicmp macros defined by platform.h
//since xerces implements these functions
#undef stricmp
#undef strnicmp
#endif

#include <xercesc/util/PlatformUtils.hpp>
#include <xalanc/XalanTransformer/XalanTransformer.hpp>
#include <xalanc/XPath/Function.hpp>
#include <xercesc/util/XMLString.hpp>
#include <xercesc/sax/EntityResolver.hpp>
#include <xercesc/sax/InputSource.hpp>
#include <xercesc/parsers/SAXParser.hpp>
#include <xercesc/framework/MemBufInputSource.hpp>
#include <xercesc/sax/HandlerBase.hpp>

#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <stdlib.h>
#include "jliball.hpp"
#include "xslcache.hpp"

#ifdef _WIN32
#undef new
#undef delete
#define URLPREFIX ""
#else
#define URLPREFIX "file://"
#endif

#include "xslprocessor.hpp"

XALAN_USING_XERCES(EntityResolver)
XALAN_USING_XERCES(InputSource)
XALAN_USING_XERCES(XMLString)
XALAN_USING_XERCES(MemBufInputSource)
XALAN_USING_XERCES(XMLPlatformUtils)
XALAN_USING_XALAN(XalanDOMString)
XALAN_USING_XALAN(XalanParsedSource)
XALAN_USING_XALAN(XSLTResultTarget)
//XALAN_USING_XALAN(StaticStringToDOMString)
XALAN_USING_XALAN(XalanTransformer)
XALAN_USING_XALAN(XalanCompiledStylesheet)
XALAN_USING_XALAN(XSLTInputSource)
XALAN_USING_XALAN(Function)
XALAN_USING_XALAN(XObjectPtr)
XALAN_USING_XALAN(XPathExecutionContext)
XALAN_USING_XALAN(MemoryManagerType)
XALAN_USING_XALAN(XalanCopyConstruct)
XALAN_USING_XALAN(XalanNode)
XALAN_USING_XERCES(Locator)

class MemSourceResolver : public EntityResolver, public CInterface, implements IInterface
{
private:
    Owned<IIncludeHandler> m_includehandler;
    StringArray* m_includes;

public:
    IMPLEMENT_IINTERFACE;

    MemSourceResolver()
    {
        m_includes = NULL;
    }

    MemSourceResolver(StringArray* includes)
    {
        m_includes = includes;
    }

    int setHandler(IIncludeHandler* handler)
    {
        if(handler != NULL)
            m_includehandler.set(handler);
        return 0;
    }

    InputSource* resolveEntity (const XMLCh* const publicId, 
                                        const XMLCh* const systemId)
    {
        if(m_includehandler.get() == NULL)
            throw MakeStringException(-1, "MemSourceResolver::resolveEntity() - m_includehandler is NULL");

        InputSource* inputsrc=NULL;
        MemoryBuffer buf;
        char includename[1025];
        XMLString::transcode(systemId, includename, 1024);
        bool pathonly = false;
        if (m_includehandler->getInclude(includename, buf, pathonly))
        {
            if(pathonly)
            {
                if(m_includes)
                {
                    StringBuffer pbuf;
                    pbuf.append(buf.length(), buf.toByteArray());
                    m_includes->append(pbuf.str());
                }
                StringBuffer path;
                if(!isAbsolutePath((const char *)buf.toByteArray()))
                {
                    char baseurl[1025];
                    GetCurrentDirectory(1024, baseurl);
                    path.append(URLPREFIX).append(baseurl).append(PATHSEPSTR);
                }
                path.append(buf.length(), buf.toByteArray());
                inputsrc = new XSLTInputSource(path.str()); 
            }
            else
            {
                MemBufInputSource* memsrc = new MemBufInputSource((const XMLByte*)buf.detach(), buf.length(), (const XMLCh*)NULL, false);
                memsrc->setCopyBufToStream(false);
                inputsrc = memsrc;
            }
        }
        return inputsrc;
    }

    IIncludeHandler* getIncludeHandler()
    {
        return m_includehandler.get();
    }
};

class CXslSource : public CInterface, implements IXslBuffer
{
private:
    XalanTransformer m_XalanTransformer;
    IO_Type m_sourcetype;
    StringAttr m_filename;
    StringAttr m_rootpath;
    StringBuffer m_xsltext;
    XalanCompiledStylesheet* m_CompiledStylesheet;
    Owned<MemSourceResolver>  m_sourceResolver;
    
    StringArray m_includes;

public:
    IMPLEMENT_IINTERFACE;

    CXslSource(const char* fname, IIncludeHandler* handler) : m_XalanTransformer()
    {
        m_filename.set(fname);
        m_sourcetype = IO_TYPE_FILE;
        m_CompiledStylesheet = NULL;

        if(handler)
            setIncludeHandler(handler);
    }

    CXslSource(const char* buf, int len, IIncludeHandler* handler, const char *rootpath = NULL) : m_XalanTransformer()
    {
        m_xsltext.append(len, buf);
        m_sourcetype = IO_TYPE_BUFFER;
        m_CompiledStylesheet = NULL;

        if(handler)
            setIncludeHandler(handler);
        if (rootpath)
            m_rootpath.set(rootpath);
    }

    virtual ~CXslSource()
    {
        if(m_CompiledStylesheet)
        {
            m_XalanTransformer.destroyStylesheet(m_CompiledStylesheet);
            m_CompiledStylesheet = NULL;
        }
    }

    XalanCompiledStylesheet* getStylesheet(bool recompile = false)
    {
        if(!recompile && m_CompiledStylesheet != NULL)
            return m_CompiledStylesheet;

        int timeout = -1;
        Owned<IXslProcessor> processor = getXslProcessor();
        if(processor)
            timeout = processor->getCacheTimeout();

        if(timeout != 0)
        {
            IXslCache* xslcache = getXslCache();
            if(xslcache)
            {
                IXslBuffer* xslbuffer = xslcache->getCompiledXsl(this, recompile);
                if(xslbuffer)
                {
                    CXslSource* xslsource = dynamic_cast<CXslSource*>(xslbuffer);
                    return xslsource->m_CompiledStylesheet;
                }
            }
        }

        compile(recompile);
        return m_CompiledStylesheet;
    }

    virtual void compile(bool recompile)
    {
        if(recompile || m_CompiledStylesheet == NULL)
        {   
            if((m_sourcetype == IO_TYPE_FILE && m_filename.length() == 0) || (m_sourcetype == IO_TYPE_BUFFER && m_xsltext.length() == 0))
                throw MakeStringException(-1, "XslSource::getStylesheet() - xsl source not set");
            
            if(m_CompiledStylesheet != NULL)
            {
                m_XalanTransformer.destroyStylesheet(m_CompiledStylesheet);
                m_CompiledStylesheet = NULL;                
            }

            m_includes.popAll();
            
            try
            {
                if(m_sourcetype == IO_TYPE_FILE)
                {               
                    XSLTInputSource xslinput(m_filename.get());
                    m_XalanTransformer.compileStylesheet((const XSLTInputSource&)xslinput, (const XalanCompiledStylesheet*&)m_CompiledStylesheet);
                }
                else if(m_sourcetype == IO_TYPE_BUFFER)
                {
                    //std::istringstream theXSLStream(m_xsltext.str(), m_xsltext.length());
                    std::istringstream theXSLStream(m_xsltext.str());
                    XSLTInputSource xslinput(&theXSLStream);
                    
                    StringBuffer baseurl(URLPREFIX);
                    if (m_rootpath)
                        baseurl.append(m_rootpath.get());
                    else
                        appendCurrentDirectory(baseurl, true).append(PATHSEPCHAR);

                    xslinput.setSystemId(XalanDOMString(baseurl.str()).c_str());
                    m_XalanTransformer.compileStylesheet((const XSLTInputSource&)xslinput, (const XalanCompiledStylesheet*&)m_CompiledStylesheet);
                }
            }
            catch(...)
            {
                m_CompiledStylesheet = NULL;
                StringBuffer estr("[Exception compiling XSLT stylesheet]");
                estr.appendf("[%s]", m_XalanTransformer.getLastError());
                DBGLOG("%s", estr.str());
                throw MakeStringException(2, "%s", estr.str());
            }

            if (!m_CompiledStylesheet)
            {
                StringBuffer estr("[failed to compile XSLT stylesheet]");
                estr.appendf("[%s]", m_XalanTransformer.getLastError());
                DBGLOG("%s", estr.str());
                throw MakeStringException(2, "%s", estr.str());
            }

        }
    }


    bool isCompiled() const 
    { 
        return m_CompiledStylesheet != NULL; 
    }

    virtual IO_Type getType()
    {
        return m_sourcetype;
    }

    virtual const char* getFileName()
    {
        return m_filename.get();
    }

    virtual char* getBuf()
    {
        return (char*)m_xsltext.str();
    }

    virtual int getLen()
    {
        return m_xsltext.length();
    }

    int setIncludeHandler(IIncludeHandler* handler)
    {
        if(handler == NULL)
        {
            throw MakeStringException(-1, "From CXslTransform::setIncludeHandler: a NULL handler is passed in");
        }

        if(m_sourceResolver.get() == NULL)
        {
            m_sourceResolver.setown(new MemSourceResolver(&m_includes));
        }
        
        m_sourceResolver->setHandler(handler);
        m_XalanTransformer.setEntityResolver(m_sourceResolver.get());

        return 0;
    }

    virtual StringArray& getIncludes()
    {
        return m_includes;
    }
};

typedef void TextFunctionType(StringBuffer& out, const char* pszIn, IXslTransform* pTransform);


class CExternalFunction : public Function
{
public:    
    CExternalFunction(TextFunctionType* fn, IXslTransform* pTransform)
    {
        m_userFunction = fn;
          m_pTransform = pTransform;
    }

    CExternalFunction(const CExternalFunction& other)
    {
        m_userFunction = other.m_userFunction;
          m_pTransform   = other.m_pTransform;
    }

    virtual ~CExternalFunction()
    {
    }

    // These methods are inherited from Function ...
    /**
     * Execute an XPath function object.  The function must return a valid
     * object.
     *
     * @param executionContext executing context
     * @param context          current context node
     * @param args             vector of pointers to XObject arguments
     * @param locator          Locator for the XPath expression that contains the function call
     * @return                 pointer to the result XObject
     */
    virtual XObjectPtr
    execute(
            XPathExecutionContext&  executionContext,
            XalanNode*              context,
            const XObjectPtr        arg1,
            const Locator*          locator) const;

#if defined(XALAN_NO_COVARIANT_RETURN_TYPE)
    virtual Function*
#else
    virtual CExternalFunction*
#endif
    clone(MemoryManagerType &theManager) const
    {
        return XalanCopyConstruct(theManager, *this);
    }

protected:
    /**
     * Create a copy of the function object.
     *
     * @return string function name
     */
    virtual const XalanDOMString& getError(XalanDOMString &ret) const
    {
        ret.assign("The CExternalFunction() function takes one argument!");
        return ret;
    }

private:
    TextFunctionType*   m_userFunction;
     IXslTransform*     m_pTransform;

    // Not implemented...
    Function& operator=(const Function&);

    bool operator==(const Function&) const;
};


class CXslFunction : public CInterface, implements IXslFunction
{
private:
    Function*   m_pFunction;
    bool        m_bAssigned;
    StringAttr  m_sName;
    IXslTransform* m_pTransform;

public:
    IMPLEMENT_IINTERFACE;

    //Xalan is implemented to take ownership of any assigned external functions 
    //and destroys them on exit.
    //
    //This constructor takes ownership of the pointer passed in as param
    //and destroys the object unless it is assigned to an XSL transformation, in 
    //which case, the XSL transformation object destroys this function object.
    //
    CXslFunction(const char* name, Function* pFunction)
    {
        m_pFunction = pFunction;
        m_sName.set(name);
        m_bAssigned = false;
        m_pTransform = NULL;
    }

    CXslFunction(const char* name, TextFunctionType* fn, IXslTransform* pTransform)
        : m_pTransform(pTransform)
    {
        m_pFunction = new CExternalFunction(fn, pTransform);
        m_sName.set(name);
        m_bAssigned = false;
    }

    virtual ~CXslFunction()
    {
        if (!m_bAssigned)
            delete m_pFunction;
    }

    virtual Function* get() const 
    { 
        return m_pFunction; 
    }

    virtual const char* getName() const
    {
        return m_sName.get();
    }

    virtual bool isAssigned ()
    {
        return m_bAssigned;
    }

    virtual void setAssigned(bool bAssigned) 
    { 
        m_bAssigned = bAssigned;
    }
};


//The transform component is used to setup, and maintain any state associated with a transform
//
class XMLLIB_API CXslTransform : public CInterface, implements IXslTransform
{
private:
    XalanTransformer    m_XalanTransformer;
    Owned<CXslSource>   m_xslsource;
    XalanParsedSource*  m_ParsedSource;
    XSLTResultTarget*   m_resultTarget;
    StringBuffer        m_resultTargetFile;
    std::ostringstream* m_ostrstream;
    MemSourceResolver*  m_sourceResolver;
    bool                m_normalizeLinefeed;
    Owned<IXslFunction> m_fnMessage;
    StringBuffer        m_sMessages;
    void*               m_pUserData;
    static const char* SEISINT_NAMESPACE;

    static void message(StringBuffer& out, const char* in, IXslTransform*);

public:
    IMPLEMENT_IINTERFACE;

    CXslTransform(IIncludeHandler* handler);
    ~CXslTransform();
    bool checkSanity();
    virtual int transform();
    virtual int transform(StringBuffer &target);
    virtual int transform(ISocket* targetSocket);

    virtual int setXmlSource(const char *pszFileName);
    virtual int setXmlSource(const char *pszBuffer, unsigned int nSize);
    virtual int setXslSource(const char *pszFileName);
    virtual int setXslSource(const char *pszBuffer, unsigned int nSize, const char *rootpath);
    virtual int setResultTarget(char *pszBuffer, unsigned int nSize);
    virtual int setResultTarget(const char *pszFileName);
    virtual int closeResultTarget();
    virtual int setParameter(const char *pszName, const char *pszExpression);
    virtual void copyParameters(IProperties *params)
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
    virtual int setStringParameter(const char *pszName, const char* pszString);

    virtual int setIncludeHandler(IIncludeHandler* handler);

    virtual IXslFunction* createExternalFunction( const char* pszNameSpace, TextFunctionType* fn)
    {
        return new CXslFunction(pszNameSpace, fn, this);
    }
    virtual int setExternalFunction( const char* pszNameSpace, IXslFunction*, bool set);

    virtual const char* getLastError() const
    {
        return m_XalanTransformer.getLastError();
    }
    virtual const char* getMessages() const
    {
        return m_sMessages.str();
    }
    virtual void setUserData(void* userData) 
    { 
        m_pUserData = userData; 
    }
    virtual void* getUserData() const
    { 
        return m_pUserData; 
    }
};

class XMLLIB_API CXslProcessor : public CInterface, implements IXslProcessor
{
private:
    Owned<IIncludeHandler> inch;
    int m_cachetimeout;

public:
    IMPLEMENT_IINTERFACE;

    CXslProcessor();
    ~CXslProcessor();
    virtual IXslTransform *createXslTransform();
    virtual int execute(IXslTransform *pITransform);

    virtual int setDefIncludeHandler(IIncludeHandler* handler){inch.set(handler); return 0;}
    IIncludeHandler* queryDefIncludeHandler(){return inch.get();}

    virtual void setCacheTimeout(int timeout);
    virtual int getCacheTimeout();
};


#endif
