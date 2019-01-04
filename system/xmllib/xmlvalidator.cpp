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

#include "xmlvalidator.hpp"
#include "jstring.hpp"
#include "jdebug.hpp"
#include "jptree.hpp"
#include "jexcept.hpp"
#include "jlog.hpp"

// platform.h's definition of new collides with xerces
#undef new 

// xalan has its own definitions and implementations:
#undef stricmp
#undef strnicmp

#include <xercesc/util/PlatformUtils.hpp>
#include <xercesc/parsers/AbstractDOMParser.hpp>
#include <xercesc/dom/DOM.hpp>
#include <xercesc/validators/common/Grammar.hpp>
#include <xercesc/util/BinFileInputStream.hpp>
#include <xercesc/sax/SAXException.hpp>
#include <xercesc/sax/ErrorHandler.hpp>
#include <xercesc/sax/SAXParseException.hpp>
#include <xercesc/sax/EntityResolver.hpp>
#include <xercesc/parsers/XercesDOMParser.hpp>
#include <xercesc/framework/MemBufInputSource.hpp>

XERCES_CPP_NAMESPACE_USE

//=============================================================================================

class  ParseErrorHandler: public ErrorHandler
{
    Owned<IMultiException> m_errors;

public:
    ParseErrorHandler() { }

    virtual void warning(const SAXParseException& e);
    virtual void error(const SAXParseException& e);
    virtual void fatalError(const SAXParseException& e);
    virtual void resetErrors();

    bool hasError() { return m_errors.get() && m_errors->ordinality()>0; }
    // the caller needs to release the return
    IMultiException* queryExceptions() { return m_errors.get(); }
    void appendException(IException* e);

protected:
    void handleSAXParserException(const SAXParseException& e, const char* errorType);
};

void ParseErrorHandler::resetErrors()
{
    m_errors.clear();
}

void ParseErrorHandler::appendException(IException* e)
{
    if (!m_errors.get())
        m_errors.setown(MakeMultiException("DOMParser"));
    m_errors->append(*e);
}

void ParseErrorHandler::handleSAXParserException(const SAXParseException& e, const char* errorType)
{
    char systemId[256], publicId[256];
    
    XMLString::transcode(e.getSystemId(),systemId,255);
    XMLString::transcode(e.getPublicId(),publicId,255);
    char* message = XMLString::transcode(e.getMessage());
   
    StringBuffer msg, line,col;
    line.appendlong(e.getLineNumber());
    col.appendlong(e.getColumnNumber());
    msg.appendf("%s at \"%s\", line %s, char %s:  %s", errorType,
        (publicId&&publicId[0]) ? publicId : systemId, line.str(), col.str(), message);
    appendException(MakeStringException(-1,"%s", msg.str()));

    XMLString::release(&message);
}

void ParseErrorHandler::error(const SAXParseException& e)
{
    handleSAXParserException(e, "Error");   
}

void ParseErrorHandler::fatalError(const SAXParseException& e)
{
    handleSAXParserException(e, "Fatal error"); 
}

void ParseErrorHandler::warning(const SAXParseException& e)
{
    handleSAXParserException(e, "Warning"); 
}

//=============================================================================================
// class InMemSourceResolver

class InMemSourceResolver : public EntityResolver, public CInterface, implements IInterface
{
private:
    StringAttr m_xmlBuf, m_xsdBuf;
    StringAttr m_xmlEntityName;
    MemBufInputSource* m_xsdSrc;
    MemBufInputSource* m_xmlSrc;

public:
    IMPLEMENT_IINTERFACE;

    InMemSourceResolver(const char* xmlBuf, const char* xmlEntityName, const char* xsdBuf)
        : m_xmlBuf(xmlBuf),m_xmlEntityName(xmlEntityName),m_xsdBuf(xsdBuf), m_xsdSrc(NULL), m_xmlSrc(NULL)
    {
    }

    ~InMemSourceResolver()
    {
        //if (m_xsdSrc)
        //  delete m_xsdSrc;
        if (m_xmlSrc)
            delete m_xmlSrc;
    }

    InputSource* resolveEntity (const XMLCh* const publicId, const XMLCh* const systemId)
    {
        char sys[256],pub[256];
        XMLString::transcode(systemId,sys,255);
        XMLString::transcode(publicId,pub,255);
        //DBGLOG("resolveEntity(%s, %s)\n",sys,pub);

        if (strcmp(sys,"_xsd-buffer_")==0)              
        {
            if (m_xsdSrc)
                delete m_xsdSrc;
            m_xsdSrc = new MemBufInputSource((const XMLByte*)m_xsdBuf.get(), m_xsdBuf.length(), publicId, false);
            m_xsdSrc->setCopyBufToStream(false);
            return m_xsdSrc;
        }
        else if (strcmp(sys,m_xmlEntityName)==0) //|| strcmp(sys,"_xml-buffer_")==0)                
        {
            if (m_xmlSrc)
                delete m_xmlSrc;
            m_xmlSrc = new MemBufInputSource((const XMLByte*)m_xmlBuf.get(), m_xmlBuf.length(), publicId, false);
            m_xmlSrc->setCopyBufToStream(false);
            return m_xmlSrc;
        }

        return NULL;
    }
};

//=============================================================================================
// class CDomXmlValidator

class CDomXmlValidator : public CInterface, public IXmlValidator
{
private:    
    StringAttr       m_xmlFileName;
    StringAttr       m_xsdFileName;
    StringAttr       m_targetNamespace;
    StringBuffer     m_xmlBuf;
    StringBuffer     m_xsdBuf;

public:
    IMPLEMENT_IINTERFACE;

    CDomXmlValidator();
    ~CDomXmlValidator();

    virtual int setXmlSource(const char *pszFileName);
    virtual int setXmlSource(const char *pszBuffer, unsigned int nSize);
 
    virtual int setSchemaSource(const char *pszFileName);
    virtual int setSchemaSource(const char *pszBuffer, unsigned int nSize);

    virtual int setDTDSource(const char *pszFileName) { throw MakeStringException(-1,"Unsupported"); }
    virtual int setDTDSource(const char *pszBuffer, unsigned int nSize) { throw MakeStringException(-1,"Unsupported"); }

    virtual void setTargetNamespace(const char* ns);

    virtual void validate();
};


CDomXmlValidator::CDomXmlValidator()
{
}

CDomXmlValidator::~CDomXmlValidator()
{
}

void CDomXmlValidator::setTargetNamespace(const char* ns)
{
    m_targetNamespace.set(ns);
}

static void loadFile(StringBuffer& s, const char* file)
{
    s.loadFile(file);
}

int CDomXmlValidator::setXmlSource(const char* pszFile)
{
    if (pszFile)
    {
        try {
            loadFile(m_xmlBuf, pszFile);
        } catch (IException* e) {
            StringBuffer msg;
            WARNLOG("Exception loading xml source file(%s): %s\n", pszFile, e->errorMessage(msg).str());
            e->Release();
            return 0; 
        }
        m_xmlFileName.set(pszFile);
        return 1;
    }

    return 0;
}

int CDomXmlValidator::setXmlSource(const char *pszBuffer, unsigned int nSize) 
{ 
    m_xmlFileName.set("_xml-buffer_");
    m_xmlBuf.clear().append(nSize,pszBuffer); 
    return 0; 
}

int CDomXmlValidator::setSchemaSource(const char* pszFile)
{
    if (pszFile)
    {
        try {
            loadFile(m_xsdBuf, pszFile);
        } catch (IException* e) {
            StringBuffer msg;
            WARNLOG("Exception loading xml source file(%s): %s\n", pszFile,e->errorMessage(msg).str());
            e->Release();
            return 0; 
        }

        m_xsdFileName.set(pszFile);
        return 1;
    }
    return 0;
}

int CDomXmlValidator::setSchemaSource(const char *pszBuffer, unsigned int nSize) 
{ 
    m_xsdFileName.set("_xsd-buffer_");
    m_xsdBuf.clear().append(nSize,pszBuffer); 
    return 0; 
}

void CDomXmlValidator::validate()
{
    // use entity resolver so we can use file/buffer for xml and xsd
    MTimeSection timing(NULL, "CDomXmlValidator::validate()");

    // error handling
    ParseErrorHandler eh;
    XercesDOMParser* parser = new XercesDOMParser();
    parser->setErrorHandler(&eh);
    parser->setExitOnFirstFatalError(false);

    parser->setDoNamespaces(true);
    parser->setValidationScheme(XercesDOMParser::Val_Always);
    parser->setDoSchema(true);

    if (m_targetNamespace.get())
    {
        StringBuffer schemaLocation(m_targetNamespace.get());
        schemaLocation.append(" ").append(m_xsdFileName);
        parser->setExternalSchemaLocation(schemaLocation);

        //printf("XML Buffer: %s\n", m_xmlBuf.str());

        // Insert namesapce if necessary: 
        try 
        {
            Owned<IPTree> xml = createPTreeFromXMLString(m_xmlBuf);

            bool hasXmlns = false;
            Owned<IAttributeIterator> attrs = xml->getAttributes();
            for (attrs->first(); attrs->isValid(); attrs->next())
            {
                //DBGLOG("%s = %s\n", attrs->queryName(), attrs->queryValue());
                if (strcmp(attrs->queryName(),"@xmlns")==0)
                {
                    DBGLOG("Default namespace in xml: %s\n", attrs->queryValue());
                    hasXmlns = true;
                    break;
                }
            }
            if (!hasXmlns)
            {
                xml->addProp("@xmlns",m_targetNamespace.get());
                m_xmlBuf.clear();
                toXML(xml,m_xmlBuf);
                // printf("XML Buffer: %s\n", m_xmlBuf.str());
            }
        }
        catch (IException* e) {
            StringBuffer msg;
            DBGLOG("Exception: %s\n", e->errorMessage(msg).str());
            e->Release();
        }
        catch (...) {
            DBGLOG("Unknown exception caught parsing XML\n");
        }
    }
    else
    {
        parser->setExternalNoNamespaceSchemaLocation(m_xsdFileName);
    }

    EntityResolver* er = new InMemSourceResolver(m_xmlBuf, m_xmlFileName, m_xsdBuf);
    parser->setEntityResolver(er);

    try 
    {
        XMLCh xml[256];
        XMLString::transcode(m_xmlFileName,xml,255);

        parser->parse(*er->resolveEntity(xml,xml));     
    } 
    catch (IException* ie)
    {
        eh.appendException(ie);
    }
    catch (const XMLException& e )
    {
        char* message = XMLString::transcode(e.getMessage());

        StringBuffer msg;
        msg.appendf("Exception in parsing \"%s\": %s", m_xmlFileName.get(), message);
        eh.appendException(MakeStringException(-2, "%s", msg.str()));

        XMLString::release(&message);
    }
    catch (...)
    {
        StringBuffer msg;
        msg.appendf("Unknown exception in parsing \"%s\"", m_xmlFileName.get());
        eh.appendException(MakeStringException(-2, "%s", msg.str()));
    }

    delete er;

    parser->resetDocumentPool();
    delete parser;

    if (eh.hasError())
        throw LINK(eh.queryExceptions());
}
    
//=============================================================================================
// DOM Parser 

class CXmlDomParser : public CInterface, public IXmlDomParser
{
public: 
    IMPLEMENT_IINTERFACE;

    CXmlDomParser();
    ~CXmlDomParser();
    
    IXmlValidator* createXmlValidator();
};

// Should be called only once per-process
CXmlDomParser::CXmlDomParser()
{
    XMLPlatformUtils::Initialize();
}

// Should be called only once per-process
CXmlDomParser::~CXmlDomParser()
{
    XMLPlatformUtils::Terminate();
}

IXmlValidator* CXmlDomParser::createXmlValidator()
{
    return new CDomXmlValidator();
}

//=============================================================================================
// factory  

XMLLIB_API IXmlDomParser* getXmlDomParser()
{
    return new CXmlDomParser();
}

// END
//=============================================================================================
