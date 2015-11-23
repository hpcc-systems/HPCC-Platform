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
#include <libxml/xmlschemas.h>

#include "xmlvalidator.hpp"
#include "xmlerror.hpp"


class CLibXmlValidator : public CInterface, public IXmlValidator
{
public:    
    StringAttr      xmlFile;
    StringBuffer    xml;
    StringAttr      xsdFile;
    StringBuffer    xsd;
    StringAttr      targetNamespace;
    Owned<IMultiException> exceptions;

public:
    IMPLEMENT_IINTERFACE;

    CLibXmlValidator();
    ~CLibXmlValidator();

    virtual int setXmlSource(const char *pszFileName);
    virtual int setXmlSource(const char *pszBuffer, unsigned int nSize);
 
    virtual int setSchemaSource(const char *pszFileName);
    virtual int setSchemaSource(const char *pszBuffer, unsigned int nSize);

    virtual int setDTDSource(const char *pszFileName) { throw MakeStringException(-1,"Unsupported"); }
    virtual int setDTDSource(const char *pszBuffer, unsigned int nSize) { throw MakeStringException(-1,"Unsupported"); }

    virtual void setTargetNamespace(const char* ns);

    IMultiException *ensureExceptions()
    {
        if (!exceptions)
            exceptions.setown(MakeMultiException());
        return exceptions.get();
    }

    virtual void validate();
};


CLibXmlValidator::CLibXmlValidator()
{
}

CLibXmlValidator::~CLibXmlValidator()
{
}

void CLibXmlValidator::setTargetNamespace(const char* ns)
{
    targetNamespace.set(ns);
}

int CLibXmlValidator::setXmlSource(const char* filename)
{
    if (!filename)
        return 0;

    try 
    {
        xml.loadFile(filename);
    }
    catch (IException* e)
    {
        DBGLOG(e);
        e->Release();
        return 0;
    }

    xmlFile.set(filename);
    return 1;
}

int CLibXmlValidator::setXmlSource(const char *s, unsigned int len) 
{ 
    xmlFile.set("_xml-buffer_");
    xml.clear().append(len, s);
    return 0; 
}

int CLibXmlValidator::setSchemaSource(const char* filename)
{
    if (!filename)
        return 0;

    try
    {
        xsd.loadFile(filename);
    }
    catch (IException* e)
    {
        DBGLOG(e);
        e->Release();
        return 0; 
    }

    xsdFile.set(filename);
    return 1;
}

int CLibXmlValidator::setSchemaSource(const char *s, unsigned int len) 
{
    xsdFile.set("_xsd-buffer_");
    xsd.clear().append(len, s); 
    return 0; 
}

static xmlSAXHandler emptySAXHandlerStruct = {
    NULL, /* internalSubset */
    NULL, /* isStandalone */
    NULL, /* hasInternalSubset */
    NULL, /* hasExternalSubset */
    NULL, /* resolveEntity */
    NULL, /* getEntity */
    NULL, /* entityDecl */
    NULL, /* notationDecl */
    NULL, /* attributeDecl */
    NULL, /* elementDecl */
    NULL, /* unparsedEntityDecl */
    NULL, /* setDocumentLocator */
    NULL, /* startDocument */
    NULL, /* endDocument */
    NULL, /* startElement */
    NULL, /* endElement */
    NULL, /* reference */
    NULL, /* characters */
    NULL, /* ignorableWhitespace */
    NULL, /* processingInstruction */
    NULL, /* comment */
    NULL, /* xmlParserWarning */
    NULL, /* xmlParserError */
    NULL, /* xmlParserError */
    NULL, /* getParameterEntity */
    NULL, /* cdataBlock; */
    NULL, /* externalSubset; */
    XML_SAX2_MAGIC,
    NULL,
    NULL, /* startElementNs */
    NULL, /* endElementNs */
    NULL  /* xmlStructuredErrorFunc */
};

static xmlSAXHandlerPtr emptySAXHandler = &emptySAXHandlerStruct;

static void libxmlXsdErrorMsgHandler(void *ctx, const char *format, ...) __attribute__((format(printf,2,3)));
static void libxmlXsdErrorMsgHandler(void *ctx, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    static_cast<CLibXmlValidator *>(ctx)->ensureExceptions()->append(*MakeStringExceptionVA(XMLERR_XsdValidationFailed, format, args));
    va_end(args);
}

void CLibXmlValidator::validate()
{
    if (!xmlFile.length() && !xml.length())
        throw MakeStringException(XMLERR_MissingSource, "Source XML not provided");
    if (!xsdFile.length() && !xsd.length())
        throw MakeStringException(XMLERR_MissingSource, "XML Schema not provided");

    xmlParserInputBufferPtr input;
    if (xmlFile.length())
        input = xmlParserInputBufferCreateFilename(xmlFile.get(), XML_CHAR_ENCODING_NONE);
    else
        input = xmlParserInputBufferCreateMem(xml.str(), xml.length()+1, XML_CHAR_ENCODING_NONE);
    if (!input)
        throw MakeStringException(XMLERR_InvalidXml, "Failed to create XML input stream");

    xmlSchemaParserCtxtPtr xsdParser;
    if (xsdFile.length())
        xsdParser = xmlSchemaNewParserCtxt(xsdFile.get());
    else
        xsdParser = xmlSchemaNewMemParserCtxt(xsd.str(), xsd.length());
    if (!xsdParser)
        throw MakeStringException(XMLERR_InvalidXsd, "Failed to load XML Schema");

    xmlSchemaSetParserErrors(xsdParser, libxmlXsdErrorMsgHandler, libxmlXsdErrorMsgHandler, this);
    xmlSchemaPtr schema = xmlSchemaParse(xsdParser);
    xmlSchemaFreeParserCtxt(xsdParser);

    if (!schema)
        throw MakeStringException(XMLERR_InvalidXsd, "XSD schema parsing failed");

    xmlSchemaValidCtxtPtr validator = xmlSchemaNewValidCtxt(schema);
    xmlSchemaSetValidErrors(validator, libxmlXsdErrorMsgHandler, libxmlXsdErrorMsgHandler, this);

    int ret = xmlSchemaValidateStream(validator, input, XML_CHAR_ENCODING_NONE, emptySAXHandler, (void *)this);
    if (ret != 0)
    {
        ensureExceptions()->append(*MakeStringException(XMLERR_XsdValidationFailed, "XML validation failed"));
        throw exceptions.getClear();
    }
    xmlSchemaFreeValidCtxt(validator);
}

class CLibXmlParser : public CInterface, public IXmlDomParser
{
public:
    IMPLEMENT_IINTERFACE;

    CLibXmlParser();
    ~CLibXmlParser();

    IXmlValidator* createXmlValidator();
};

CLibXmlParser::CLibXmlParser()
{
}

CLibXmlParser::~CLibXmlParser()
{
}

IXmlValidator* CLibXmlParser::createXmlValidator()
{
    return new CLibXmlValidator();
}

//=============================================================================================
// factory  

XMLLIB_API IXmlDomParser* getXmlDomParser()
{
    return new CLibXmlParser();
}

// END
//=============================================================================================
