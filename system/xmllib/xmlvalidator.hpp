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

#ifndef __XMLDOMPARSER_HPP__
#define __XMLDOMPARSER_HPP__

#include "jiface.hpp"
#include "xmllib.hpp"

interface XMLLIB_API IXmlValidator : public IInterface
{
    // setXmlSource - specifies the source of the XML to process
    virtual int setXmlSource(const char *pszFileName) = 0;
    virtual int setXmlSource(const char *pszBuffer, unsigned int nSize) = 0;
 
    // setSchemaSource - specifies the schema to validate against
    virtual int setSchemaSource(const char *pszFileName) = 0;
    virtual int setSchemaSource(const char *pszBuffer, unsigned int nSize) = 0;

    // setDTDSource - specifies the DTD to validate against
    virtual int setDTDSource(const char *pszFileName) = 0;
    virtual int setDTDSource(const char *pszBuffer, unsigned int nSize) = 0;
    
    // set schema target namespace
    virtual void setTargetNamespace(const char* ns) = 0;

    // validation am xml against Schema; exception on error 
    virtual void validate() = 0;
};

interface XMLLIB_API IXmlDomParser : public IInterface
{
    virtual IXmlValidator* createXmlValidator() = 0;
};


extern "C" XMLLIB_API IXmlDomParser* getXmlDomParser();


#endif
