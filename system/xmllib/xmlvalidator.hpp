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


XMLLIB_API IXmlDomParser* getXmlDomParser();


#endif
