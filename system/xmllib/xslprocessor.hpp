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

#ifndef XSLPROCESSOR_HPP_INCL
#define XSLPROCESSOR_HPP_INCL
#include "jliball.hpp"

#include "xmllib.hpp"

class StringBuffer;


interface XMLLIB_API IIncludeHandler : public IInterface
{
public:
    virtual bool getInclude(const char* includename, MemoryBuffer& includebuf, bool& pathOnly) = 0;
};


interface XMLLIB_API IXslFunction : public IInterface
{
public:
   virtual const char* getName() const = 0;
   virtual bool isAssigned ()  = 0;
   virtual void setAssigned(bool bAssigned)  = 0;
};

//The transform component is used to setup, and maintain any state associated with a transform
//
interface XMLLIB_API IXslTransform : public IInterface
{
public:
    // setXmlSource - specifies the source of the XML to process
    //
    virtual int setXmlSource(const char *pszFileName) = 0;
    virtual int setXmlSource(const char *pszBuffer, unsigned int nSize) = 0;
 
    // setXslSource - specifies the source of the XSLT "script" to use
    //
    virtual int setXslSource(const char *pszFileName, const char *cacheId=NULL) = 0;
    virtual int setXslSource(const char *pszBuffer, unsigned int nSize, const char *rootpath=NULL, const char *cacheId=NULL) = 0;
 
    // setResultTarget - Specifies where the post transform data is to be placed
    // must be set before calling parameterless variety of transform
    virtual int setResultTarget(const char *pszFileName) = 0; 
    virtual int setResultTarget(char *pszBuffer, unsigned int nSize) = 0;
    virtual int closeResultTarget() = 0;
 
    // setParameter - for adding value pair parameters
    //    szExpression == "" is empty parameter
    //    szExpression == NULL removes any existing parameter with the given name
    //
    virtual int setParameter(const char *pszName, const char *pszExpression) = 0;
    virtual void copyParameters(IProperties *params) = 0;

    virtual int setStringParameter(const char *pszName, const char* pszString) = 0;

    //In XalanTransformer, m_paramPairs are cleaned after each doTransform, so this function is really not necessary.
    //virtual int removeAllParameters () = 0;

    // transform - for doing it
    virtual int transform() = 0;
    virtual int transform(StringBuffer &target) = 0;
    virtual int transform(ISocket* targetSocket) = 0;

    // Set include source
    virtual int setIncludeHandler(IIncludeHandler* handler) = 0;

    // createExternalFunction - create a simple external function that processes text (a convenient way to define external functions)
    // 
    virtual IXslFunction* createExternalFunction( const char* pszNameSpace, void (*fn)(StringBuffer& out, const char* pszIn, IXslTransform*)) = 0;

    // setExternalFunction - add an external function that can be called from within the XSLT
    //
    virtual int setExternalFunction( const char* pszNameSpace, IXslFunction*, bool set) = 0;

    // getLastError - returns the error, if any, in last transformation, compiling stylesheet or parsing source
    //
    virtual const char* getLastError() const = 0;

    // getMessages - returns the messages produced, if any, in last transformation, compiling stylesheet or parsing source
    // The most useful application is to fetch any <xsl:message/> as a result of transformation.
    // Note: in case of any errors, any cumulative messages until the error are bundled with the
    // message.
    //
    virtual const char* getMessages() const = 0;

    //Allow the caller to set necessary info that can be used to identify it.
    //This is typically useful in [static] external functions, which can use 
    //this info to work with caller/context.
    //
    virtual void setUserData(void*) = 0;
    virtual void* getUserData() const = 0;
};

interface XMLLIB_API IXslProcessor : public IInterface
{
    virtual IXslTransform *createXslTransform() = 0;
 
    // execute - runs the transformation placing the results in the specified output location
    //
    virtual int execute(IXslTransform *pITransform) = 0;

    virtual int setDefIncludeHandler(IIncludeHandler* handler)=0;

    virtual void setCacheTimeout(int timeout) = 0;

    virtual int getCacheTimeout() = 0;
};

extern "C" XMLLIB_API IXslProcessor* getXslProcessor();
extern "C" XMLLIB_API IXslProcessor* getXslProcessor2();


#endif
