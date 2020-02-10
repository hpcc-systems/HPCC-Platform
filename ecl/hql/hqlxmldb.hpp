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
#ifndef XMLDB_INCL
#define XMLDB_INCL

#include "jiface.hpp"
#include "jiter.hpp"
#include "hql.hpp"
#include "hqlerror.hpp"

//-----------------------------------------------------------------------------
// Class Definitions
//-----------------------------------------------------------------------------
/**
 * Iterator interface
 */
class StringBuffer; 

interface IXmlScope : public IInterface
{
    virtual IIterator* getScopes(const char *, bool doAll) = 0;
    virtual bool getValue(const char*, StringBuffer &) = 0;
    virtual bool setValue(const char*, const char*) = 0;
    virtual bool appendValue(const char*, const char*) = 0;
    virtual bool declareValue(const char *) = 0;

    virtual int getInt(const char*) = 0;
    virtual int getInt(const char*, int defValue) = 0;
    virtual void loadXML(const char * text, const char * element) = 0;
};


interface IEclRepository;

#define HashFor(sub,scope)  \
    IIterator*  sub##Itr = scope->getScopes(#sub, false); \
    ForEach(*sub##Itr) \
    { \
        IXmlScope* sub = (IXmlScope*)&sub##Itr->query(); 

#define EndHashFor(sub) \
    } \
    sub##Itr->Release(); 

/* variable name can not be defaulted to the subscope name */
#define HashForEx(sub,name,scope)  \
    IIterator*  sub##Itr = scope->getScopes(name, false); \
    ForEach(*sub##Itr) \
    { \
        IXmlScope* sub = (IXmlScope*)&sub##Itr->query(); 

#define EndHashForEx(sub) \
    } \
    sub##Itr->Release(); 

//-----------------------------------------------------------------------------
// Prototypes
//-----------------------------------------------------------------------------

HQL_API IXmlScope* loadXML(const char* filename);
HQL_API IXmlScope* createXMLScope();

#endif
