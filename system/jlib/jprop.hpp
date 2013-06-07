/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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



#ifndef JPROP_INCL
#define JPROP_INCL

#include "jiface.hpp"
#include "jhash.hpp"
#include "jstring.hpp"

// Property tables

template <class PTYPE>
interface jlib_decl IPropertyIteratorOf : extends IInterface
{
    virtual bool first() = 0;
    virtual bool next() = 0;
    virtual bool isValid() = 0;
    virtual PTYPE getPropKey() = 0;
};

template <class PTYPE, class PITER>
interface jlib_decl IPropertiesOf : extends serializable
{
    virtual int getPropInt(PTYPE propname, int dft=0) = 0;
    virtual bool getProp(PTYPE propname, StringBuffer &ret) = 0;
    virtual const char *queryProp(PTYPE propname) = 0;
    virtual void setProp(PTYPE propname, int val) = 0;
    virtual void setProp(PTYPE propname, const char *val) = 0;
    virtual bool hasProp(PTYPE propname) = 0;
    virtual PITER *getIterator() = 0;
    virtual void loadFile(const char *filename) = 0;
    virtual void loadProps(const char *text) = 0;
    virtual void loadProp(const char *text) = 0;
    virtual void loadProp(const char *text, int dft) = 0;
    virtual void loadProp(const char *text, bool dft) = 0;
    virtual void loadProp(const char *text, const char * dft) = 0;
    virtual void saveFile(const char *filename) = 0;
    virtual bool removeProp(PTYPE propname) = 0;
    virtual bool getPropBool(PTYPE propname, bool dft=false) = 0;
};

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4275 ) // hope this warning not significant! (may get link errors I guess)
#endif

interface IPropertyIterator : public IPropertyIteratorOf<char_ptr> { };
interface IAtomPropertyIterator : public IPropertyIteratorOf<IAtom *> { };
interface IProperties : public IPropertiesOf<char_ptr, IPropertyIterator> { };

#ifdef _MSC_VER
#pragma warning( pop ) 
#endif

extern jlib_decl IProperties *createProperties(bool nocase = false);
extern jlib_decl IProperties *createProperties(const char *filename, bool nocase = false);
extern jlib_decl IProperties *querySystemProperties();
extern jlib_decl IProperties *getSystemProperties();

#endif

