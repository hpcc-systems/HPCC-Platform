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
interface IAtomPropertyIterator : public IPropertyIteratorOf<_ATOM> { };
interface IProperties : public IPropertiesOf<char_ptr, IPropertyIterator> { };

#ifdef _MSC_VER
#pragma warning( pop ) 
#endif

extern jlib_decl IProperties *createProperties(bool nocase = false);
extern jlib_decl IProperties *createProperties(const char *filename, bool nocase = false);
extern jlib_decl IProperties *querySystemProperties();
extern jlib_decl IProperties *getSystemProperties();

#endif

