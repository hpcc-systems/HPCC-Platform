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
#ifndef BUILDFUNC_HPP
#define BUILDFUNC_HPP

#ifdef READSCRIPT_EXPORTS
#define READSCRIPT_DECL __declspec(dllexport)
#else
#define READSCRIPT_DECL __declspec(dllimport)
#endif


interface ITypeInfo;
interface IFunctionParamInfo : public IInterface
{
public:
    virtual ITypeInfo *             queryType() const = 0;
};

interface IModuleInfo;
interface ITypeInfo;
interface IHelperFunction : public IInterface
{
public:
    virtual StringBuffer &      generateCpp(StringBuffer & out) = 0;
};

interface IFunctionInfo : public IHelperFunction
{
public:
    virtual unsigned                    getNumParameters() = 0;
    virtual const char *            queryCppName() = 0;
    virtual IModuleInfo *       queryModule() = 0;
    virtual _ATOM                       queryName() = 0;
    virtual IFunctionParamInfo * queryParam(unsigned idx) = 0;
    virtual ITypeInfo *             queryReturnType() = 0;
};

interface IModuleInfo : public IInterface
{
public:
    virtual StringBuffer &      getName(StringBuffer & out) = 0;
    virtual bool                            inLibrary() = 0;
    virtual bool                            isSystem() = 0;
};


interface IFunctionDatabase : public IInterface
{
public:
    virtual IFunctionInfo *     queryFunction(unsigned idx) = 0;
    virtual IFunctionInfo *     queryFunction(_ATOM name) = 0;
    virtual IModuleInfo *       queryModule(unsigned idx) = 0;
};

#endif


