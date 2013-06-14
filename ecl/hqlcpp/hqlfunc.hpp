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
    virtual IAtom *                       queryName() = 0;
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
    virtual IFunctionInfo *     queryFunction(IAtom * name) = 0;
    virtual IModuleInfo *       queryModule(unsigned idx) = 0;
};

#endif


