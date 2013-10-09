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
#ifndef BUILDFUNC_IPP
#define BUILDFUNC_IPP

class FunctionParamInfo : public CInterface, implements IFunctionParamInfo
{
public:
  FunctionParamInfo(ITypeInfo * type, IAtom * name, unsigned _dir, bool _isArray);
  ~FunctionParamInfo();
  IMPLEMENT_IINTERFACE

//interface IFunctionParamInfo
  virtual ITypeInfo *       queryType() const;

          StringBuffer &        generateCpp(StringBuffer & out);

public:
  enum { ParamIn = 1, ParamOut=2, ParamInOut=3 };
protected:
  ITypeInfo *           type;
  IAtom *                 name;
  unsigned              dir;
  bool                  isArray;
};

class FunctionInfo : public CInterface, implements IFunctionInfo
{
public:
  FunctionInfo(IAtom * _name, IModuleInfo * _module);
  ~FunctionInfo();
  IMPLEMENT_IINTERFACE

//interface IFunctionInfo
  virtual StringBuffer &    generateCpp(StringBuffer & out);
  virtual unsigned          getNumParameters();
  virtual const char *      queryCppName();
  virtual IModuleInfo *     queryModule();
  virtual IAtom *             queryName();
  virtual IFunctionParamInfo *  queryParam(unsigned idx);
  virtual ITypeInfo *       queryReturnType();

          void              addParameter(FunctionParamInfo & info) { params.append(info); }
          void              setName(const char * _cppname);
          void              setReturnType(ITypeInfo * type);

protected:
  IAtom *                 name;
  StringAttr            cppName;
  CIArray               params;
  ITypeInfo *           returnType;
  IModuleInfo *         module;
};


class ModuleInfo : public CInterface, implements IModuleInfo
{
public:
  ModuleInfo(const char * _filename, bool _isSystem);
  IMPLEMENT_IINTERFACE

  virtual StringBuffer &    getName(StringBuffer & out);
  virtual bool              inLibrary();
  virtual bool              isSystem();
protected:
  StringAttr            filename;
  bool                  systemModule;
};


class FunctionDatabase : public CInterface, implements IFunctionDatabase
{
public:
  FunctionDatabase();
  IMPLEMENT_IINTERFACE

//interface IFunctionDatabase
  virtual IFunctionInfo *   queryFunction(unsigned idx);
  virtual IFunctionInfo *   queryFunction(IAtom * name);
  virtual IModuleInfo *     queryModule(unsigned idx);

          void              addFunction(FunctionInfo & info) { functions.append(info); }
          void              addModule(ModuleInfo & info) { modules.append(info); }
protected:
  CIArray               modules;
  CIArray               functions;
};

#endif


