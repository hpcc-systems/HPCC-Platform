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
#ifndef BUILDFUNC_IPP
#define BUILDFUNC_IPP

class FunctionParamInfo : public CInterface, implements IFunctionParamInfo
{
public:
  FunctionParamInfo(ITypeInfo * type, _ATOM name, unsigned _dir, bool _isArray);
  ~FunctionParamInfo();
  IMPLEMENT_IINTERFACE

//interface IFunctionParamInfo
  virtual ITypeInfo *       queryType() const;

          StringBuffer &        generateCpp(StringBuffer & out);

public:
  enum { ParamIn = 1, ParamOut=2, ParamInOut=3 };
protected:
  ITypeInfo *           type;
  _ATOM                 name;
  unsigned              dir;
  bool                  isArray;
};

class FunctionInfo : public CInterface, implements IFunctionInfo
{
public:
  FunctionInfo(_ATOM _name, IModuleInfo * _module);
  ~FunctionInfo();
  IMPLEMENT_IINTERFACE

//interface IFunctionInfo
  virtual StringBuffer &    generateCpp(StringBuffer & out);
  virtual unsigned          getNumParameters();
  virtual const char *      queryCppName();
  virtual IModuleInfo *     queryModule();
  virtual _ATOM             queryName();
  virtual IFunctionParamInfo *  queryParam(unsigned idx);
  virtual ITypeInfo *       queryReturnType();

          void              addParameter(FunctionParamInfo & info) { params.append(info); }
          void              setName(const char * _cppname);
          void              setReturnType(ITypeInfo * type);

protected:
  _ATOM                 name;
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
  virtual IFunctionInfo *   queryFunction(_ATOM name);
  virtual IModuleInfo *     queryModule(unsigned idx);

          void              addFunction(FunctionInfo & info) { functions.append(info); }
          void              addModule(ModuleInfo & info) { modules.append(info); }
protected:
  CIArray               modules;
  CIArray               functions;
};

#endif


