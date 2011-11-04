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
#ifndef HQLWCPP_HPP
#define HQLWCPP_HPP

#ifdef HQLCPP_EXPORTS
#define HQLCPP_API __declspec(dllexport)
#else
#define HQLCPP_API __declspec(dllimport)
#endif

interface IHqlCppInstance;
interface HQLCPP_API ISectionWriter : public IInterface
{
public:
    virtual void generateSection(unsigned indent, _ATOM section, unsigned pass) = 0;
    virtual void noteLines(size32_t count) = 0;
    virtual void setOutput(IFile * out, IIOStream * outStream) = 0; 
};

interface HQLCPP_API ITemplateExpander : public IInterface
{
public:
    virtual void generate(ISectionWriter & writer, unsigned pass, IProperties * properties = NULL) = 0;
};

extern HQLCPP_API StringBuffer & generateExprCpp(StringBuffer & out, IHqlExpression * expr, CompilerType compiler);
extern HQLCPP_API StringBuffer & generateTypeCpp(StringBuffer & out, ITypeInfo * type, const char * name, CompilerType compiler);
bool generateFunctionPrototype(StringBuffer & out, IHqlExpression * funcdef, CompilerType compiler);
void generateFunctionReturnType(StringBuffer & prefix, StringBuffer & params, ITypeInfo * retType, IHqlExpression * attrs, CompilerType compiler);

extern HQLCPP_API ITemplateExpander * createTemplateExpander(IFile * output, const char * filename, const char *dir);
extern HQLCPP_API ISectionWriter * createCppWriter(IHqlCppInstance & _instance, CompilerType compiler);
extern bool isTypePassedByAddress(ITypeInfo * type);


#endif
