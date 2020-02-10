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
#ifndef HQLWCPP_HPP
#define HQLWCPP_HPP

#ifdef HQLCPP_EXPORTS
#define HQLCPP_API DECL_EXPORT
#else
#define HQLCPP_API DECL_IMPORT
#endif

interface IHqlCppInstance;
interface HQLCPP_API ISectionWriter : public IInterface
{
public:
    virtual void generateSection(unsigned indent, IAtom * section, unsigned pass) = 0;
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

extern HQLCPP_API ITemplateExpander * createTemplateExpander(IFile * output, const char * codeTemplate);
extern HQLCPP_API ISectionWriter * createCppWriter(IHqlCppInstance & _instance, CompilerType compiler);
extern bool isTypePassedByAddress(ITypeInfo * type);


#endif
