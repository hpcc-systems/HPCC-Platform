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
#ifndef HQLWCPP_IPP
#define HQLWCPP_IPP

class HQLCPP_API CppWriterTemplate : public CInterface, public ITemplateExpander
{
public:
    CppWriterTemplate();
    ~CppWriterTemplate();
    IMPLEMENT_IINTERFACE

    virtual void generate(ISectionWriter & writer, unsigned pass, IProperties * properties = NULL);
                    bool loadTemplate(const char * filename, const char *dir);

    void outputQuoted(ISectionWriter & writer, size32_t len, const char * str)
    {
        writer.noteLines(memcount(len, str, '\n'));
        outStream->write(len, str);
    }

    void setOutput(IFile * _output)
    {
        Owned<IFileIO> io = _output->open(IFOcreate);
        if (!io)
            throwError1(HQLERR_CouldNotCreateOutputX, _output->queryFilename());

        out.set(_output);
        outStream.setown(createIOStream(io));
    }


private:
  enum TplSectionType { TplEmbed, TplExpand, TplCondition, TplEndCondition };
    struct CppTemplateSection : public CInterface
    {
        TplSectionType  type;
        IAtom *                       id;
        const char *        position;
        unsigned                len;
        unsigned                indent;
    };

protected:
    char * text;
    unsigned len;
    CIArray sections;
    Owned<IFile> out;
    Owned<IIOStream> outStream;
};

class HQLCPP_API HqlCppWriter
{
public:
    HqlCppWriter(CompilerType _compiler);
    HqlCppWriter(StringBuffer & _out, CompilerType _compiler);

    StringBuffer & generateExprCpp(IHqlExpression * expr);
    bool generateFunctionPrototype(IHqlExpression * funcdef);
    void generateFunctionReturnType(StringBuffer & params, ITypeInfo * retType, IHqlExpression * attrs);
    void generateStatementsForPass(HqlStmts & stmts, unsigned indent, unsigned pass);
    void generateType(ITypeInfo * type, const char * name);

    void noteLines(size32_t count) { outputLineNum += count; }
    void setOutput(IFile * out, IIOStream * outStream); 

protected:
    void flush();
    void generate(HqlStmtArray & stmts);
    void generateChildren(IHqlStmt * stmt, bool addBraces);
    StringBuffer & generateChildExpr(StringBuffer & out, IHqlExpression * expr, unsigned childIndex);
    bool generateFunctionPrototype(IHqlExpression * funcdef, const char * name);
    void generateInitializer(IHqlExpression * expr);
    void generateParamCpp(IHqlExpression * expr, IHqlExpression * attrs);
    void generateSimpleAssign(IHqlExpression * target, IHqlExpression * source);
    void generateStmt(IHqlStmt * stmt);
    void generateStmtAssign(IHqlStmt * assign, bool link);
    void generateStmtAssignModify(IHqlStmt * assign);
    void generateStmtCase(IHqlStmt * stmt);
    void generateStmtCatch(IHqlStmt * stmt);
    void generateStmtDeclare(IHqlStmt * declare);
    void generateStmtFilter(IHqlStmt * stmt);
    void generateStmtFunction(IHqlStmt * stmt);
    void generateStmtLine(IHqlStmt * stmt);
    void generateStmtLoop(IHqlStmt * stmt);
    void generateStmtSwitch(IHqlStmt * stmt);
    void generateStmtForPass(IHqlStmt * stmt, unsigned pass);

    //Wrappers around recursive calls.
    StringBuffer & generateExprCpp(StringBuffer & out, IHqlExpression * expr);
    void generateType(StringBuffer & result, ITypeInfo * type, const char * name);

    StringBuffer & indent();
    void indent(int delta)          { curIndent += delta; }
    StringBuffer & newline();
    StringBuffer & queryBreakLine();
    StringBuffer & queryIndent();
    void queryNewline();

    StringBuffer & generateChildExpr(IHqlExpression * expr, unsigned childIndex);
    StringBuffer & generateCommaChildren(IHqlExpression * expr);
    void generateOrderExpr(IHqlExpression * left, IHqlExpression * right);
    StringBuffer & generateExprAsChar(IHqlExpression * expr);

protected:
    Linked<IFile> targetFile;
    Linked<IIOStream> target;
    StringBuffer & out;
    StringBuffer defaultOut;
    unsigned curIndent;
    unsigned startOffset;
    unsigned outputLineNum;
    CompilerType compiler;
};

class HQLCPP_API HqlCppSectionWriter : public CInterface, implements ISectionWriter
{
public:
    HqlCppSectionWriter(IHqlCppInstance & _instance, CompilerType _compiler) : instance(_instance), writer(_compiler)
    {
    }
    IMPLEMENT_IINTERFACE

    virtual void generateSection(unsigned indent, IAtom * section, unsigned pass);
    virtual void noteLines(size32_t count) { writer.noteLines(count); }
    virtual void setOutput(IFile * out, IIOStream * outStream) { writer.setOutput(out, outStream); }

    IHqlCppInstance &   instance;
    HqlCppWriter writer;
};

#endif
