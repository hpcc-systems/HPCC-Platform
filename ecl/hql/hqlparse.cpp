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
#include "hqlgram.hpp"
#include "hqlfold.hpp"
#include "jiter.ipp"
#include "jptree.hpp"
#include "hqlerrors.hpp"
#include "hqlthql.hpp"
#include "hqlexpr.hpp"
#include "jdebug.hpp"
#include "hqlexpr.ipp"

#include "hqlgram.h"

#define YY_NO_UNISTD_H
#include "hqllex.hpp"

//#define TIMING_DEBUG

#define  MAX_LOOP_TIMES 250000

// =========================== local helper functions ===================================

static bool isInModule(HqlLookupContext & ctx, const char* module_name, const char* attr_name);
static StringBuffer& mangle(IErrorReceiver* errReceiver,const char* src, StringBuffer& mangled,bool demangle);

// =========================== CDummyScopeIterator ======================================

class CDummyScopeIterator : public IIterator, public CInterface
{
    IXmlScope *parent;
public:
    IMPLEMENT_IINTERFACE;
    CDummyScopeIterator(IXmlScope *_parent)
    {
        parent = _parent;
    }
    ~CDummyScopeIterator ()
    {
    }
    virtual bool first()
    {
        return true;
    };
    virtual bool next()
    {
        return true;
    }
    virtual bool isValid()
    {
        return true;
    }
    virtual IInterface & query()
    {
        return *parent;
    }
    virtual IInterface & get()
    {
        IInterface &ret = query(); ret.Link(); return ret;
    }
};

// =========================== CTemplateContext =========================================

class CTemplateContext : implements ITemplateContext, public CInterface
{
    HqlLex * lexer;
    IXmlScope* m_xmlScope;
    int m_startLine,m_startCol;
    HqlLookupContext & m_lookupContext;

public:
    IMPLEMENT_IINTERFACE;

    CTemplateContext(HqlLex * _lexer, HqlLookupContext & lookupContext, IXmlScope* xmlScope, int startLine,int startCol)
     : lexer(_lexer), m_xmlScope(xmlScope), m_lookupContext(lookupContext),
       m_startLine(startLine), m_startCol(startCol) {}

    virtual IXmlScope* queryXmlScope()  { return m_xmlScope; }
    virtual IEclRepository* queryDataServer()  { return m_lookupContext.queryRepository(); }

    // convenient functions
    virtual bool isInModule(const char* moduleName, const char* attrName) { return ::isInModule(m_lookupContext, moduleName,attrName); }
    virtual StringBuffer& getDataType(const char* field, StringBuffer& tgt) { return lexer->doGetDataType(tgt, field, m_startLine, m_startCol); }

    virtual StringBuffer& mangle(const char* src, StringBuffer& mangled) { return ::mangle(m_lookupContext.errs,src,mangled,false); }
    virtual StringBuffer& demangle(const char* mangled, StringBuffer& demangled) { return ::mangle(m_lookupContext.errs,mangled,demangled,true); }

    virtual void reportError(int errNo,const char* format,...);
    virtual void reportWarning(int warnNo,const char* format,...);
};

void CTemplateContext::reportError(int errNo,const char* format,...)
{
    if (m_lookupContext.errs)
    {
        va_list args;
        va_start(args, format);
        StringBuffer msg;
        msg.valist_appendf(format,args);
        m_lookupContext.errs->reportError(errNo,msg.str(),NULL,m_startLine,m_startCol,0);
        va_end(args);
    }
}

void CTemplateContext::reportWarning(int warnNo,const char* format,...)
{
    if (m_lookupContext.errs)
    {
        va_list args;
        va_start(args, format);
        StringBuffer msg;
        msg.valist_appendf(format,args);
        WarnErrorCategory category = CategoryUnusual; // a reasonable default
        m_lookupContext.errs->reportWarning(category, warnNo,msg.str(),NULL,m_startLine,m_startCol,0);
        va_end(args);
    }
}


// ===================================== HqlLex ============================================

class CHqlParserPseduoScope : public CHqlScope
{
protected:
    HqlGram * parser;

public:
    CHqlParserPseduoScope(HqlGram * _parser) : CHqlScope(no_privatescope) { parser = _parser; }

    virtual IHqlExpression *lookupSymbol(IIdAtom * name, unsigned lookupFlags, HqlLookupContext & ctx)
    {
        attribute errpos;
        errpos.clearPosition();
        return parser->lookupSymbol(name, errpos);
    }

    virtual IHqlScope * queryConcreteScope() { return this; }
    virtual bool allBasesFullyBound() const { return true; }
};


// ===================================== HqlLex ============================================

HqlLex::HqlLex(HqlGram *parser, IFileContents * contents, IXmlScope *_xmlScope, IHqlExpression *_macroExpr)
 : yyParser(parser), xmlScope(LINK(_xmlScope)), macroExpr(_macroExpr), sourcePath(contents->querySourcePath())
{
    assertex(parser);
    init(contents);
}

void HqlLex::init(IFileContents * _text)
{
    text.set(_text);
    inmacro = NULL;
    parentLex = NULL;
    inComment = false;
    inSignature = false;
    inCpp = false;
    hasHashbreak = false;
    encrypted = false;
    loopTimes = 0;
    skipNesting = 0;
    macroGathering = 0;
    forLoop = NULL;

    size32_t len = _text->length();
    yyBuffer = new char[len+2]; // Include room for \0 and another \0 that we write beyond the end null while parsing
    memcpy(yyBuffer, text->getText(), len);
    yyBuffer[len] = '\0';
    yyBuffer[len+1] = '\0';

    yyLineNo = 1;
    yyPosition = 0;
    yyColumn = 1;
    yyStartPos = 0;
    lastToken = 0;

    eclyylex_init(&scanner);
    eclyy_scan_buffer(yyBuffer, len+2, scanner);
}

///////////////////////////////////////////////////
//    public destructor
//

HqlLex::~HqlLex()
{
    eclyylex_destroy(scanner);
    scanner = NULL;
    delete[] yyBuffer;
    ::Release(xmlScope);
    ::Release(macroExpr);
    if (inmacro) delete inmacro;
    ::Release(forLoop);
}

char* HqlLex::get_yyText(void)
{
    if (inmacro)
        return inmacro->get_yyText();
    return eclyyget_text(scanner);
}

IFileContents* HqlLex::query_FileContents(void)
{
    if (inmacro)
        return inmacro->query_FileContents();
    return text;
}

bool HqlLex::isMacroActive(IHqlExpression *expr)
{
    if (expr==macroExpr)
        return true;
    else if (parentLex)
        return parentLex->isMacroActive(expr);
    else
        return false;
}

bool HqlLex::assertNext(YYSTYPE & returnToken, int expected, unsigned code, const char * msg)
{
    if (yyLex(returnToken, false,0) != expected)
    {
        reportError(returnToken, code, "%s", msg);
        returnToken.release();
        return false;
    }
    return true;
}

bool HqlLex::assertNextOpenBra()
{
    YYSTYPE tempToken;
    return assertNext(tempToken, '(', ERR_EXPECTED_LEFTCURLY, "( expected");
}

bool HqlLex::assertNextComma()
{
    YYSTYPE tempToken;
    return assertNext(tempToken, ',', ERR_EXPECTED_COMMA, ", expected");
}


StringBuffer &HqlLex::getTokenText(StringBuffer &ret)
{
    if (inmacro)
        return inmacro->getTokenText(ret);
    return ret.append(yyPosition - yyStartPos, yyBuffer+yyStartPos);
}

IHqlExpression *HqlLex::lookupSymbol(IIdAtom * name, const attribute& errpos)
{
    return yyParser->lookupSymbol(name, errpos);
}

unsigned HqlLex::hex2digit(char c)
{
  if (c >= 'a')
    return (c - 'a' + 10);
  else if (c >= 'A')
    return (c - 'A' + 10);
  return (c - '0');
}

__int64 HqlLex::str2int64(unsigned len, const char * digits, unsigned base)
{
  __int64 value = 0;
  while (len--)
  {
    char c = *digits++;
    value = value * base + hex2digit(c);
  }
  return value;
}

void HqlLex::hex2str(char * target, const char * digits, unsigned len)
{
  while (len)
  {
    *target++ = (hex2digit(digits[0]) << 4) | hex2digit(digits[1]);
    len -= 2;
    digits += 2;
  }
}

IHqlExpression * HqlLex::createIntegerConstant(__int64 value, bool isSigned)
{
    return createConstant(createIntValue(value, makeIntType(8, isSigned)));
}

void HqlLex::pushText(IFileContents * text, int startLineNo, int startColumn)
{
#ifdef TIMING_DEBUG
    MTIME_SECTION(timer, "HqlLex::pushText");
#endif
    inmacro = new HqlLex(yyParser, text, NULL, NULL);
    inmacro->set_yyLineNo(startLineNo);
    inmacro->set_yyColumn(startColumn);
}


void HqlLex::pushText(const char *s, int startLineNo, int startColumn)
{
    Owned<IFileContents> macroContents = createFileContentsFromText(s, sourcePath);
    pushText(macroContents, startLineNo, startColumn);
}


void HqlLex::pushText(const char *s)
{
#ifdef TIMING_DEBUG
    MTIME_SECTION(timer, "HqlLex::pushText");
#endif
    Owned<IFileContents> macroContents = createFileContentsFromText(s, sourcePath);
    inmacro = new HqlLex(yyParser, macroContents, NULL, NULL);
    inmacro->set_yyLineNo(yyLineNo);
    inmacro->set_yyColumn(yyColumn);

#if defined (TRACE_MACRO)
    PrintLog("MACRO>> inmacro %p created for \"%s\" for macro parameters.\n",inmacro,s);
#endif
}

void HqlLex::setMacroParam(const YYSTYPE & errpos, IHqlExpression* funcdef, StringBuffer& curParam, IIdAtom * argumentId, unsigned& parmno,IProperties *macroParms)
{
    IHqlExpression * formals = queryFunctionParameters(funcdef);
    IHqlExpression * defaults = queryFunctionDefaults(funcdef);
    unsigned numFormals = formals->numChildren();
    unsigned thisParam = (unsigned)-1;
    if (argumentId)
    {
        IAtom * argumentName = lower(argumentId);
        unsigned argNum = 0;
        for (unsigned i=0; i < numFormals; i++)
        {
            if (formals->queryChild(i)->queryName() == argumentName)
            {
                argNum = i+1;
                break;
            }
        }

        if (argNum == 0)
            reportError(errpos, ERR_NAMED_PARAM_NOT_FOUND, "Named parameter '%s' not found in macro", str(argumentId));
        else
            thisParam = argNum;
    }
    else
        thisParam = ++parmno;

    if (thisParam <= numFormals)
    {
        IHqlExpression* formal = formals->queryChild(thisParam-1);
        if (curParam.length()==0)
        {
            IHqlExpression* def = queryDefaultValue(defaults, thisParam-1);
            if (!def)
            {
                StringBuffer msg("Omitted parameter ");
                msg.append(parmno).append(" has no default value");
                reportError(errpos, ERR_PARAM_NODEFVALUE, "%s", msg.str());
            }
            else
            {
                if (!getFoldedConstantText(curParam, def))
                {
                    StringBuffer msg("Default value for parameter ");
                    msg.append(parmno).append(" should be a constant");
                    reportError(errpos, ERR_PARAM_NODEFVALUE, "%s", msg.str());
                }
            }
        }
        //PrintLog("Set macro parm: %s", curParam.str());
//      if (macroParms->queryProp(formal->queryName()))
//          reportError(errpos, ERR_NAMED_ALREADY_HAS_VALUE, "Parameter %s already has a value supplied", argumentName->str());
//      else
            macroParms->setProp(str(formal->queryName()), curParam.str());
    }
    curParam.clear();
}

/* This is pushing a macro definition. */
void HqlLex::pushMacro(IHqlExpression *expr)
{
    /* expr points to namedSymbol(no_funcdef):
        child(0)-> no_macro = macro body
        child(1) = parameters
        child(2) = defaults for parameters
    */

    YYSTYPE nextToken;
    int tok = yyLex(nextToken, false, 0);
    if (tok != '(')
    {
        //throw MakeStringException(2, "( expected");
        reportError(nextToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        if (tok==EOF)
            return;
        // assume we've got an '(', we can continue parsing.
    }

    unsigned parenDepth = 1;
    StringBuffer curParam;
    Owned<IProperties> macroParms = createProperties();
    IHqlExpression * formals = expr->queryChild(1);
    IHqlExpression * defaults = expr->queryChild(2);
    unsigned formalParmCt = formals->numChildren();
    unsigned parmno = 0;
    IIdAtom * possibleName = NULL;
    IIdAtom * argumentName = NULL;
    while (parenDepth)
    {
        tok = yyLex(nextToken, false, 0);
        switch(tok)
        {
        case '[':
        case '(':
            parenDepth++;
            curParam.append((char)tok);
            break;
        case ']':
            if (parenDepth > 1)
                parenDepth--;
            curParam.append((char)tok);
            break;
        case ')':
            parenDepth--;
            if (parenDepth)
                curParam.append(')');
            else if (formalParmCt>0 || curParam.length()>0) // handle last parameter
                setMacroParam(nextToken, expr, curParam, argumentName, parmno, macroParms);
            break;
        case ',':
            if (parenDepth==1)
                setMacroParam(nextToken, expr, curParam, argumentName, parmno, macroParms);
            else
                curParam.append(',');
            possibleName = NULL;
            argumentName = NULL;
            break;
        case ASSIGN:
            {
                bool done = false;
                if (parenDepth == 1 && possibleName)
                {
                    //Horrible.  Allow NAMED <id> := or <id> :=
                    const char * text = curParam.str();
                    while (isspace((byte)*text)) text++;
                    if (memicmp(text, "NAMED", 5) == 0)
                        text += 5;
                    while (isspace((byte)*text)) text++;
                    if (strlen(str(possibleName)) == strlen(text))
                    {
                        argumentName = possibleName;
                        possibleName = NULL;
                        curParam.clear();
                        done = true;
                    }
                }
                if (!done)
                    getTokenText(curParam.append(' '));
                break;
            }
        case EOF:
            reportError(nextToken, ERR_MACRO_EOFINPARAM,"EOF encountered while gathering macro parameters");
            // no attempt to recover at the end of the file, but cleanup is needed.
            return;
        case UNKNOWN_ID:
            possibleName = nextToken.getId();
            curParam.append(' ').append(str(possibleName));
            break;
        default:
            curParam.append(' ');
            getTokenText(curParam);
            break;
        }
        nextToken.release();
    }

    if (parmno > formalParmCt)
    {
        StringBuffer msg("Too many actual parameters supplied to macro");
        if (expr->queryName())
            msg.append(' ').append(expr->queryName());
        msg.appendf(": expected %d, given %d", formalParmCt, parmno);
        reportError(nextToken, ERR_PARAM_TOOMANY, "%s", msg.str());
    }
    else if (parmno < formalParmCt)
    {
        for (unsigned idx = parmno; idx < formalParmCt; idx++)
        {
            IHqlExpression* formal = formals->queryChild(idx);
            if (!macroParms->queryProp(str(formal->queryName())))
            {
                IHqlExpression* def = queryDefaultValue(defaults, idx);
                if (def)
                {
                    if (!getFoldedConstantText(curParam, def))
                    {
                        StringBuffer msg("Omitted parameter ");
                        msg.append(idx+1);
                        if (expr->queryName())
                            msg.append(" to macro ").append(expr->queryName());
                        msg.append(" should be a constant value");
                        reportError(nextToken, ERR_PARAM_NODEFVALUE, "%s", msg.str());
                    }
                    macroParms->setProp(str(formal->queryName()), curParam.str());
                    //PrintLog("Set macro parm: %s", curParam.str());
                    curParam.clear();
                }
                else
                {
                    StringBuffer msg("Omitted parameter ");
                    msg.append(idx+1);
                    if (expr->queryName())
                        msg.append(" to macro ").append(expr->queryName());
                    msg.append(" has no default value");
                    reportError(nextToken, ERR_PARAM_NODEFVALUE, "%s", msg.str());
                }
            }
        }
    }

    IHqlExpression *macroBodyExpr = expr->queryChild(0);
    IFileContents * macroContents = static_cast<IFileContents *>(macroBodyExpr->queryUnknownExtra());
    if (isMacroActive(expr))
    {
        StringBuffer msg;
        msg.append("recursive macro call: ").append(getMacroName());
        reportError(nextToken, ERR_MACRO_RECURSIVE, "%s", msg.str());

        // error recovery
        if (expr->isAction())
            pushText("0;");
        else if (expr->isDataset())
            pushText("{}");
        else
            pushText("0 ENDMACRO");
    }
    else
    {
        inmacro = new HqlLex(yyParser, macroContents, NULL, LINK(expr));

#if defined(TRACE_MACRO)
        PrintLog("MACRO>> inmacro %p created for \"%s\" at %d:%d\n",inmacro, s.str(),macroBodyExpr->getStartLine(),macroBodyExpr->getStartColumn());
//      PrintLog("MACRO>> macro called at %d:%d\n", expr->getStartLine(),expr->getStartColumn());
#endif
        /* set the lineno and column in the original source as the starting point */
        inmacro->yyLineNo = macroBodyExpr->getStartLine();
        inmacro->yyColumn = macroBodyExpr->getStartColumn();
        inmacro->setParentLex(this);
        inmacro->macroParms.setown(macroParms.getClear());
    }
}

void HqlLex::checkSignature()
{
    YYSTYPE dummyToken;
    dummyToken.setPosition(0,0,0,text->querySourcePath());
    Owned<IPipeProcess> pipe = createPipeProcess();
    pipe->run("gpg", "gpg --verify -", ".", true, false, true, 0, false);
    try
    {
        pipe->write(text->length(), text->getText());
        pipe->closeInput();
        unsigned retcode = pipe->wait();
        if (retcode)
        {
            StringBuffer buf;
            Owned<ISimpleReadStream> pipeReader = pipe->getErrorStream();
            const size32_t chunkSize = 8192;
            for (;;)
            {
                size32_t sizeRead = pipeReader->read(chunkSize, buf.reserve(chunkSize));
                if (sizeRead < chunkSize)
                {
                    buf.setLength(buf.length() - (chunkSize - sizeRead));
                    break;
                }
            }
            DBGLOG("GPG %d %s", retcode, buf.str());
            StringArray allErrs;
            allErrs.appendList(buf, "\n");
            ForEachItemIn(idx, allErrs)
            {
                if (strlen(allErrs.item(idx)))
                    reportWarning(CategorySecurity, dummyToken, WRN_SECURITY_SIGNERROR, "gpg error: %s", allErrs.item(idx));
            }
        }
        else
            yyParser->inSignedModule = true;
    }
    catch (IException *e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        reportWarning(CategorySecurity, dummyToken, WRN_SECURITY_SIGNERROR, "Failed to execute gpg: %d %s. Module will be treated as unsigned", e->errorCode(), msg.str());
        e->Release();
    }
}

/* Read encrypted syntax, and push the decrypted text as a macro. */
void HqlLex::processEncrypted()
{
    YYSTYPE nextToken;
    if (yyLex(nextToken, false,0) != '(')
    {
        reportError(nextToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        nextToken.release();
        return;
    }
    StringBuffer encoded64;
    loop
    {
        if (yyLex(nextToken, false,0) != STRING_CONST)
        {
            reportError(nextToken, ERR_EXPECTED, "String expected");
            nextToken.release();
            return;
        }

        OwnedHqlExpr str = nextToken.getExpr();
        getStringValue(encoded64, str);

        int next = yyLex(nextToken, false,0);
        if (next == ')')
            break;
        if (next != ',')
        {
            reportError(nextToken, ERR_EXPECTED_COMMA, ", expected");
            nextToken.release();
            return;
        }
    }
    if (yyLex(nextToken, false,0) != ';')
    {
        reportError(nextToken, ERR_EXPECTED, "; expected");
        nextToken.release();
        return;
    }

    MemoryBuffer decrypted;
    decryptEclAttribute(decrypted, encoded64.str());
    decrypted.append(0);    // add a null terminator to the string...
    Owned<ISourcePath> sourcePath = createSourcePath("<encrypted>");
    Owned<IFileContents> decryptedContents = createFileContentsFromText((const char *)decrypted.toByteArray(), sourcePath);
    inmacro = new HqlLex(yyParser, decryptedContents, NULL, NULL);
    inmacro->setParentLex(this);
    inmacro->encrypted = true;
}

/* Return: true if more parameter(s) left. */
bool HqlLex::getParameter(StringBuffer &curParam, const char* for_what, int* startLine, int* startCol)
{
    unsigned parenDepth = 1;
    if (startLine) *startLine = -1;
    YYSTYPE nextToken;
    for (;;)
    {
        int tok = yyLex(nextToken, false, 0);
        if (startLine && *startLine == -1)
        {
            *startLine = nextToken.pos.lineno;
            *startCol = nextToken.pos.column;
        }
        switch(tok)
        {
        case '(':
        case '[':
            parenDepth++;
            curParam.append((char) tok);
            break;
        case ')':
            if (parenDepth==1)
                return false;
            // fall into
        case ']':
            parenDepth--;
            curParam.append((char) tok);
            break;
        case ',':
            if (parenDepth==1)
                return true;
            curParam.append(',');
            break;
        case EOF:
            {
                StringBuffer msg("EOF encountered while gathering parameters for ");
                msg.append(for_what);
                reportError(nextToken, ERR_TMPLT_EOFINPARAM, "%s", msg.str());
            }
            return false;
        default:
            curParam.append(' ');
            getTokenText(curParam);
            break;
        }
        nextToken.release();
    }
}

void HqlLex::doSkipUntilEnd(YYSTYPE & returnToken, const char * forwhat)
{
    while (skipNesting)
    {
        int tok = yyLex(returnToken, false,0);
        returnToken.release();
        if (tok == EOF)
        {
            StringBuffer msg;
            msg.appendf("Unexpected EOF in %s: #END expected",forwhat);
            reportError(returnToken, ERR_TMPLT_HASHENDEXPECTED, "%s", msg.str());
            clearNestedHash();      // prevent unnecessary more error messages
            break;
        }
    }
}

void HqlLex::doIf(YYSTYPE & returnToken, bool isElseIf)
{
    StringBuffer forwhat;
    int line = returnToken.pos.lineno, col = returnToken.pos.column;
    forwhat.appendf("#IF(%d,%d)",returnToken.pos.lineno,returnToken.pos.column);

    int tok = yyLex(returnToken, false, 0);
    if (tok != '(')
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected"); // MORE - make it fatal!
    StringBuffer curParam("(");
    if (getParameter(curParam, forwhat.str()))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "too many operands");
        StringBuffer dummy;
        while (getParameter(dummy,forwhat.str()))
            ;
    }
    curParam.append(')');
    Owned<IValue> value = parseConstExpression(returnToken, curParam, queryTopXmlScope(),line,col);
    if (value && !value->getBoolValue())
    {
        setHashEndFlags(0);
        skipNesting = 1;
        if (!isElseIf)
            doSkipUntilEnd(returnToken, forwhat);
    }
    else
        setHashEndFlags(HEFhadtrue);
}

int HqlLex::doElse(YYSTYPE & returnToken, bool lookup, const short * activeState, bool isElseIf)
{
    StringBuffer forwhat;
    forwhat.appendf("#%s(%d,%d)",isElseIf ? "ELSEIF" : "ELSE", returnToken.pos.lineno,returnToken.pos.column);

    if ((hashendKinds.ordinality() == 0) || (hashendKinds.tos() != HashStmtIf))
    {
        reportError(returnToken, ERR_TMPLT_EXTRAELSE,"#ELSE does not match a #IF");
        return SKIPPED;
    }

    unsigned flags = hashendFlags.tos();
    if (!isElseIf)
    {
        if (flags & HEFhadelse)
            reportError(returnToken, ERR_TMPLT_EXTRAELSE,"Multiple #ELSE for the same #IF");
        setHashEndFlags(flags|HEFhadelse);
    }

    switch (skipNesting)
    {
    case 0:
        skipNesting = 1;
        doSkipUntilEnd(returnToken, forwhat);
        return yyLex(returnToken, lookup, activeState);
    case 1:
        if (flags & HEFhadtrue)
        {
            //Don't need to do anything
        }
        else
        {
            skipNesting = 0;
            if (isElseIf)
                doIf(returnToken, true);
            else
                setHashEndFlags(HEFhadtrue|HEFhadelse);
        }
        return SKIPPED;     // looks wrong, but called within a doIf() loop, and first return is ignored
    default:
        return SKIPPED;
    }
}

int HqlLex::doEnd(YYSTYPE & returnToken, bool lookup, const short * activeState)
{
    if (hashendKinds.ordinality() != 0)
    {
        endNestedHash();
        if (skipNesting)
        {
            skipNesting -= 1;
            return(HASHEND);
        }
    }
    else
        reportError(returnToken, ERR_TMPLT_EXTRAEND,"#END doesn't match a # command");

    return yyLex(returnToken, lookup, activeState);
}

void HqlLex::doDeclare(YYSTYPE & returnToken)
{
    StringBuffer forwhat;
    forwhat.appendf("#DECLARE(%d,%d)",returnToken.pos.lineno,returnToken.pos.column);

    IIdAtom * name = NULL;
    if (yyLex(returnToken, false,0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }

    for (;;)
    {
        int tok = yyLex(returnToken, false,0);
        if (tok == EOF)
        {
            StringBuffer msg;
            msg.append("Unexpected EOF in ").append(forwhat.str()).append(": ')' expected");
            reportError(returnToken, ERR_TMPLT_HASHENDEXPECTED, "%s", msg.str());
            clearNestedHash();      // prevent unnecessary more error messages
            return;
        }

        if (tok != UNKNOWN_ID)
        {
            reportError(returnToken, ERR_EXPECTED_IDENTIFIER, "Identifier expected");
            returnToken.release();
            continue;
        }

        name = returnToken.getId();
        declareXmlSymbol(returnToken, str(name));

        tok = yyLex(returnToken, false,0);
        if (tok == ')')
            break;
        else if (tok == ',')
            continue;
        else if (tok == EOF)
        {
            StringBuffer msg;
            msg.append("Unexpected EOF in ").append(forwhat.str()).append(": ) expected");
            reportError(returnToken, ERR_TMPLT_HASHENDEXPECTED, "%s", msg.str());
            clearNestedHash();      // prevent unnecessary more error messages
        }
        else
        {
            reportError(returnToken, ERR_EXPECTED, "',' or ')' expected");
            returnToken.release();
        }
    }
}

void HqlLex::doExpand(YYSTYPE & returnToken)
{
    StringBuffer forwhat;
    forwhat.appendf("#DECLARE(%d,%d)",returnToken.pos.lineno,returnToken.pos.column);

    if (yyLex(returnToken, false,0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }

    StringBuffer curParam("(");
    int startLine, startCol;
    if (getParameter(curParam,forwhat.str(),&startLine,&startCol))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "Too many operands");
        StringBuffer dummy;
        while (getParameter(dummy,forwhat.str()))
            ;
    }
    curParam.append(')');
    Owned<IValue> value = parseConstExpression(returnToken, curParam, queryTopXmlScope(),startLine-1,startCol);
    if (value)
    {
        StringBuffer buf;
        value->getStringValue(buf);
        if (buf.length())
            pushText(buf.str());
    }
}

void HqlLex::doSet(YYSTYPE & returnToken, bool append)
{
    StringBuffer forwhat;
    forwhat.appendf("%s(%d,%d)",append?"#APPEND":"#SET",returnToken.pos.lineno,returnToken.pos.column);

    IIdAtom * name = NULL;
    if (yyLex(returnToken, false,0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }
    if (yyLex(returnToken, false,0) != UNKNOWN_ID)
    {
        reportError(returnToken, ERR_EXPECTED_IDENTIFIER, "Identifier expected");
        returnToken.release();
        return;
    }
    name = returnToken.getId();
    if (yyLex(returnToken, false,0) != ',')
    {
        reportError(returnToken, ERR_EXPECTED_COMMA, ", expected");
        return;
    }
    StringBuffer curParam("(");

    int startLine, startCol;
    if (getParameter(curParam,forwhat.str(),&startLine,&startCol))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "Too many operands");
        StringBuffer dummy;
        while (getParameter(dummy,forwhat.str()))
            ;
    }
    curParam.append(')');
    IValue *value = parseConstExpression(returnToken, curParam, queryTopXmlScope(),startLine-1,startCol);
    if (value)
    {
        StringBuffer buf;
        value->getStringValue(buf);
        setXmlSymbol(returnToken, str(name), buf.str(), append);
        value->Release();
    }
}

void HqlLex::doLine(YYSTYPE & returnToken)
{
    StringBuffer forwhat;
    int line = returnToken.pos.lineno, col = returnToken.pos.column;
    forwhat.appendf("LINE(%d,%d)",returnToken.pos.lineno,returnToken.pos.column);

    IIdAtom * name = NULL;
    if (yyLex(returnToken, false,0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }
    StringBuffer curParam("(");
    bool moreParams = getParameter(curParam, forwhat.str(), &line, &col);
    curParam.append(')');

    IValue *value = parseConstExpression(returnToken, curParam, queryTopXmlScope(),line,col);
    if (value && value->getTypeCode()==type_int)
    {
        returnToken.pos.lineno = yyLineNo = (int)value->getIntValue();
    }
    else
        reportError(returnToken, ERR_EXPECTED_CONST, "Constant integer expression expected");
    ::Release(value);

    if (moreParams)
    {
        int startLine, startCol;
        if (getParameter(curParam,forwhat.str(),&startLine,&startCol))
        {
            reportError(returnToken, ERR_OPERANDS_TOOMANY, "Too many operands");
            StringBuffer dummy;
            while (getParameter(dummy,forwhat.str()))
                ;
        }
        curParam.append(')');
        IValue *value = parseConstExpression(returnToken, curParam, queryTopXmlScope(),startLine-1,startCol);
        if (value && value->getTypeCode()==type_string)
        {
            StringBuffer buf;
            value->getStringValue(buf);
            // MORE - set filename here
            value->Release();
        }
        else
            reportError(returnToken, ERR_EXPECTED_CONST, "Constant string expression expected");
    }
}

void HqlLex::doError(YYSTYPE & returnToken, bool isError)
{
    StringBuffer forwhat;
    forwhat.appendf("%s(%d,%d)",isError?"#ERROR":"#WARNING",returnToken.pos.lineno,returnToken.pos.column);

    IIdAtom * name = NULL;
    if (yyLex(returnToken, false,0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }
    StringBuffer curParam("(");
    int startLine, startCol;
    if (getParameter(curParam,forwhat.str(),&startLine,&startCol))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "Too many operands");
        StringBuffer dummy;
        while (getParameter(dummy,forwhat.str()))
            ;
    }
    curParam.append(')');
    StringBuffer buf;
    OwnedIValue value = parseConstExpression(returnToken, curParam, queryTopXmlScope(),startLine-1,startCol);
    if (value)
    {
        value->getStringValue(buf);
    }
    else
        buf.append(curParam.length()-2, curParam.str()+1);
    if (isError)
        reportError(returnToken, ERR_HASHERROR, "#ERROR: %s", buf.str());
    else
        reportWarning(CategoryUnusual, returnToken, WRN_HASHWARNING, "#WARNING: %s", buf.str());
}

void HqlLex::doExport(YYSTYPE & returnToken, bool toXml)
{
    StringBuffer forwhat;
    forwhat.appendf("#EXPORT(%d,%d)",returnToken.pos.lineno,returnToken.pos.column);

    IIdAtom * exportname = NULL;
    if (yyLex(returnToken, false,0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }
    if (yyLex(returnToken, false,0) != UNKNOWN_ID)
    {
        reportError(returnToken, ERR_EXPECTED_IDENTIFIER, "Identifier expected");
        returnToken.release();
        return;
    }
    exportname = returnToken.getId();
    if (yyLex(returnToken, false,0) != ',')
    {
        reportError(returnToken, ERR_EXPECTED_COMMA, ", expected");
        return;
    }
    IPropertyTree *data = createPTree("Data", ipt_caseInsensitive);
    for (;;)
    {
        StringBuffer curParam("SIZEOF(");
        bool more = getParameter(curParam,"#EXPORT");
        curParam.append(",MAX)");

        OwnedHqlExpr expr;
        Owned<IHqlScope> scope = new CHqlParserPseduoScope(yyParser);
        try
        {
            HqlLookupContext ctx(yyParser->lookupCtx);
            Owned<IFileContents> exportContents = createFileContentsFromText(curParam.str(), sourcePath);
            expr.setown(parseQuery(scope, exportContents, ctx, xmlScope, NULL, true));

            if (expr && (expr->getOperator() == no_sizeof))
            {
                IHqlExpression * child = expr->queryChild(0);
                node_operator op = child->getOperator();
                if(op==no_table || op==no_usertable || op==no_newusertable || op == no_record || op == no_select || op == no_field)
                    exportData(data, child, true);
                else if (child->queryRecord())
                    exportData(data, child->queryRecord(), true);
                else
                    reportError(returnToken, ERR_EXPECTED_COMMA, "DATASET or TABLE expression expected");
            }
            else
                reportError(returnToken, ERR_EXPECTED_COMMA, "Could not parse the argument passed to #EXPORT");
        }
        catch (...)
        {
            setXmlSymbol(returnToken, str(exportname), "", false);
            PrintLog("Unexpected exception in doExport()");
        }
        if (!more)
            break;
    }
    StringBuffer buf;
    toXML(data, buf, 0);
    if (toXml)
        ensureTopXmlScope()->loadXML(buf.str(), str(exportname));
    else
        setXmlSymbol(returnToken, str(exportname), buf.str(), false);
    data->Release();
}

void HqlLex::doTrace(YYSTYPE & returnToken)
{
    StringBuffer forwhat;
    forwhat.appendf("#TRACE(%d,%d)",returnToken.pos.lineno,returnToken.pos.column);

    IIdAtom * name = NULL;
    if (yyLex(returnToken, false,0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }
    StringBuffer curParam("(");

    int startLine, startCol;
    if (getParameter(curParam,forwhat.str(),&startLine,&startCol))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "Too many operands");
        StringBuffer dummy;
        while (getParameter(dummy,forwhat.str()))
            ;
    }
    curParam.append(')');
    Owned<IValue> value = parseConstExpression(returnToken, curParam, queryTopXmlScope(),startLine-1,startCol);
    if (value)
    {
        StringBuffer buf;
        value->getStringValue(buf);
        FILE *trace = fopen("hql.log", "at");
        if (trace)
        {
            fwrite(buf.str(),buf.length(),1,trace);
            fclose(trace);
        }
    }
}

void HqlLex::doFor(YYSTYPE & returnToken, bool doAll)
{
    //MTIME_SECTION(timer, "HqlLex::doFor")

    int startLine = -1, startCol = 0;
    StringBuffer forwhat;
    forwhat.appendf("#FOR(%d,%d)",returnToken.pos.lineno,returnToken.pos.column);

    IIdAtom * name = NULL;
    if (yyLex(returnToken, false,0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }
    if (yyLex(returnToken, false,0) != UNKNOWN_ID)
    {
        reportError(returnToken, ERR_EXPECTED_IDENTIFIER, "Identifier expected");
        returnToken.release();
        return;
    }
    name = returnToken.getId();

    StringBuffer forFilterText;
    // Note - we gather the for filter and body in skip mode (deferring evaluation of #if etc) since the context will be different each time...
    skipNesting = 1;
    int tok = yyLex(returnToken, false,0);
    if (tok == '(')
    {
        forFilterText.append('(');
        while (getParameter(forFilterText, forwhat.str()))
            forFilterText.append(") AND (");
        forFilterText.append(')');
        tok = yyLex(returnToken, false,0);
    }
    if (tok != ')')
    {
        reportError(returnToken, ERR_EXPECTED_RIGHTCURLY, ") expected");

        // recovery: assume a ')' is here. And push back the token.
        pushText(get_yyText());
        returnToken.release();
    }

    // Now gather the tokens we are going to repeat...
    StringBuffer forBodyText;
    for (;;)
    {
        int tok = yyLex(returnToken, false,0);
        if (startLine == -1)
        {
            startLine = returnToken.pos.lineno - 1;
            startCol = returnToken.pos.column;
        }

        if (tok == EOF)
        {
            reportError(returnToken, ERR_TMPLT_HASHENDEXPECTED, "EOF encountered inside %s: #END expected", forwhat.str());
            clearNestedHash();      // prevent unnecessary more error messages
            return;
        }
        if (tok == HASHEND && !skipNesting)
            break;
        forBodyText.append(' ');
        getTokenText(forBodyText);
        returnToken.release();
    }
    ::Release(forLoop);

    forLoop = getSubScopes(returnToken, str(name), doAll);
    if (forFilterText.length())
        forFilter.setown(createFileContentsFromText(forFilterText, sourcePath));
    forBody.setown(createFileContentsFromText(forBodyText, sourcePath));

    loopTimes = 0;
    if (forLoop && forLoop->first()) // more - check filter
        checkNextLoop(returnToken, true, startLine, startCol);
}

void HqlLex::doLoop(YYSTYPE & returnToken)
{
    int startLine = -1, startCol = 0;
    StringBuffer forwhat;
    forwhat.appendf("#LOOP(%d,%d)",returnToken.pos.lineno,returnToken.pos.column);


    // Now gather the tokens we are going to repeat...
    StringBuffer forBodyText;
    // Note - we gather the for filter and body in skip mode (deferring evaluation of #if etc) since the context will be different each time...
    skipNesting = 1;
    hasHashbreak = false;
    for (;;)
    {
        int tok = yyLex(returnToken, false,0);
        if (startLine == -1)
        {
            startLine = returnToken.pos.lineno-1;
            startCol = returnToken.pos.column;
        }

        if (tok == EOF)
        {
            reportError(returnToken, ERR_TMPLT_HASHENDEXPECTED, "EOF encountered inside %s: #END expected",forwhat.str());
            clearNestedHash();      // prevent unnecessary more error messages
            return;
        }
        if (tok == HASHEND && !skipNesting)
            break;
        forBodyText.append(' ');
        getTokenText(forBodyText);
        returnToken.release();
    }
    if (!hasHashbreak)
    {
        reportError(returnToken, ERR_TMPLT_NOBREAKINLOOP,"No #BREAK inside %s: infinite loop will occur", forwhat.str());
        return;
    }

    ::Release(forLoop);
    forLoop = new CDummyScopeIterator(ensureTopXmlScope());
    forFilter.clear();
    forBody.setown(createFileContentsFromText(forBodyText, sourcePath));
    loopTimes = 0;
    if (forLoop->first()) // more - check filter
        checkNextLoop(returnToken, true,startLine,startCol);
}

void HqlLex::doGetDataType(YYSTYPE & returnToken)
{
    int tok = yyLex(returnToken, false,0);
    if (tok != '(')
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected"); // MORE - make it fatal!
    StringBuffer curParam("(");
    if (getParameter(curParam, "#GETDATATYPE"))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "too many operands");
        StringBuffer dummy;
        while (getParameter(dummy,"#GETDATATYPE"))
            ;
    }
    curParam.append(')');

    StringBuffer type;
    doGetDataType(type, curParam.str(), returnToken.pos.lineno, returnToken.pos.column);
    pushText(type.str());
}

StringBuffer& HqlLex::doGetDataType(StringBuffer & type, const char * text, int lineno, int column)
{
    OwnedHqlExpr expr = parseECL(text, queryTopXmlScope(), lineno, column);
    if(expr)
    {
        type.append('\'');
        if (expr->queryType())
            expr->queryType()->getECLType(type);
        type.append('\'');
    }
    else
        type.append("'unknown_type'");
    return type;
}

int HqlLex::doHashText(YYSTYPE & returnToken)
{
    StringBuffer forwhat;
    forwhat.appendf("#TEXT(%d,%d)",returnToken.pos.lineno,returnToken.pos.column);

    if (yyLex(returnToken, false,0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        returnToken.setExpr(createConstant(""));
        return STRING_CONST;
    }

    StringBuffer parameterText;
    bool moreParams = getParameter(parameterText, forwhat.str());
    if (!moreParams)
    {
        while (parameterText.length() && parameterText.charAt(0)==' ')
            parameterText.remove(0, 1);
    }
    else
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "Too many operands");
        StringBuffer dummy;
        while (getParameter(dummy,forwhat.str()))
            ;
    }

    returnToken.setExpr(createConstant(parameterText));
    return (STRING_CONST);
}


void HqlLex::doInModule(YYSTYPE & returnToken)
{
#ifdef TIMING_DEBUG
    MTIME_SECTION(timer, "HqlLex::doInModule");
#endif
    int tok = yyLex(returnToken, false,0);
    if (tok != '(')
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");

    StringBuffer moduleName, attrName;

    if (getParameter(moduleName,"#INMODULE"))
    {
        if (getParameter(attrName,"#INMODULE"))
        {
            reportError(returnToken, ERR_OPERANDS_TOOMANY, "too many operands");

            /* skip the rest */
            StringBuffer dummy;
            while (getParameter(dummy,"#INMODULE"))
                ;
        }
    }
    else
    {
        reportError(returnToken, ERR_PARAM_TOOFEW,"Too few parameters: #INMODULE needs 2");
        /* recovery */
        pushText("true");
        return;
    }

    if (isInModule(yyParser->lookupCtx, moduleName.str(),attrName.str()))
        pushText("true");
    else
        pushText("false");
}

static bool isInModule(HqlLookupContext & ctx, const char* moduleName, const char* attrName)
{
    if (!ctx.queryRepository())
        return false;

    try
    {

        //hack: get rid of the extra leading spaces
        const char* pModule = moduleName;
        while(*pModule==' ') pModule++;
        const char* pAttr = attrName;
        while(*pAttr==' ') pAttr++;

        OwnedHqlExpr match = ctx.queryRepository()->queryRootScope()->lookupSymbol(createIdAtom(pModule), LSFpublic, ctx);
        IHqlScope * scope = match ? match->queryScope() : NULL;
        if (scope)
        {
            OwnedHqlExpr expr = scope->lookupSymbol(createIdAtom(pAttr), LSFpublic, ctx);
            if (expr)
                return true;
        }
    }
    catch (...)
    {
        PrintLog("Unexpected exception in doInModule()");
    }

    return false;
}

void HqlLex::doUniqueName(YYSTYPE & returnToken)
{
    int tok = yyLex(returnToken, false,0);
    if (tok != '(')
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
    else
        tok = yyLex(returnToken, false,0);

    if (tok != UNKNOWN_ID)
    {
        reportError(returnToken, ERR_EXPECTED_IDENTIFIER, "Identifier expected");
        returnToken.release();
    }
    else
    {
        IIdAtom * name = returnToken.getId();
        StringAttr pattern("__#__$__");

        tok = yyLex(returnToken, false,0);
        if (tok == ',')
        {
            tok = yyLex(returnToken, false,0);
            if (tok == STRING_CONST)
            {
                StringBuffer text;
                OwnedHqlExpr str = returnToken.getExpr();
                getStringValue(text, str);
                pattern.set(text.str());
                tok = yyLex(returnToken, false,0);
            }
            else
                reportError(returnToken, ERR_EXPECTED, "string expected");
        }
        declareUniqueName(str(name), pattern);
    }

    if (tok != ')')
        reportError(returnToken, ERR_EXPECTED_RIGHTCURLY, ") expected");
}

static int gUniqueId = 0;
void resetLexerUniqueNames() { gUniqueId = 0; }

void HqlLex::declareUniqueName(const char *name, const char * pattern)
{
    IXmlScope *top = ensureTopXmlScope();

    StringBuffer value;
    if (!top->getValue(name,value))
        top->declareValue(name);

    StringBuffer uniqueName;
    bool added = false;
    for (const char * cur = pattern; *cur; cur++)
    {
        char next = *cur;
        switch (next)
        {
        case '#':
            uniqueName.append(name);
            break;
        case '$':
            uniqueName.append(++gUniqueId);
            added = true;
            break;
        default:
            uniqueName.append(next);
            break;
        }
    }
    if (!added)
        uniqueName.append(++gUniqueId);

    //PrintLog("Declaring unique name: %s",uniqueName.str());
    top->setValue(name,uniqueName.str());
}

void HqlLex::doIsValid(YYSTYPE & returnToken)
{
    int tok = yyLex(returnToken, false,0);
    if (tok != '(')
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
    StringBuffer curParam("(");
    if (getParameter(curParam,"#ISVALID"))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "too many operands");
        StringBuffer dummy;
        while (getParameter(dummy,"#ISVALID"))
            ;
    }
    curParam.append(')');

    IHqlExpression * expr = NULL;
    IHqlScope *scope = createScope();

    try
    {
        HqlLookupContext ctx(yyParser->lookupCtx);
        ctx.errs.clear();   //Deliberately ignore any errors
        Owned<IFileContents> contents = createFileContentsFromText(curParam.str(), sourcePath);
        expr = parseQuery(scope, contents, ctx, xmlScope, NULL, true);

        if(expr)
        {
            pushText("true");
        }
        else
        {
            pushText("false");
        }
    }
    catch (...)
    {
        pushText("false");
        PrintLog("Unexpected exception in doIsValid()");
    }

    ::Release(expr);
    ::Release(closeScope(scope));
}


void HqlLex::checkNextLoop(const YYSTYPE & errpos, bool first, int startLine, int startCol)
{
    if (yyParser->aborting)
        return;
    if (loopTimes++ > MAX_LOOP_TIMES)
    {
        reportError(errpos, ERR_TMPLT_LOOPEXCESSMAX,"The loop exceeded the limit: infinite loop is suspected");
        return;
    }
    //printf("%d\r",loopTimes);
    //assertex(forLoop);
    while (first || forLoop->next())
    {
        bool filtered;
        IXmlScope *subscope = (IXmlScope *) &forLoop->query();
        if (forFilter)
        {
#ifdef TIMING_DEBUG
            MTIME_SECTION(timer, "HqlLex::checkNextLoopcond");
#endif
            IValue *value = parseConstExpression(errpos, forFilter, subscope,startLine,startCol);
            filtered = !value || !value->getBoolValue();
            ::Release(value);
        }
        else
            filtered = false;
        if (!filtered)
        {
            pushText(forBody,startLine,startCol);
            inmacro->xmlScope = LINK(subscope);
            return;
        }
        first = false;
    }
    forLoop->Release();
    forLoop = NULL;
}


void HqlLex::doPreprocessorLookup(const YYSTYPE & errpos, bool stringify, int extra)
{
    StringBuffer out;

    char *text = get_yyText() + 1;
    unsigned len = (size32_t)strlen(text) - 1;
    text += extra;
    len -= (extra+extra);

    StringBuffer in;
    in.append(len, text);
    bool matched = lookupXmlSymbol(errpos, in.str(), out);
    if (stringify)
    {
        char *expanded = (char *) malloc(out.length()*2 + 3); // maximum it could be (might be a bit big for alloca)
        char *s = expanded;
        *s++='\'';
        const char *finger = out.str();
        for (;;)
        {
            char c = *finger++;
            if (!c)
                break;
            switch(c)
            {
            case '\r':
                *s++='\\'; *s++ ='r';
                break;
            case '\n':
                *s++='\\'; *s++ ='n';
                break;
            case '\\':
            case '\'':
                *s++='\\';
                // fall into
            default:
                *s++=c;
            }
        }
        *s++ = '\'';
        *s = '\0';
        pushText(expanded);
        free(expanded);
    }
    else
    {
        // a space is needed sometimes, e.g, #IF(true or %x%=2)
        out.trim();
        if (out.length())
        {
            out.insert(0," ");
            pushText(out.str());
        }
        else
        {
            //Don't report errors accessing attributes, but do complain about missing symbols
            if (!matched && (*text != '@'))
                reportError(errpos, WRN_UNRESOLVED_SYMBOL, "Symbol %%%s not resolved", text);
            pushText(" 0");
        }
    }
}


//Read the text of a parameter, but also have a good guess at whether it is defined.
bool HqlLex::getDefinedParameter(StringBuffer &curParam, YYSTYPE & returnToken, const char* for_what, SharedHqlExpr & resolved)
{
    enum { StateStart, StateDot, StateSelectId, StateFailed } state = StateStart;
    unsigned parenDepth = 1;
    OwnedHqlExpr expr;
    for (;;)
    {
        int tok = yyLex(returnToken, false, 0);
        switch(tok)
        {
        case '(':
        case '[':
            parenDepth++;
            break;
        case ')':
            if (parenDepth-- == 1)
            {
                if (state == StateDot)
                    resolved.setown(expr.getClear());
                return false;
            }
            break;
        case ']':
            parenDepth--;
            break;
        case ',':
            if (parenDepth==1)
            {
                if (state == StateDot)
                    resolved.setown(expr.getClear());
                return true;
            }
            break;
        case EOF:
            {
                StringBuffer msg("EOF encountered while gathering parameters for ");
                msg.append(for_what);
                reportError(returnToken, ERR_TMPLT_EOFINPARAM, "%s", msg.str());
            }
            return false;
        case UNKNOWN_ID:
            if (parenDepth == 1)
            {
                switch (state)
                {
                case StateStart:
                    {
                        expr.setown(lookupSymbol(returnToken.getId(), returnToken));
                        state = expr ? StateDot : StateFailed;
                        break;
                    }
                case StateDot:
                    {
                        state = StateFailed;
                        break;
                    }
                case StateSelectId:
                    {
                        state = StateFailed;
                        if (expr->getOperator() == no_funcdef)
                            expr.set(expr->queryChild(0));
                        IHqlScope * scope = expr->queryScope();
                        if (scope)
                        {
                            expr.setown(yyParser->lookupSymbol(scope, returnToken.getId()));
                            if (expr)
                                state = StateDot;
                        }
                        break;
                    }
                }
            }
            curParam.append(' ');
            break;
        case '.':
            if (parenDepth == 1)
            {
                if (state == StateDot)
                    state = StateSelectId;
                else
                    state = StateFailed;
            }
            break;
        default:
            curParam.append(' ');
            break;
        }
        getTokenText(curParam);
        returnToken.release();
    }
}

bool HqlLex::doIsDefined(YYSTYPE & returnToken)
{
    StringBuffer forwhat;
    forwhat.appendf("#ISDEFINED(%d,%d)",returnToken.pos.lineno,returnToken.pos.column);

    if (!assertNextOpenBra())
        return false;

    OwnedHqlExpr resolved;
    StringBuffer paramText;
    bool hasMore = getDefinedParameter(paramText, returnToken, forwhat.str(), resolved);
    if (hasMore)
        reportError(returnToken, ERR_EXPECTED, "Expected ')'");
    return resolved != NULL;
}


void HqlLex::doDefined(YYSTYPE & returnToken)
{
    StringBuffer forwhat;
    forwhat.appendf("#DEFINED(%d,%d)",returnToken.pos.lineno,returnToken.pos.column);

    if (!assertNextOpenBra())
        return;

    OwnedHqlExpr resolved;
    StringBuffer param1Text;
    StringBuffer param2Text;
    bool hasMore = getDefinedParameter(param1Text, returnToken, forwhat.str(), resolved);
    if (hasMore)
        hasMore = getParameter(param2Text, forwhat.str());
    if (hasMore)
        reportError(returnToken, ERR_EXPECTED, "Expected ')'");

    if (resolved)
        pushText(param1Text);
    else if (param2Text.length())
        pushText(param2Text);
}

IHqlExpression *HqlLex::parseECL(IFileContents * contents, IXmlScope *xmlScope, int startLine, int startCol)
{
#ifdef TIMING_DEBUG
    MTIME_SECTION(timer, "HqlLex::parseConstExpression");
#endif
    //  Use an ECL reserved word as the scope name to avoid name conflicts with these defined localscope.
    Owned<IHqlScope> scope = new CHqlMultiParentScope(sharedId,yyParser->queryPrimaryScope(false),yyParser->queryPrimaryScope(true),yyParser->parseScope.get(),NULL);

    HqlGramCtx parentContext(yyParser->lookupCtx, yyParser->inSignedModule);
    yyParser->saveContext(parentContext, false);
    HqlGram parser(parentContext, scope, contents, xmlScope, true);
    parser.getLexer()->set_yyLineNo(startLine);
    parser.getLexer()->set_yyColumn(startCol);
    return parser.yyParse(false, false);
}


IHqlExpression *HqlLex::parseECL(const char * text, IXmlScope *xmlScope, int startLine, int startCol)
{
    Owned<IFileContents> contents = createFileContentsFromText(text, querySourcePath());
    return parseECL(contents, xmlScope, startLine, startCol);
}


IValue *HqlLex::foldConstExpression(const YYSTYPE & errpos, IHqlExpression * expr, IXmlScope *xmlScope, int startLine, int startCol)
{
    OwnedIValue value;
    if (expr)
    {
        try
        {
            CTemplateContext context(this, yyParser->lookupCtx, xmlScope, startLine, startCol);
            OwnedHqlExpr folded = foldHqlExpression(*yyParser, expr, &context, HFOthrowerror|HFOfoldimpure|HFOforcefold);
            if (folded)
            {
                if (folded->queryValue())
                    value.set(folded->queryValue());
            }
        }
        catch (IException* except)
        {
            StringBuffer s;
            reportError(errpos, except->errorCode(), "%s", except->errorMessage(s).str());
            except->Release();
        }
    }

    if (!value.get())
        reportError(errpos, ERR_EXPECTED_CONST, "Constant expression expected"); // errpos could be better

    return value.getClear();
}

IValue *HqlLex::parseConstExpression(const YYSTYPE & errpos, StringBuffer &curParam, IXmlScope *xmlScope, int startLine, int startCol)
{
#ifdef TIMING_DEBUG
    MTIME_SECTION(timer, "HqlLex::parseConstExpression");
#endif
    OwnedHqlExpr expr = parseECL(curParam, xmlScope, startLine, startCol);
    return foldConstExpression(errpos, expr, xmlScope, startLine, startCol);
}

IValue *HqlLex::parseConstExpression(const YYSTYPE & errpos, IFileContents * text, IXmlScope *xmlScope, int startLine, int startCol)
{
#ifdef TIMING_DEBUG
    MTIME_SECTION(timer, "HqlLex::parseConstExpression");
#endif
    OwnedHqlExpr expr = parseECL(text, xmlScope, startLine, startCol);
    return foldConstExpression(errpos, expr, xmlScope, startLine, startCol);
}

int hexchar(char c)
{
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    else if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    else
        return c - '0';
}

void HqlLex::doApply(YYSTYPE & returnToken)
{
    int tok = yyLex(returnToken, false,0);
    int line = returnToken.pos.lineno, col = returnToken.pos.column;
    if (tok != '(')
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected"); // MORE - make it fatal!
    StringBuffer curParam("(");
    if (getParameter(curParam, "#APPLY"))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "too many operands");
        StringBuffer dummy;
        while (getParameter(dummy, "#APPLY"))
            ;
    }
    curParam.append(')');
    OwnedHqlExpr actions = parseECL(curParam, queryTopXmlScope(), line, col);
    if (actions)
    {
        CTemplateContext context(this, yyParser->lookupCtx, xmlScope,line,col);
        OwnedHqlExpr folded = foldHqlExpression(*yyParser, actions, &context, HFOthrowerror|HFOfoldimpure|HFOforcefold);
    }
    else
        reportError(returnToken, ERR_EXPECTED_CONST, "Constant expression expected");
}

void HqlLex::doMangle(YYSTYPE & returnToken, bool de)
{
    int tok = yyLex(returnToken, false,0);
    int line = returnToken.pos.lineno, col = returnToken.pos.column;
    if (tok != '(')
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected"); // MORE - make it fatal!
    StringBuffer curParam("(");
    if (getParameter(curParam, de?"#DEMANGLE":"#MANGLE"))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "too many operands");
        StringBuffer dummy;
        while (getParameter(dummy, de?"#DEMANGLE":"MANGLE"))
            ;
    }
    curParam.append(')');
    IValue *value = parseConstExpression(returnToken, curParam, queryTopXmlScope(), line, col);
    if (value)
    {
        const char *str = value->getStringValue(curParam.clear());
        value->Release();

        StringBuffer mangled;
        mangle(yyParser->errorHandler,str,mangled,de);
        pushText(mangled.str());
    }
    else
        reportError(returnToken, ERR_EXPECTED_CONST, "Constant expression expected");
}

static StringBuffer& mangle(IErrorReceiver* errReceiver,const char* src, StringBuffer& mangled,bool de)
{
    mangled.append("'");

    for (const char *finger = src; *finger!=0; finger++)
    {
        unsigned char c = *finger;
        if (isalnum(c))
        {
            if (finger == src && isdigit(c)) // a leading digit
            {
                if (de)
                {
                    //errReceiver->reportError(returnToken, ERR_EXPECTED_CONST, "Bad parameter to #DEMANGLE", "CppTemplate");
                    PrintLog("Bad parameter to #DEMANGLE");
                    break;
                }
                else
                    mangled.appendf("_%02x",(int)c);
            }
            else
                mangled.append(c);
        }
        else if (de)
        {
            if (c != '_')
            {
                //errReceiver->reportError(returnToken, ERR_EXPECTED_CONST, "Bad parameter to #DEMANGLE");
                PrintLog("Bad parameter to #DEMANGLE");
                break;
            }
            c = hexchar(finger[1])*16 + hexchar(finger[2]);
            finger += 2;
            if (c=='\'')
                mangled.append('\\');
            mangled.append(c);
        }
        else
            mangled.appendf("_%02x", (int) c);
    }
    mangled.append('\'');

    return mangled;
}

bool HqlLex::checkUnicodeLiteral(char const * str, unsigned length, unsigned & ep, StringBuffer & msg)
{
    unsigned i;
    for(i = 0; i < length; i++)
    {
        if (str[i] == '\\')
        {
            unsigned char next = str[++i];
            if (next == '\'' || next == '\\' || next == 'n' || next == 'r' || next == 't' || next == 'a' || next == 'b' || next == 'f' || next == 'v' || next == '?' || next == '"')
            {
                continue;
            }
            else if (isdigit(next) && next < '8')
            {
                unsigned count;
                for(count = 1; count < 3; count++)
                {
                    next = str[++i];
                    if(!isdigit(next) || next >= '8')
                    {
                        msg.append("3-digit numeric escape sequence contained non-octal digit: ").append(next);
                        ep = i;
                        return false;
                    }
                }
            }
            else if (next == 'u' || next == 'U')
            {
                unsigned count;
                unsigned max = (next == 'u') ? 4 : 8;
                for(count = 0; count < max; count++)
                {
                    next = str[++i];
                    if(!isdigit(next) && (!isalpha(next) || tolower(next) > 'f'))
                    {
                        msg.append((max == 4) ? '4' : '8').append("-digit unicode escape sequence contained non-hex digit: ").append(next);
                        ep = i;
                        return false;
                    }
                }
            }
            else
            {
                msg.append("Unrecognized escape sequence: ").append("\\").append(next);
                ep = i;
                return false;
            }
        }
    }
    return true;
}

//====================================== Error Reporting  ======================================

bool HqlLex::isAborting()
{
    return yyParser->isAborting();
}

void HqlLex::reportError(const YYSTYPE & returnToken, int errNo, const char *format, ...)
{
    if (yyParser)
    {
        va_list args;
        va_start(args, format);
        yyParser->reportErrorVa(errNo, returnToken.pos, format, args);
        va_end(args);
    }
}

void HqlLex::reportWarning(WarnErrorCategory category, const YYSTYPE & returnToken, int warnNo, const char *format, ...)
{
    if (yyParser)
    {
        va_list args;
        va_start(args, format);
        yyParser->reportWarningVa(category, warnNo, returnToken, format, args);
        va_end(args);
    }
}

//====================================== XML DB =============================================
IXmlScope *HqlLex::queryTopXmlScope()
{
    IXmlScope *top = NULL;
    HqlLex *inlex = this;
    while (inlex->inmacro)
        inlex = inlex->inmacro;

    while (inlex && !top)
    {
        top = inlex->xmlScope;
        inlex = inlex->parentLex;
    }
    return top;
}

IXmlScope *HqlLex::ensureTopXmlScope()
{
    IXmlScope *top = queryTopXmlScope();
    if (!top)
    	top = xmlScope = createXMLScope();
    return top;
}

 bool HqlLex::lookupXmlSymbol(const YYSTYPE & errpos, const char *name, StringBuffer &ret)
{
    if (*name==0)
        name=NULL;

    IXmlScope *top = ensureTopXmlScope();
    bool idFound = top->getValue(name, ret);
    if (idFound)
        return true;

    HqlLex * lexer = parentLex;
    while (lexer && !idFound)
    {
    	if (lexer->xmlScope)
            idFound = lexer->xmlScope->getValue(name, ret);
    	lexer = lexer->parentLex;
    }

    return idFound;
}

void HqlLex::setXmlSymbol(const YYSTYPE & errpos, const char *name, const char *value, bool append)
{
    IXmlScope *top = ensureTopXmlScope();
    bool ok;
    if (append)
        ok = top->appendValue(name, value);
    else
        ok = top->setValue(name, value);

    if (!ok)
    {
        StringBuffer msg("Symbol has not been declared: ");
        msg.append(name);
        reportError(errpos, ERR_TMPLT_SYMBOLNOTDECLARED, "%s", msg.str());
    }
}

void HqlLex::declareXmlSymbol(const YYSTYPE & errpos, const char *name)
{
    IXmlScope *top = ensureTopXmlScope();
    if (!top->declareValue(name))
    {
        StringBuffer msg("Symbol has already been declared: ");
        msg.append(name);
        reportError(errpos, ERR_TMPLT_SYMBOLREDECLARE, "%s", msg.str());
    }
}

IIterator *HqlLex::getSubScopes(const YYSTYPE & errpos, const char *name, bool doAll)
{
    IXmlScope *top = ensureTopXmlScope();
    return top->getScopes(name, doAll);
}

void HqlLex::loadXML(const YYSTYPE & errpos, const char *name, const char * child)
{
    if (xmlScope && child)
    {
        xmlScope->loadXML(name, child);
        return;
    }

    if (false && inmacro)
    {
        inmacro->loadXML(errpos, name);
        return;
    }

    // MORE - give an error if an XML scope is active...
    ::Release(xmlScope);
    try
    {
        xmlScope = ::loadXML(name);
    }
    catch (IException* e)
    {
        e->Release();
        xmlScope = NULL;
    }
    catch (...)
    {
        xmlScope = NULL;
    }

    if (!xmlScope)
    {
        if (name && strlen(name))
        {
            StringBuffer msg;
            msg.appendf("Load XML(\'%s\') failed",name);
            reportError(errpos, ERR_TMPLT_LOADXMLFAILED, "%s", msg.str());
        }

        // recovery: create a default XML scope
        xmlScope = createXMLScope();
    }
}

IPropertyTree * HqlLex::getClearJavadoc()
{
    if (javaDocComment.length() == 0)
        return NULL;

    IPropertyTree * tree = createPTree("javadoc");
    extractJavadoc(tree, javaDocComment.str());
    javaDocComment.clear();
    return tree;
}

unsigned HqlLex::getTypeSize(unsigned lengthTypeName)
{
    const char * tok = get_yyText();
    if (strlen(tok)> lengthTypeName)
        return atoi(tok + lengthTypeName);
    return UNKNOWN_LENGTH;
}

void HqlLex::enterEmbeddedMode()
{
    doEnterEmbeddedMode(scanner);
    inCpp = true;
}

int HqlLex::yyLex(YYSTYPE & returnToken, bool lookup, const short * activeState)
{
    loop
    {
        while (inmacro)
        {
            int ret = inmacro->yyLex(returnToken, lookup, activeState);
            if (ret > 0 && ret != HASHBREAK)
            {
                lastToken = ret;
                return ret;
            }

#if defined(TRACE_MACRO)
            PrintLog("MACRO>> inmacro %p deleted\n", inmacro);
#endif

            delete inmacro;
            inmacro = NULL;

            if (ret == HASHBREAK)
            {
                if (forLoop)
                {
                    forLoop->Release();
                    forLoop = NULL;
                }
                else
                {
                    lastToken = ret;
                    return ret;
                }
            }

            if (forLoop)
                checkNextLoop(returnToken, false,0,0);
        }

        returnToken.clear();

        yyStartPos = yyPosition;
        int ret = doyyFlex(returnToken, scanner, this, lookup, activeState);
        if (ret == 0) ret = EOF;
        if (ret == INTERNAL_READ_NEXT_TOKEN)
            continue;
        if (ret == EOF)
        {
            setTokenPosition(returnToken);
            if (inComment)
                reportError(returnToken, ERR_COMMENT_UNENDED,"Comment is not terminated");
            else if (inSignature)
                reportError(returnToken, ERR_COMMENT_UNENDED,"Signature is not terminated");
            else if (inCpp)
                reportError(returnToken, ERR_COMMENT_UNENDED,"BEGINC++ or EMBED is not terminated");
            if (hashendKinds.ordinality())
            {
                StringBuffer msg("Unexpected EOF: ");
                msg.append(hashendKinds.ordinality()).append(" more #END needed");
                reportError(returnToken, ERR_TMPLT_HASHENDEXPECTED, "%s", msg.str());
                clearNestedHash();
            }
        }

        lastToken = ret;
        return ret;
    }
}


