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
#include "eclrtl.hpp"
#include "codesigner.hpp"

//#define TIMING_DEBUG

#define  MAX_LOOP_TIMES 250000

// =========================== local helper functions ===================================

static bool isInModule(HqlLookupContext & ctx, const char* module_name, const char* attr_name);
static StringBuffer& mangle(IErrorReceiver* errReceiver,const char* src, StringBuffer& mangled,bool demangle);
static const char * skipws(const char * str)
{
    while (isspace(*str))
        str++;
    return str;
}

// =========================== CDummyScopeIterator ======================================

class CDummyScopeIterator : public IIterator, public CInterface
{
    Linked<IXmlScope> parent;
public:
    IMPLEMENT_IINTERFACE;
    CDummyScopeIterator(IXmlScope *_parent) : parent(_parent)
    {
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
 : yyParser(parser), sourcePath(contents->querySourcePath()), xmlScope(LINK(_xmlScope)), macroExpr(_macroExpr)
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
    inMultiString = false;
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

bool HqlLex::assertNext(attribute & returnToken, int expected, unsigned code, const char * msg)
{
    //Pass LEXnone since only used for simple punctuation
    if (yyLex(returnToken, LEXnone, 0) != expected)
    {
        reportError(returnToken, code, "%s", msg);
        returnToken.release();
        return false;
    }
    return true;
}

bool HqlLex::assertNextOpenBra()
{
    attribute tempToken;
    return assertNext(tempToken, '(', ERR_EXPECTED_LEFTCURLY, "( expected");
}

bool HqlLex::assertNextComma()
{
    attribute tempToken;
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

__uint64 HqlLex::str2uint64(unsigned len, const char * digits, unsigned base)
{
  __uint64 value = 0;
  while (len--)
  {
    char c = *digits++;
    value = value * base + hex2digit(c);
  }
  return value;
}

void HqlLex::hex2str(char * target, const char * digits, unsigned len)
{
  if (len & 1)
  {
    //If there are an odd number of digits, process the odd digit as a leading digit.
    *target++ = hex2digit(*digits++);
    len--;
  }

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
    bool useLegacyImport = hasLegacyImportSemantics();
    bool useLegacyWhen = hasLegacyWhenSemantics();
    inmacro = new HqlLex(yyParser, text, NULL, NULL);
    inmacro->setLegacyImport(useLegacyImport);
    inmacro->setLegacyWhen(useLegacyWhen);
    inmacro->set_yyLineNo(startLineNo);
    inmacro->set_yyColumn(startColumn);
}


void HqlLex::pushText(const char *s, int startLineNo, int startColumn)
{
    Owned<IFileContents> macroContents = createFileContentsFromText(s, sourcePath, yyParser->inSignedModule, yyParser->gpgSignature, 0);
    pushText(macroContents, startLineNo, startColumn);
}


void HqlLex::pushText(const char *s)
{
#ifdef TIMING_DEBUG
    MTIME_SECTION(timer, "HqlLex::pushText");
#endif
    Owned<IFileContents> macroContents = createFileContentsFromText(s, sourcePath, yyParser->inSignedModule, yyParser->gpgSignature, 0);
    bool useLegacyImport = hasLegacyImportSemantics();
    bool useLegacyWhen = hasLegacyWhenSemantics();
    inmacro = new HqlLex(yyParser, macroContents, NULL, NULL);
    inmacro->setLegacyImport(useLegacyImport);
    inmacro->setLegacyWhen(useLegacyWhen);
    inmacro->set_yyLineNo(yyLineNo);
    inmacro->set_yyColumn(yyColumn);

#if defined (TRACE_MACRO)
    DBGLOG("MACRO>> inmacro %p created for \"%s\" for macro parameters.\n",inmacro,s);
#endif
}

bool HqlLex::hasLegacyImportSemantics() const
{
    if (inmacro)
        return inmacro->hasLegacyImportSemantics();
    return legacyImportMode;
}

bool HqlLex::hasLegacyWhenSemantics() const
{
    if (inmacro)
        return inmacro->hasLegacyWhenSemantics();
    return legacyWhenMode;
}

void HqlLex::setMacroParam(const attribute & errpos, IHqlExpression* funcdef, StringBuffer& curParam, IIdAtom * argumentId, unsigned& parmno,IProperties *macroParms)
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
    attribute nextToken;
    int tok = yyLex(nextToken, LEXnone, 0);
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
        tok = yyLex(nextToken, LEXidentifier|LEXexpand, 0);
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
                    //DBGLOG("Set macro parm: %s", curParam.str());
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
    IFileContents * macroContents = static_cast<IFileContents *>(macroBodyExpr->queryUnknownExtra(0));
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
        bool useLegacyImport = hasLegacyImportSemantics();
        bool useLegacyWhen = hasLegacyWhenSemantics();
        inmacro = new HqlLex(yyParser, macroContents, NULL, LINK(expr));
        inmacro->setLegacyImport(useLegacyImport);
        inmacro->setLegacyWhen(useLegacyWhen);

#if defined(TRACE_MACRO)
        DBGLOG("MACRO>> inmacro %p created for \"%s\" at %d:%d\n",inmacro, s.str(),macroBodyExpr->getStartLine(),macroBodyExpr->getStartColumn());
//      DBGLOG("MACRO>> macro called at %d:%d\n", expr->getStartLine(),expr->getStartColumn());
#endif
        /* set the lineno and column in the original source as the starting point */
        inmacro->yyLineNo = macroBodyExpr->getStartLine();
        inmacro->yyColumn = macroBodyExpr->getStartColumn();
        inmacro->setParentLex(this);
        inmacro->macroParms.setown(macroParms.getClear());
        inmacro->hashDollar = macroBodyExpr->queryBody()->queryName();
        inmacro->hashDollarPackage = static_cast<IEclPackage *>(macroBodyExpr->queryUnknownExtra(1));
    }
}

void HqlLex::checkSignature(const attribute & dummyToken)
{
    if (yyParser->lookupCtx.queryParseContext().ignoreSignatures)
        return;
    try
    {
        StringBuffer signer;
        if (!queryCodeSigner().verifySignature(text->getText(), signer))
            throw makeStringExceptionV(MSGAUD_user, CODESIGNER_ERR_VERIFY, "Code sign verify: signature not verified");

        yyParser->gpgSignature.setown(createExprAttribute(_signed_Atom,createConstant(signer.str())));
        yyParser->inSignedModule = true;
    }
    catch (IException *e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        reportWarning(CategorySecurity, dummyToken, WRN_SECURITY_SIGNERROR, "%s", msg.str());
        e->Release();
    }
}

/* Read encrypted syntax, and push the decrypted text as a macro. */
void HqlLex::processEncrypted()
{
    attribute nextToken;
    if (yyLex(nextToken, LEXnone, 0) != '(')
    {
        reportError(nextToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        nextToken.release();
        return;
    }
    StringBuffer encoded64;
    for (;;)
    {
        if (yyLex(nextToken, LEXstring, 0) != STRING_CONST)
        {
            reportError(nextToken, ERR_EXPECTED, "String expected");
            nextToken.release();
            return;
        }

        OwnedHqlExpr str = nextToken.getExpr();
        getStringValue(encoded64, str);

        int next = yyLex(nextToken, LEXnone, 0);
        if (next == ')')
            break;
        if (next != ',')
        {
            reportError(nextToken, ERR_EXPECTED_COMMA, ", expected");
            nextToken.release();
            return;
        }
    }
    if (yyLex(nextToken, LEXnone, 0) != ';')
    {
        reportError(nextToken, ERR_EXPECTED, "; expected");
        nextToken.release();
        return;
    }

    MemoryBuffer decrypted;
    decryptEclAttribute(decrypted, encoded64.str());
    decrypted.append(0);    // add a null terminator to the string...
    Owned<ISourcePath> sourcePath = createSourcePath("<encrypted>");
    Owned<IFileContents> decryptedContents = createFileContentsFromText((const char *)decrypted.toByteArray(), sourcePath, yyParser->inSignedModule, yyParser->gpgSignature, text->getTimeStamp());
    bool useLegacyImport = hasLegacyImportSemantics();
    bool useLegacyWhen = hasLegacyWhenSemantics();
    inmacro = new HqlLex(yyParser, decryptedContents, NULL, NULL);
    inmacro->setLegacyImport(useLegacyImport);
    inmacro->setLegacyWhen(useLegacyWhen);
    inmacro->setParentLex(this);
    inmacro->encrypted = true;
}

/* Return: true if more parameter(s) left. */
bool HqlLex::getParameter(StringBuffer &curParam, const char* directive, const ECLlocation & location)
{
    unsigned parenDepth = 1;
    attribute nextToken;
    for (;;)
    {
        int tok = yyLex(nextToken, LEXnone, 0);
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
            // fallthrough
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
                reportError(location, ERR_TMPLT_EOFINPARAM, "EOF encountered while gathering parameters for %s", directive);
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

void HqlLex::doSkipUntilEnd(attribute & returnToken, const char* directive, const ECLlocation & location)
{
    while (skipNesting)
    {
        int tok = yyLex(returnToken, LEXnone, 0);
        returnToken.release();
        if (tok == EOF)
        {
            reportError(location, ERR_TMPLT_HASHENDEXPECTED, "Unexpected EOF in %s: #END expected", directive);
            clearNestedHash();      // prevent unnecessary more error messages
            break;
        }
    }
}

void HqlLex::doIf(attribute & returnToken, bool isElseIf)
{
    ECLlocation location = returnToken.pos;
    const char * directive = "#IF";

    int tok = yyLex(returnToken, LEXnone, 0);
    if (tok != '(')
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected"); // MORE - make it fatal!

    StringBuffer curParam("(");
    if (getParameter(curParam, directive, location))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "too many operands");
        StringBuffer dummy;
        while (getParameter(dummy, directive, location))
            ;
    }
    curParam.append(')');
    Owned<IValue> value = parseConstExpression(location, curParam, queryTopXmlScope());
    if (value && !value->getBoolValue())
    {
        setHashEndFlags(0);
        skipNesting = 1;
        if (!isElseIf)
            doSkipUntilEnd(returnToken, directive, location);
    }
    else
        setHashEndFlags(HEFhadtrue);
}

int HqlLex::doElse(attribute & returnToken, LexerFlags lookupFlags, const short * activeState, bool isElseIf)
{
    ECLlocation location = returnToken.pos;
    const char * directive = isElseIf ? "#ELSEIF" : "#ELSE";

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
        doSkipUntilEnd(returnToken, directive, location);
        return yyLex(returnToken, lookupFlags, activeState);
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

int HqlLex::doEnd(attribute & returnToken, LexerFlags lookupFlags, const short * activeState)
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

    return yyLex(returnToken, lookupFlags, activeState);
}

void HqlLex::doDeclare(attribute & returnToken)
{
    ECLlocation location = returnToken.pos;
    IIdAtom * name = NULL;
    if (yyLex(returnToken, LEXnone, 0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }

    for (;;)
    {
        int tok = yyLex(returnToken, LEXidentifier, 0);
        if (tok == EOF)
        {
            reportError(location, ERR_TMPLT_HASHENDEXPECTED, "Unexpected EOF in #DECLARE: ')' expected");
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

        tok = yyLex(returnToken, LEXnone, 0);
        if (tok == ')')
            break;
        else if (tok == ',')
            continue;
        else if (tok == EOF)
        {
            reportError(location, ERR_TMPLT_HASHENDEXPECTED, "Unexpected EOF in #DECLARE: ')' expected");
            clearNestedHash();      // prevent unnecessary more error messages
        }
        else
        {
            reportError(returnToken, ERR_EXPECTED, "',' or ')' expected");
            returnToken.release();
        }
    }
}

void HqlLex::doExpand(attribute & returnToken)
{
    ECLlocation location = returnToken.pos;
    const char * directive = "#EXPAND";

    if (yyLex(returnToken, LEXnone, 0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }

    StringBuffer curParam("(");
    if (getParameter(curParam, directive, location))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "Too many operands");
        StringBuffer dummy;
        while (getParameter(dummy, directive, location))
            ;
    }
    curParam.append(')');
    Owned<IValue> value = parseConstExpression(returnToken.pos, curParam, queryTopXmlScope());
    if (value)
    {
        StringBuffer buf;
        value->getUTF8Value(buf);
        if (buf.length())
            pushText(buf.str());
    }
}

void HqlLex::doSet(attribute & returnToken, bool append)
{
    ECLlocation location = returnToken.pos;
    const char * directive = append ? "#APPEND" : "#SET";

    IIdAtom * name = NULL;
    if (yyLex(returnToken, LEXnone, 0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }
    if (yyLex(returnToken, LEXidentifier, 0) != UNKNOWN_ID)
    {
        reportError(returnToken, ERR_EXPECTED_IDENTIFIER, "Identifier expected");
        returnToken.release();
        return;
    }
    name = returnToken.getId();
    if (yyLex(returnToken, LEXnone,0) != ',')
    {
        reportError(returnToken, ERR_EXPECTED_COMMA, ", expected");
        return;
    }

    StringBuffer curParam("(");
    if (getParameter(curParam, directive, location))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "Too many operands");
        StringBuffer dummy;
        while (getParameter(dummy, directive, location))
            ;
    }
    curParam.append(')');
    IValue *value = parseConstExpression(returnToken.pos, curParam, queryTopXmlScope());
    if (value)
    {
        StringBuffer buf;
        value->getStringValue(buf);
        setXmlSymbol(returnToken, str(name), buf.str(), append);
        value->Release();
    }
}

void HqlLex::doLine(attribute & returnToken)
{
    ECLlocation location = returnToken.pos;
    const char * directive = "#LINE";

    if (yyLex(returnToken, LEXnone, 0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }
    StringBuffer curParam("(");
    bool moreParams = getParameter(curParam, directive, location);
    curParam.append(')');

    IValue *value = parseConstExpression(returnToken.pos, curParam, queryTopXmlScope());
    if (value && value->getTypeCode()==type_int)
    {
        returnToken.pos.lineno = yyLineNo = (int)value->getIntValue();
    }
    else
        reportError(returnToken, ERR_EXPECTED_CONST, "Constant integer expression expected");
    ::Release(value);

    if (moreParams)
    {
        ECLlocation extrapos;
        getPosition(extrapos);

        if (getParameter(curParam, directive, location))
        {
            reportError(extrapos, ERR_OPERANDS_TOOMANY, "Too many operands");
            StringBuffer dummy;
            while (getParameter(dummy, directive, location))
                ;
        }
        curParam.append(')');
        IValue *value = parseConstExpression(extrapos, curParam, queryTopXmlScope());
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


void HqlLex::doSlashSlashHash(attribute const & returnToken, const char * command)
{
    if (inmacro)
    {
        inmacro->doSlashSlashHash(returnToken, command);
        return;
    }

    if (macroGathering)
        return;

    if (hasPrefix(command, "import", false))
    {
        const char * next = skipws(command + 6);
        if (*next == '(')
        {
            next = skipws(next + 1);
            const char * bra = strchr(next, ')');
            if (bra)
            {
                StringBuffer option(bra - next, next);
                option.clip();
                if (strieq(option, "legacy"))
                    setLegacyImport(true);
                else if (strieq(option, "modern"))
                    setLegacyImport(false);
                else
                    reportError(returnToken, ERR_EXPECTED_ID, "Unknown #import option '%s' - expected legacy or modern", option.str());
            }
            else
                reportError(returnToken, ERR_EXPECTED_RIGHTCURLY, "Expected closing )");
        }
        else
            reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "Expected (");
    }
    else if (hasPrefix(command, "when", false))
    {
        const char * next = skipws(command + 4);
        if (*next == '(')
        {
            next = skipws(next + 1);
            const char * bra = strchr(next, ')');
            if (bra)
            {
                StringBuffer option(bra - next, next);
                option.clip();
                if (strieq(option, "legacy"))
                    setLegacyWhen(true);
                else if (strieq(option, "modern"))
                    setLegacyWhen(false);
                else
                    reportError(returnToken, ERR_EXPECTED_ID, "Unknown #import option '%s' - expected legacy or modern", option.str());
            }
            else
                reportError(returnToken, ERR_EXPECTED_RIGHTCURLY, "Expected closing )");
        }
        else
            reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "Expected (");
    }
    //Ignore any unrecognised commands
}

void HqlLex::doError(attribute & returnToken, bool isError)
{
    ECLlocation location = returnToken.pos;
    const char * directive = isError ? "#ERROR" : "#WARNING";

    if (yyLex(returnToken, LEXnone, 0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }
    StringBuffer curParam("(");
    if (getParameter(curParam, directive, location))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "Too many operands");
        StringBuffer dummy;
        while (getParameter(dummy, directive, location))
            ;
    }
    curParam.append(')');
    StringBuffer buf;
    OwnedIValue value = parseConstExpression(returnToken.pos, curParam, queryTopXmlScope());
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

void HqlLex::doExport(attribute & returnToken, bool toXml)
{
    ECLlocation location = returnToken.pos;
    const char * directive = "#EXPORT";

    IIdAtom * exportname = NULL;
    if (yyLex(returnToken, LEXnone, 0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }
    if (yyLex(returnToken, LEXidentifier, 0) != UNKNOWN_ID)
    {
        reportError(returnToken, ERR_EXPECTED_IDENTIFIER, "Identifier expected");
        returnToken.release();
        return;
    }
    exportname = returnToken.getId();
    if (yyLex(returnToken, LEXnone,0) != ',')
    {
        reportError(returnToken, ERR_EXPECTED_COMMA, ", expected");
        return;
    }
    IPropertyTree *data = createPTree("Data", ipt_caseInsensitive);
    for (;;)
    {
        StringBuffer curParam("SIZEOF(");
        bool more = getParameter(curParam, directive, location);
        curParam.append(",MAX)");

        OwnedHqlExpr expr;
        Owned<IHqlScope> scope = new CHqlParserPseduoScope(yyParser);
        try
        {
            HqlLookupContext ctx(yyParser->lookupCtx);
            Owned<IFileContents> exportContents = createFileContentsFromText(curParam.str(), sourcePath, yyParser->inSignedModule, yyParser->gpgSignature, 0);
            expr.setown(parseQuery(scope, exportContents, ctx, xmlScope, NULL, true, false));

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
            IERRLOG("Unexpected exception in doExport()");
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

void HqlLex::doTrace(attribute & returnToken)
{
    ECLlocation location = returnToken.pos;
    const char * directive = "#TRACE";

    if (yyLex(returnToken, LEXnone, 0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }
    StringBuffer curParam("(");

    if (getParameter(curParam, directive, location))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "Too many operands");
        StringBuffer dummy;
        while (getParameter(dummy, directive, location))
            ;
    }
    curParam.append(')');
    Owned<IValue> value = parseConstExpression(returnToken.pos, curParam, queryTopXmlScope());
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

void HqlLex::doFor(attribute & returnToken, bool doAll)
{
    ECLlocation location = returnToken.pos;
    const char * directive = "#FOR";

    IIdAtom * name = NULL;
    if (yyLex(returnToken, LEXnone, 0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        return;
    }
    if (yyLex(returnToken, LEXidentifier, 0) != UNKNOWN_ID)
    {
        reportError(returnToken, ERR_EXPECTED_IDENTIFIER, "Identifier expected");
        returnToken.release();
        return;
    }
    name = returnToken.getId();

    StringBuffer forFilterText;
    // Note - we gather the for filter and body in skip mode (deferring evaluation of #if etc) since the context will be different each time...
    skipNesting = 1;
    int tok = yyLex(returnToken, LEXnone,0);
    if (tok == '(')
    {
        forFilterText.append('(');
        while (getParameter(forFilterText, directive, location))
            forFilterText.append(") AND (");
        forFilterText.append(')');
        tok = yyLex(returnToken, LEXnone, 0);
    }
    if (tok != ')')
    {
        reportError(returnToken, ERR_EXPECTED_RIGHTCURLY, ") expected");

        // recovery: assume a ')' is here. And push back the token.
        pushText(get_yyText());
        returnToken.release();
    }
    ECLlocation loopPos = returnToken.pos;

    // Now gather the tokens we are going to repeat...
    StringBuffer forBodyText;
    for (;;)
    {
        int tok = yyLex(returnToken, LEXnone,0);

        if (tok == EOF)
        {
            reportError(location, ERR_TMPLT_HASHENDEXPECTED, "EOF encountered inside %s: #END expected", directive);
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

    forLocation = loopPos;
    forLoop = getSubScopes(returnToken, str(name), doAll);
    if (forFilterText.length())
        forFilter.setown(createFileContentsFromText(forFilterText, sourcePath, yyParser->inSignedModule, yyParser->gpgSignature, 0));
    forBody.setown(createFileContentsFromText(forBodyText, sourcePath, yyParser->inSignedModule, yyParser->gpgSignature, 0));

    loopTimes = 0;
    if (forLoop && forLoop->first()) // more - check filter
        checkNextLoop(true);
}

void HqlLex::doLoop(attribute & returnToken)
{
    ECLlocation location = returnToken.pos;
    const char * directive = "#LOOP";

    // Now gather the tokens we are going to repeat...
    StringBuffer forBodyText;
    // Note - we gather the for filter and body in skip mode (deferring evaluation of #if etc) since the context will be different each time...
    skipNesting = 1;
    hasHashbreak = false;
    for (;;)
    {
        int tok = yyLex(returnToken, LEXnone,0);

        if (tok == EOF)
        {
            reportError(location, ERR_TMPLT_HASHENDEXPECTED, "EOF encountered inside #LOOP: #END expected");
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
        reportError(location, ERR_TMPLT_NOBREAKINLOOP, "No #BREAK inside %s: infinite loop will occur", directive);
        return;
    }

    ::Release(forLoop);
    forLocation = location;
    forLoop = new CDummyScopeIterator(ensureTopXmlScope());
    forFilter.clear();
    forBody.setown(createFileContentsFromText(forBodyText, sourcePath, yyParser->inSignedModule, yyParser->gpgSignature, 0));
    loopTimes = 0;
    if (forLoop->first()) // more - check filter
        checkNextLoop(true);
}

void HqlLex::doGetDataType(attribute & returnToken)
{
    ECLlocation location = returnToken.pos;
    const char * directive = "#GETDATATYPE";

    int tok = yyLex(returnToken, LEXnone, 0);
    if (tok != '(')
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected"); // MORE - make it fatal!
    StringBuffer curParam("(");
    if (getParameter(curParam, directive, location))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "too many operands");
        StringBuffer dummy;
        while (getParameter(dummy, directive, location))
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

int HqlLex::doHashText(attribute & returnToken)
{
    ECLlocation location = returnToken.pos;
    const char * directive = "#TEXT";

    if (yyLex(returnToken, LEXnone, 0) != '(')
    {
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
        returnToken.release();
        returnToken.setExpr(createBlankString());
        return STRING_CONST;
    }

    StringBuffer parameterText;
    bool moreParams = getParameter(parameterText, directive, location);
    if (!moreParams)
    {
        while (parameterText.length() && parameterText.charAt(0)==' ')
            parameterText.remove(0, 1);
    }
    else
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "Too many operands");
        StringBuffer dummy;
        while (getParameter(dummy, directive, location))
            ;
    }

    returnToken.setExpr(createConstant(parameterText));
    return (STRING_CONST);
}


void HqlLex::doInModule(attribute & returnToken)
{
    ECLlocation location = returnToken.pos;
    const char * directive = "#INMODULE";

    int tok = yyLex(returnToken, LEXnone, 0);
    if (tok != '(')
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");

    StringBuffer moduleName, attrName;

    if (getParameter(moduleName, directive, location))
    {
        if (getParameter(attrName, directive, location))
        {
            reportError(returnToken, ERR_OPERANDS_TOOMANY, "too many operands");

            /* skip the rest */
            StringBuffer dummy;
            while (getParameter(dummy, directive, location))
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
    if (!ctx.queryPackage())
        return false;

    try
    {

        //hack: get rid of the extra leading spaces
        const char* pModule = moduleName;
        while(*pModule==' ') pModule++;
        const char* pAttr = attrName;
        while(*pAttr==' ') pAttr++;

        OwnedHqlExpr match = ctx.queryPackage()->queryRootScope()->lookupSymbol(createIdAtom(pModule), LSFpublic, ctx);
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
        IERRLOG("Unexpected exception in doInModule()");
    }

    return false;
}

void HqlLex::doUniqueName(attribute & returnToken)
{
    int tok = yyLex(returnToken, LEXnone, 0);
    if (tok != '(')
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
    else
        tok = yyLex(returnToken, LEXidentifier,0);

    if (tok != UNKNOWN_ID)
    {
        reportError(returnToken, ERR_EXPECTED_IDENTIFIER, "Identifier expected");
        returnToken.release();
    }
    else
    {
        IIdAtom * name = returnToken.getId();
        StringAttr pattern("__#__$__");

        tok = yyLex(returnToken, LEXnone,0);
        if (tok == ',')
        {
            tok = yyLex(returnToken, LEXstring,0);
            if (tok == STRING_CONST)
            {
                StringBuffer text;
                OwnedHqlExpr str = returnToken.getExpr();
                getStringValue(text, str);
                pattern.set(text.str());
                tok = yyLex(returnToken, LEXnone,0);
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

    //DBGLOG("Declaring unique name: %s",uniqueName.str());
    top->setValue(name,uniqueName.str());
}

void HqlLex::doIsValid(attribute & returnToken)
{
    ECLlocation location = returnToken.pos;
    const char * directive = "#ISVALID";

    int tok = yyLex(returnToken, LEXnone, 0);
    if (tok != '(')
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected");
    StringBuffer curParam("(");
    if (getParameter(curParam, directive, location))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "too many operands");
        StringBuffer dummy;
        while (getParameter(dummy, directive, location))
            ;
    }
    curParam.append(')');

    IHqlExpression * expr = NULL;
    IHqlScope *scope = createScope();

    try
    {
        HqlLookupContext ctx(yyParser->lookupCtx);
        ctx.errs.clear();   //Deliberately ignore any errors
        Owned<IFileContents> contents = createFileContentsFromText(curParam.str(), sourcePath, yyParser->inSignedModule, yyParser->gpgSignature, 0);
        expr = parseQuery(scope, contents, ctx, xmlScope, NULL, true, false);

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
        IERRLOG("Unexpected exception in doIsValid()");
    }

    ::Release(expr);
    ::Release(closeScope(scope));
}


void HqlLex::checkNextLoop(bool first)
{
    if (yyParser->checkAborting())
        return;
    if (loopTimes++ > MAX_LOOP_TIMES)
    {
        reportError(forLocation, ERR_TMPLT_LOOPEXCESSMAX,"The loop exceeded the limit: infinite loop is suspected");
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
            Owned<IValue> value = parseConstExpression(forLocation, forFilter, subscope);
            filtered = !value || !value->getBoolValue();
        }
        else
            filtered = false;
        if (!filtered)
        {
            pushText(forBody,forLocation.lineno,forLocation.column);
            inmacro->xmlScope = LINK(subscope);
            return;
        }
        first = false;
    }
    forLoop->Release();
    forLoop = NULL;
}


void HqlLex::doPreprocessorLookup(const attribute & errpos, bool stringify, int extra)
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
                // fallthrough
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
bool HqlLex::getDefinedParameter(StringBuffer &curParam, attribute & returnToken, const char* directive, const ECLlocation & location, SharedHqlExpr & resolved)
{
    enum { StateStart, StateDot, StateSelectId, StateFailed } state = StateStart;
    unsigned parenDepth = 1;
    OwnedHqlExpr expr;
    for (;;)
    {
        int tok = yyLex(returnToken, LEXidentifier|LEXexpand, 0);
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
                reportError(location, ERR_TMPLT_EOFINPARAM, "EOF encountered while gathering parameters for %s", directive);
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

bool HqlLex::doIsDefined(attribute & returnToken)
{
    ECLlocation location = returnToken.pos;
    const char * directive = "#ISDEFINED";

    if (!assertNextOpenBra())
        return false;

    OwnedHqlExpr resolved;
    StringBuffer paramText;
    bool hasMore = getDefinedParameter(paramText, returnToken, directive, location, resolved);
    if (hasMore)
        reportError(returnToken, ERR_EXPECTED, "Expected ')'");
    return resolved != NULL;
}


void HqlLex::doDefined(attribute & returnToken)
{
    ECLlocation location = returnToken.pos;
    const char * directive = "#DEFINED";

    if (!assertNextOpenBra())
        return;

    OwnedHqlExpr resolved;
    StringBuffer param1Text;
    StringBuffer param2Text;
    bool hasMore = getDefinedParameter(param1Text, returnToken, directive, location, resolved);
    if (hasMore)
        hasMore = getParameter(param2Text, directive, location);
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
    Owned<IHqlScope> scope = createScope();

    HqlGram parser(*yyParser, scope, contents, xmlScope);
    parser.getLexer()->set_yyLineNo(startLine);
    parser.getLexer()->set_yyColumn(startCol);
    return parser.yyParse(false, false);
}


IHqlExpression *HqlLex::parseECL(const char * text, IXmlScope *xmlScope, int startLine, int startCol)
{
    Owned<IFileContents> contents = createFileContentsFromText(text, querySourcePath(), yyParser->inSignedModule, yyParser->gpgSignature, 0);
    return parseECL(contents, xmlScope, startLine, startCol);
}


IValue *HqlLex::foldConstExpression(const ECLlocation & errpos, IHqlExpression * expr, IXmlScope *xmlScope)
{
    OwnedIValue value;
    if (expr)
    {
        try
        {
            OwnedHqlExpr folded = foldHqlExpression(*yyParser, expr, HFOthrowerror|HFOfoldimpure|HFOforcefold);
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
    {
        if (yyParser->lookupCtx.syntaxChecking() && yyParser->lookupCtx.hasCacheLocation())
        {
            reportError(errpos, ERR_EXPECTED_CONST, "Unable to expand constant expression when using cache. Try disabling cache."); // errpos could be better
        }
        else
        {
            reportError(errpos, ERR_EXPECTED_CONST, "Constant expression expected"); // errpos could be better
        }
    }

    return value.getClear();
}

IValue *HqlLex::parseConstExpression(const ECLlocation & errpos, StringBuffer &curParam, IXmlScope *xmlScope)
{
#ifdef TIMING_DEBUG
    MTIME_SECTION(timer, "HqlLex::parseConstExpression");
#endif
    OwnedHqlExpr expr = parseECL(curParam, xmlScope, errpos.lineno, errpos.position);
    return foldConstExpression(errpos, expr, xmlScope);
}

IValue *HqlLex::parseConstExpression(const ECLlocation & errpos, IFileContents * text, IXmlScope *xmlScope)
{
#ifdef TIMING_DEBUG
    MTIME_SECTION(timer, "HqlLex::parseConstExpression");
#endif
    OwnedHqlExpr expr = parseECL(text, xmlScope, errpos.lineno, errpos.position);
    return foldConstExpression(errpos, expr, xmlScope);
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

void HqlLex::doApply(attribute & returnToken)
{
    ECLlocation location = returnToken.pos;
    const char * directive = "#APPLY";

    int tok = yyLex(returnToken, LEXnone, 0);
    if (tok != '(')
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected"); // MORE - make it fatal!

    int line = returnToken.pos.lineno, col = returnToken.pos.column;
    StringBuffer curParam("(");
    if (getParameter(curParam, directive, location))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "too many operands");
        StringBuffer dummy;
        while (getParameter(dummy, directive, location))
            ;
    }
    curParam.append(')');
    OwnedHqlExpr actions = parseECL(curParam, queryTopXmlScope(), line, col);
    if (actions)
    {
        OwnedHqlExpr folded = foldHqlExpression(*yyParser, actions, HFOthrowerror|HFOfoldimpure|HFOforcefold);
    }
    else
        reportError(returnToken, ERR_EXPECTED_CONST, "Constant expression expected");
}

void HqlLex::doMangle(attribute & returnToken, bool de)
{
    ECLlocation location = returnToken.pos;
    const char * directive = de?"#DEMANGLE":"#MANGLE";

    int tok = yyLex(returnToken, LEXnone, 0);
    if (tok != '(')
        reportError(returnToken, ERR_EXPECTED_LEFTCURLY, "( expected"); // MORE - make it fatal!

    ECLlocation pos = returnToken.pos;
    StringBuffer curParam("(");
    if (getParameter(curParam, directive, location))
    {
        reportError(returnToken, ERR_OPERANDS_TOOMANY, "too many operands");
        StringBuffer dummy;
        while (getParameter(dummy, directive, location))
            ;
    }
    curParam.append(')');
    IValue *value = parseConstExpression(pos, curParam, queryTopXmlScope());
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
                    IERRLOG("Bad parameter to #DEMANGLE");
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
                IERRLOG("Bad parameter to #DEMANGLE");
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

int HqlLex::processStringLiteral(attribute & returnToken, char *CUR_TOKEN_TEXT, unsigned CUR_TOKEN_LENGTH, int oldColumn, int oldPosition)
{
    MemoryAttr tempBuff;
    char *b = (char *)tempBuff.allocate(CUR_TOKEN_LENGTH); // Escape sequence can only make is shorter...
    char *bf = b;
    const char *finger = CUR_TOKEN_TEXT;
    type_t tc = type_string;
    if (*finger != '\'')
    {
        if ((*finger == 'd') || (*finger == 'D'))
            tc = type_data;
        else if((*finger == 'q') || (*finger == 'Q'))
            tc = type_qstring;
        else if((*finger == 'v') || (*finger == 'V'))
            tc = type_varstring;
        finger++;
    }
    bool isMultiline = false;
    if (finger[1]=='\'' && finger[2]=='\'')
    {
        isMultiline = true;
        CUR_TOKEN_TEXT[CUR_TOKEN_LENGTH-2] = '\0';
        finger += 2;
    }
    for (finger++; finger[1]; finger++)
    {
        unsigned char next = *finger;
        size32_t delta = (size32_t)(finger-CUR_TOKEN_TEXT);
        if (next == '\\')
        {
            next = finger[1];
            if (finger[2]==0)  // finger[1] must be '.
            {
                returnToken.setPosition(yyLineNo, oldColumn+delta, oldPosition+delta, querySourcePath());
                StringBuffer msg("Can not terminate a string with escape char '\\': ");
                msg.append(CUR_TOKEN_TEXT);
                reportError(returnToken, RRR_ESCAPE_ENDWITHSLASH, "%s", msg.str());
                if (checkAborting())
                    return EOF;
            }
            else if (next == '\'' || next == '\\' || next == '?' || next == '"')
            {
                finger++;
            }
            else if (next == '\n')
            {
                finger++;
                continue;  // A \ at end of line in a multiline constant means remove the end-of-line
            }
            else if (next == '\r')
            {
                if (finger[2] == '\n')
                    finger += 2;
                else
                    finger++;
                continue;  // A \ at end of line in a multiline constant means remove the end-of-line
            }
            else if (next == 'a')
            {
                next = '\a';
                finger++;
            }
            else if (next == 'b')
            {
                next = '\b';
                finger++;
            }
            else if (next == 'f')
            {
                next = '\f';
                finger++;
            }
            else if (next == 'n')
            {
                next = '\n';
                finger++;
            }
            else if (next == 'r')
            {
                next = '\r';
                finger++;
            }
            else if (next == 't')
            {
                next = '\t';
                finger++;
            }
            else if (next == 'v')
            {
                next = '\v';
                finger++;
            }
            else if (isdigit(next) && next < '8')
            {
                //Allow octal constants for ^Z etc.
                unsigned value = 0;
                unsigned count;
                for (count=0; count < 3; count++)
                {
                    next = finger[count+1];
                    if (!isdigit(next) || next >= '8')
                        break;
                    value = value * 8 + (next - '0');
                }
                if(count != 3)
                {
                    returnToken.setPosition(yyLineNo, oldColumn+delta, oldPosition+delta, querySourcePath());
                    StringBuffer msg;
                    msg.append("3-digit numeric escape sequence contained non-octal digit: ").append(next);
                    reportError(returnToken, ERR_ESCAPE_UNKNOWN, "%s", msg.str());
                    if (checkAborting())
                        return EOF;
                }
                *bf++ = value;
                if(!(isValidAsciiLikeCharacter(value) || (tc == type_data)))
                {
                    returnToken.setPosition(yyLineNo, oldColumn+delta, oldPosition+delta, querySourcePath());
                    reportWarning(CategoryCast, returnToken, ERR_STRING_NON_ASCII, "Character in string literal is not defined in encoding " ASCII_LIKE_CODEPAGE);
                    if (checkAborting())
                        return EOF;
                }
                finger += count;
                continue;
            }
            else
            {
                StringBuffer msg;
                msg.append("Unrecognized escape sequence: ");
                msg.append("\\").append(finger[1]);
                returnToken.setPosition(yyLineNo, oldColumn+delta, oldPosition+delta, querySourcePath());
                reportError(returnToken, ERR_ESCAPE_UNKNOWN, "%s", msg.str());
                if (checkAborting())
                    return EOF;
            }
            *bf++ = next;
        }
        else if (next == '\r')
        {
            //Convert \r\n to \n and \r to \n
            next = finger[1];
            if (next == '\n')
                finger++;
            *bf++ = '\n';
        }
        else if (next == '\'' && !isMultiline)
        {
            returnToken.setPosition(yyLineNo, oldColumn+delta, oldPosition+delta, querySourcePath());
            reportError(returnToken, ERR_STRING_NEEDESCAPE,"' needs to be escaped by \\ inside string");
            if (checkAborting())
                return EOF;
        }
        else if (next >= 128)
        {
            unsigned lenLeft = CUR_TOKEN_LENGTH - (size32_t)(finger - CUR_TOKEN_TEXT);
            int extraCharsRead = rtlSingleUtf8ToCodepage(bf, lenLeft, finger, ASCII_LIKE_CODEPAGE);
            if (extraCharsRead == -1)
            {
                //This really has to be an error, otherwise it will work most of the time, but will then sometimes fail
                //because two characters > 128 are next to each other.
                returnToken.setPosition(yyLineNo, oldColumn+delta, oldPosition+delta, querySourcePath());
                reportError(returnToken, ERR_STRING_NON_ASCII, "Character in string literal is not legal UTF-8");
                if (checkAborting())
                    return EOF;
                *bf = next;
            }
            else
            {
                if (*bf == ASCII_LIKE_SUBS_CHAR)
                {
                    returnToken.setPosition(yyLineNo, oldColumn+delta, oldPosition+delta, querySourcePath());
                    reportWarning(CategoryCast, returnToken, ERR_STRING_NON_ASCII, "Character in string literal is not defined in encoding " ASCII_LIKE_CODEPAGE ", try using a unicode constant");
                }
                finger += extraCharsRead;
            }
            bf++;
        }
        else
        {
            *bf++ = next;
            if(!(isValidAsciiLikeCharacter(next) || (tc == type_data)))
            {
                returnToken.setPosition(yyLineNo, oldColumn+delta, oldPosition+delta, querySourcePath());
                reportError(returnToken, ERR_STRING_NON_ASCII, "Character in string literal is not defined in encoding " ASCII_LIKE_CODEPAGE);
                if (checkAborting())
                    return EOF;
            }
        }
    }
    returnToken.setPosition(yyLineNo, oldColumn, oldPosition, querySourcePath());
    switch (tc)
    {
    case type_qstring:
        {
            Owned<ITypeInfo> qStrType = makeQStringType(UNKNOWN_LENGTH);
            returnToken.setExpr(createConstant(qStrType->castFrom((size32_t)(bf-b), b)));
            return (DATA_CONST);
        }
    case type_data:
        {
            returnToken.setExpr(createConstant(createDataValue(b, (size32_t)(bf-b))));
            return (DATA_CONST);
        }
    case type_varstring:
        {
            returnToken.setExpr(createConstant(createVarStringValue((size32_t)(bf-b), b, makeVarStringType(UNKNOWN_LENGTH))));
            return (DATA_CONST);
        }
    case type_string:
        returnToken.setExpr(createConstant(createStringValue(b, (size32_t)(bf-b))));
        return (STRING_CONST);
    }
    throwUnexpected();
}


void HqlLex::stripSlashNewline(attribute & returnToken, StringBuffer & target, size_t len, const char * text)
{
    target.ensureCapacity(len);
    for (size_t i = 0; i < len; i++)
    {
        char cur = text[i];
        if (cur == '\\')
        {
            if (i+1 < len)
            {
                byte next = text[i+1];
                if (next == '\n')
                {
                    i++;
                    continue;
                }
                if (next == '\r')
                {
                    i++;
                    if ((i+1) < len && text[i+1] == '\n')
                        i++;
                    continue;
                }
            }
            else
            {
                StringBuffer msg("Cannot terminate a string with escape char '\\': ");
                msg.append(len, text);
                reportError(returnToken, RRR_ESCAPE_ENDWITHSLASH, "%s", msg.str());
            }
        }
        else if (cur == '\r')
        {
            //Normalize unusual end of line sequences
            if ((i+1 < len) && (text[i+1] == '\n'))
                i++;
            cur = '\n';
        }

        target.append(cur);
    }
}

bool HqlLex::isImplicitlySigned()
{
    if (text && text->isImplicitlySigned())
        return true;
    if (!inmacro)
        return false;
    return inmacro->isImplicitlySigned();
}

//====================================== Error Reporting  ======================================

bool HqlLex::checkAborting()
{
    return yyParser->checkAborting();
}

void HqlLex::reportError(const attribute & returnToken, int errNo, const char *format, ...)
{
    if (yyParser)
    {
        va_list args;
        va_start(args, format);
        yyParser->reportErrorVa(errNo, returnToken.pos, format, args);
        va_end(args);
    }
}

void HqlLex::reportError(const ECLlocation & pos, int errNo, const char *format, ...)
{
    if (yyParser)
    {
        va_list args;
        va_start(args, format);
        yyParser->reportErrorVa(errNo, pos, format, args);
        va_end(args);
    }
}

void HqlLex::reportWarning(WarnErrorCategory category, const attribute & returnToken, int warnNo, const char *format, ...)
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

 bool HqlLex::lookupXmlSymbol(const attribute & errpos, const char *name, StringBuffer &ret)
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

void HqlLex::setXmlSymbol(const attribute & errpos, const char *name, const char *value, bool append)
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

void HqlLex::declareXmlSymbol(const attribute & errpos, const char *name)
{
    IXmlScope *top = ensureTopXmlScope();
    if (!top->declareValue(name))
    {
        StringBuffer msg("Symbol has already been declared: ");
        msg.append(name);
        reportError(errpos, ERR_TMPLT_SYMBOLREDECLARE, "%s", msg.str());
    }
}

IIterator *HqlLex::getSubScopes(const attribute & errpos, const char *name, bool doAll)
{
    IXmlScope *top = ensureTopXmlScope();
    return top->getScopes(name, doAll);
}

void HqlLex::loadXML(const attribute & errpos, const char *name, const char * child)
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

const char * HqlLex::queryMacroScopeName(IEclPackage * & package)
{
    if (inmacro)
    {
        const char * scope = inmacro->queryMacroScopeName(package);
        if (scope)
            return scope;
    }
    if (hashDollar)
    {
        package = hashDollarPackage;
        return str(hashDollar);
    }
    return nullptr;
}

IPropertyTree * HqlLex::getClearJavadoc()
{
    if (javaDocComment.length() == 0)
        return NULL;

    IPropertyTree * tree = createPTree("javadoc");
    tree->addProp("content", javaDocComment.str());
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
    if (inmacro)
        inmacro->enterEmbeddedMode();
    else
    {
        doEnterEmbeddedMode(scanner);
        inCpp = true;
    }
}

int HqlLex::yyLex(attribute & returnToken, LexerFlags lookupFlags, const short * activeState)
{
    for (;;)
    {
        while (inmacro)
        {
            int ret = inmacro->yyLex(returnToken, lookupFlags, activeState);
            if (ret > 0 && ret != HASHBREAK)
            {
                lastToken = ret;
                return ret;
            }

#if defined(TRACE_MACRO)
            DBGLOG("MACRO>> inmacro %p deleted\n", inmacro);
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
                checkNextLoop(false);
        }

        returnToken.clear();

        yyStartPos = yyPosition;
        int ret = doyyFlex(returnToken, scanner, this, lookupFlags, activeState);
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
            else if (inMultiString)
                reportError(returnToken, ERR_COMMENT_UNENDED,"Multiline string constant is not terminated");
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


