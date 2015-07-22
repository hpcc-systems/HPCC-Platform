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
#include "jliball.hpp"
#include "hql.hpp"

#include "platform.h"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jmisc.hpp"

#include "hqlexpr.hpp"
#include "hqlattr.hpp"

#include "hqlstmt.hpp"
#include "hqlfunc.hpp"

#include "hqlcerrors.hpp"
#include "hqlcpp.ipp"
#include "hqlwcpp.hpp"
#include "hqlwcpp.ipp"

#define INDENT_SOURCE
#define FILE_CHUNK_SIZE         65000
#define PREFERRED_LINE_LIMIT    160
#define REASONABLE_LINE_LIMIT   512


static const char * vcIntTypes[] = { "char","short","int","int","__int64","__int64","__int64","__int64" };
static const char * vcUIntTypes[] = { "unsigned char","unsigned short","unsigned","unsigned","unsigned __int64","unsigned __int64","unsigned __int64","unsigned __int64" };
static const char * gccIntTypes[] = { "char","short","int","int","long long","long long","long long","long long" };
static const char * gccUIntTypes[] = { "unsigned char","unsigned short","unsigned","unsigned","unsigned long long","unsigned long long","unsigned long long","unsigned long long" };

// ITypeInfo and IValue implementations
inline const char * intTypeName(unsigned len, CompilerType compiler, bool isSigned)
{
    switch (compiler)
    {
    case GccCppCompiler:
        return isSigned ? gccIntTypes[len] : gccUIntTypes[len];
    case Vs6CppCompiler:
        return isSigned ? vcIntTypes[len] : vcUIntTypes[len];
    default:
        throwUnexpected();
    }
}

bool isTypePassedByAddress(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_decimal:
    case type_string:
    case type_data:
    case type_qstring:
    case type_varstring:
    case type_unicode:
    case type_varunicode:
    case type_utf8:
    case type_set:
    case type_row:
        return true;
    case type_dictionary:
    case type_table:
    case type_groupedtable:
        return !isArrayRowset(type);
    case type_record:
        throwUnexpected();
    }
    return false;
}

static StringBuffer & appendCapital(StringBuffer & s, StringBuffer & _name)
{
    const char * name = _name.str();
    if (name && name[0])
    {
        s.append((char)toupper(*name));
        s.append(name+1);
    }
    return s;
}

//---------------------------------------------------------------------------

CppWriterTemplate::CppWriterTemplate()
{
    text = NULL;
    len = 0;
}

CppWriterTemplate::~CppWriterTemplate()
{
    free(text);
}

void CppWriterTemplate::generate(ISectionWriter & writer, unsigned pass, IProperties * properties)
{
    writer.setOutput(out, outStream);

    const char * finger = text;
    bool output = true;
    BoolArray outputStack;
    StringBuffer temp;

    ForEachItemIn(idx, sections)
    {
        CppTemplateSection & cur = (CppTemplateSection &)sections.item(idx);

        if (output && (cur.position > finger))
            outputQuoted(writer, cur.position-finger, finger);
        switch (cur.type)
        {
        case TplEmbed:
            if (output && cur.id)
                writer.generateSection(cur.indent, cur.id, pass);
            break;
        case TplExpand:
            if (output && properties)
            {
                temp.clear();
                properties->getProp(str(cur.id), temp);
                outputQuoted(writer, temp.length(), temp.str());
            }
            break;
        case TplCondition:
            outputStack.append(output);
            if (output)
                output = (properties && properties->hasProp(str(cur.id)));
            break;
        case TplEndCondition:
            output = outputStack.popGet();
            break;
        }

        finger = cur.position   + cur.len;
    }
    char * end = text+len;
    if (output && (end > finger))
        outputQuoted(writer, end-finger, finger);

    writer.setOutput(NULL, NULL);
}

bool CppWriterTemplate::loadTemplate(const char * filename, const char *dir)
{
    StringBuffer tpl(dir);
    if(tpl.length())
        tpl.append(PATHSEPCHAR);
    tpl.append(filename);

    Owned<IFile> file = createIFile(tpl);
    Owned<IFileIO> io = file->openShared(IFOread, IFSHread);
    if (!io)
        return false;
    offset_t size = (size32_t)io->size();
    if (size != (size32_t)size)
        return false;
    text = (char *)malloc((size_t)size);
    len=io->read(0, (size32_t)size, text);
    
    unsigned index=0;
    unsigned startLine = 0;
    while (index != len)
    {
        char c = text[index];
        switch (c)
        {
        case '$':
        case '@':
            {
                unsigned start = index+1;
                TplSectionType type = TplEmbed;
                if (c == '$')
                {
                    type = TplExpand;
                    if ((start < len) && (text[start] == '?'))
                    {
                        start++;
                        type = TplCondition;
                        if ((start < len) && (text[start] == c))
                            type = TplEndCondition;
                    }
                }
                
                unsigned end = start;
                loop
                {
                    if (end >= len)
                        throwError(HQLERR_MissingTemplateTerminator);
                    if (text[end] == c)
                        break;
                    ++end;
                }
                
                unsigned indent = 0;
                while (indent < index - startLine && isspace((byte)text[index-indent-1]))
                    indent++;
                CppTemplateSection * next = new CppTemplateSection;
                next->type = type;
                next->position = text+index - indent;
                next->len = end+1 - index + indent;
                next->indent = indent;
                next->id = createAtom(text+start, end-start);
                
                if (end == index+1)
                    next->len--;        // quoted character => include the next @/$
                
                sections.append(*next);
                index = end+1;
                break;
            }
        case '\r':
        case '\n':
            startLine = index+1;
            index++;
            break;
        default:
            index++;
            break;
        }
    }
    return true;
}

//---------------------------------------------------------------------------

const char * getOpText(node_operator op)
{
    switch (op)
    {
        case no_mul: return "*"; 
        case no_div: return "/"; 
        case no_modulus: return "%";
        case no_negate: return "-";
        case no_add: return "+"; 
        case no_sub: return "-";
        case no_eq: return "=="; 
        case no_ne: return "!="; 
        case no_lt: return "<"; 
        case no_le: return "<="; 
        case no_gt: return ">"; 
        case no_ge: return ">=";
        case no_not: return "!";
        case no_and: return "&&"; 
        case no_or: return "||"; 
        case no_xor: return "xor";  //doesn't actually exist, should be transformed
        case no_comma: return ",";
        case no_compound: return ",";
        case no_select: return ".";
        case no_bnot: return "~";
        case no_band: return "&";
        case no_bor: return "|";
        case no_bxor: return "^";
        case no_postinc: return "++";
        case no_postdec: return "--";
        case no_preinc: return "++";
        case no_predec: return "--";
        case no_pselect: return "->";
        case no_address: return "&";
        case no_deref: return "*";
        case no_lshift: return "<<";
        case no_rshift: return ">>";
    }
    throwUnexpectedOp(op);
}

unsigned getPrecedence(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    if (op == no_typetransfer)
        return getPrecedence(expr->queryChild(0));

    switch (op)
    {
        case no_order:          //pseudo operator always generated in brackets
            return 20;
        // :: = 18
        case no_pselect: case no_select: case no_index: case no_externalcall: case no_call:
        case no_reference:
            return 17;
        case no_postinc: case no_postdec:
            return 16;
        case no_not: case no_negate: 
        case no_preinc: case no_predec: 
        case no_bnot: case no_address: case no_deref:
            return 15;
        case no_cast:
        case no_implicitcast:
            return 14;
        case no_mul: case no_div: case no_modulus:
            return 13;
        case no_add: case no_sub: 
            return 12;
        case no_lshift: case no_rshift: 
            return 11;
        case no_lt: case no_gt: case no_le: case no_ge:
            return 10;
        case no_eq: case no_ne:
            return 9;
        case no_band:
            return 8;
        case no_bxor:
            return 7;
        case no_bor:
            return 6;
        case no_and:
            return 5;
        case no_or:
            return 4;
        case no_if:
            return 3;
        case no_assign: //op no_plus_assign etc.
            return 2;
        case no_compound:
        case no_comma:
            return 1;
    }
    return 50;
}

//---------------------------------------------------------------------------

class TypeNameBuilder
{
public:
    TypeNameBuilder(const char * name) { typeOnLeft = false; str.append(name); }

    void addPrefix(const char * text)
    {
        if (str.length())
            str.insert(0, " ");
        str.insert(0, text);
        typeOnLeft = true;
    }

    StringBuffer & addSuffix()
    {
        if (typeOnLeft)
        {
            str.insert(0, "(").append(")");
            typeOnLeft = false;
        }
        return str;
    }
    
    StringBuffer & addArray(unsigned length)
    {
        return addSuffix().append("[").append(length ? length : 1).append("]");
    }
    
    void get(StringBuffer & out) { out.append(str); }

protected:
    StringBuffer str;
    bool typeOnLeft;
};

void HqlCppWriter::generateType(StringBuffer & result, ITypeInfo * type, const char * name)
{
    ::generateTypeCpp(result, type, name, compiler);
}

void HqlCppWriter::generateType(ITypeInfo * type, const char * name)
{
    TypeNameBuilder result(name);

    loop
    {
        bool isPointer = false;
        bool outOfLine= false;
        ITypeInfo * fullType = type;
        loop
        {
            typemod_t tmod = type->queryModifier();
            if (tmod == typemod_none)
                break;
            switch (tmod)
            {
            case typemod_const:
//              result.addPrefix("const");
                break;
            case typemod_outofline:
                outOfLine = false;
                break;
            case typemod_ref:
                isPointer = true;
                break;
            }
            type = type->queryTypeBase();
        }

        ITypeInfo * next = type->queryChildType();
        type_t tc = type->getTypeCode();
        size32_t size = type->getSize();
        const char * prefix = NULL;
        switch (tc)
        {
        case type_pointer:
            {
                prefix = "*";
                break;
            }
        case type_bitfield:
            result.addSuffix().append(":").append(type->getBitSize());
            break;
        case type_array:
            {
                unsigned dim = type->getCardinality();
                result.addSuffix().append("[");
                if (dim && (dim != UNKNOWN_LENGTH))
                    result.addSuffix().append(dim);
                result.addSuffix().append("]");
                break;
            }
        case type_decimal:
            //MORE: decimal different as parameters???
        case type_varstring:
        case type_string:
        case type_qstring:
        case type_data:
        case type_utf8:
            {
                if ((size != UNKNOWN_LENGTH) && !isPointer)
                {
                    result.addArray(size);
                }
                else
                    isPointer = true;

                if (tc == type_data)
                {
                    if (isPointer)
                        prefix = "void";
                    else
                        prefix = "byte";
                }
                else if (tc == type_decimal)
                {
                    //make this to byte
                    if (isPointer)
                        prefix = "void";
                    else
                        prefix = "char";
                }
                else
                    prefix = "char";
                break;
            }
        case type_varunicode:
        case type_unicode:
            {
                if ((type->getSize() != UNKNOWN_LENGTH) && !isPointer)
                    result.addArray(size/2);
                else
                    isPointer = true;

                prefix = "UChar";
                break;
            }
        case type_char:
            prefix = "char";
            break;
        case type_row:
        case type_sortlist:
            if (hasLinkCountedModifier(fullType))
                isPointer = true;
            prefix = "byte";
            next = NULL;
            break;
        case type_groupedtable:
            //next = next->queryChildType();
            isPointer = false;
            break;
        case type_dictionary:// MORE - is this right?
        case type_table:
            if (isArrayRowset(fullType))
            {
                prefix = "*";
            }
            else
            {
                isPointer = false;
                prefix = "*";
            }
            break;
        case type_class:
            prefix = type->queryTypeName();
            break;
        case type_boolean:
            prefix = "bool";
            break;
        case type_int:
        case type_swapint:
            prefix = intTypeName(size-1, compiler, type->isSigned());
            break;
        case type_real:
            prefix = (size == 4) ? "float" : "double";
            break;
        case type_packedint:
        case type_enumerated:
            //recurse to child type
            break;
        case type_void:
        case type_any:
            prefix = "void";
            break;
        case type_set:
            if (isPointer)
            {
                prefix = "byte";
                next = NULL;
            }
            else if (!next)
            {
                result.addPrefix("char * *");
//              result.addPrefix("byte");
                isPointer = false;
            }
            break;
        case type_function:
            {
                StringBuffer parameterText;
                IFunctionTypeExtra * extra = queryFunctionTypeExtra(type);
                IHqlExpression * args = static_cast<IHqlExpression *>(extra->queryParameters());
                IHqlExpression * attrs = static_cast<IHqlExpression *>(extra->queryAttributes());
                if (queryAttributeInList(contextAtom, attrs))
                    parameterText.append("ICodeContext * ctx");

                ForEachChild(i, args)
                {
                    if (parameterText.length())
                        parameterText.append(", ");
                    generateExprCpp(parameterText, args->queryChild(i));
                }

                //Walk args and add the types
                result.addSuffix().append("(").append(parameterText).append(")");
            }
            break;
        case type_transform:
        default:
            throwUnexpected();
        }

        if (isPointer)
        {
            result.addPrefix("*");
            isPointer = false;
        }

        if (prefix)
            result.addPrefix(prefix);

        if (!next)
        {
            result.get(out);
            return;
        }

        type = next;
    }
}


bool HqlCppWriter::generateFunctionPrototype(IHqlExpression * funcdef)
{
    IHqlExpression *body = funcdef->queryChild(0);
    StringBuffer name;
    getAttribute(body, entrypointAtom, name);
    if (!name.length())
        name.append(funcdef->queryName());
    return generateFunctionPrototype(funcdef, name);
}

bool HqlCppWriter::generateFunctionPrototype(IHqlExpression * funcdef, const char * name)
{
    IHqlExpression *body = funcdef->queryChild(0);
    IHqlExpression *formals = funcdef->queryChild(1);

    if (body->hasAttribute(includeAtom) || body->hasAttribute(ctxmethodAtom) || body->hasAttribute(gctxmethodAtom) || body->hasAttribute(methodAtom) || body->hasAttribute(sysAtom) || body->hasAttribute(omethodAtom))
        return false;

    IHqlExpression *proto = body->queryAttribute(prototypeAtom);
    if (proto)
    {
        StringBuffer s;
        getStringValue(s, proto->queryChild(0));
        out.append(s);
        return true;
    }
    enum { ServiceApi, RtlApi, FastApi, CApi, CppApi, LocalApi } api = ServiceApi;
    bool isVirtual = funcdef->hasAttribute(virtualAtom);
    bool isLocal = body->hasAttribute(localAtom);
    if (body->hasAttribute(eclrtlAtom))
        api = RtlApi;
    else if (body->hasAttribute(fastAtom))
        api = FastApi;
    else if (body->hasAttribute(cAtom))
        api = CApi;
    else if (body->hasAttribute(cppAtom))
        api = CppApi;
    else if (isLocal || isVirtual)
        api = LocalApi;

    if (isVirtual)
        out.append("virtual");
    else
        out.append("extern");

    if ((api == ServiceApi) || api == CApi)
        out.append(" \"C\" ");

    switch (api)
    {
    case ServiceApi: out.append(" SERVICE_API"); break;
    case RtlApi:     out.append(" RTL_API"); break;
    case FastApi:     out.append(" BCD_API"); break;
    }
    out.append(" ");

    StringBuffer returnParameters;
    ITypeInfo * retType = funcdef->queryType()->queryChildType();
    generateFunctionReturnType(returnParameters, retType, body);

    switch (api)
    {
    case CApi:
        switch (compiler)
        {
        case Vs6CppCompiler:
            out.append(" _cdecl");
            break;
        }
        break;
    case FastApi:
        switch (compiler)
        {
        case Vs6CppCompiler:
            out.append(" __fastcall");
            break;
        }
        break;
    }

    out.append(" ").append(name);

    bool firstParam = true;
    out.append('(');
    if (functionBodyUsesContext(body))
    {
        out.append("ICodeContext * ctx");
        firstParam = false;
    }
    else if (body->hasAttribute(globalContextAtom) )
    {
        out.append("IGlobalCodeContext * gctx");
        firstParam = false;
    }
    else if (body->hasAttribute(userMatchFunctionAtom))
    {
        out.append("IMatchWalker * results");
        firstParam = false;
    }

    if (returnParameters.length())
    {
        if (!firstParam)
            out.append(',');
        out.append(returnParameters);
        firstParam = false;
    }

    ForEachChild(i, formals)
    {
        if (!firstParam)
            out.append(',');
        else
            firstParam = false;

        IHqlExpression * param = formals->queryChild(i);
        generateParamCpp(param, body);
    }
    out.append(")");
    return true;
}



void HqlCppWriter::generateInitializer(IHqlExpression * expr)
{
    IValue * value = expr->queryValue();
    assertex(value);

    ITypeInfo * type = value->queryType();
    type_t tc = type->getTypeCode();
    size32_t size = value->getSize();

    const byte * raw = (const byte *)value->queryValue();

    out.append("{ ");
    switch (tc)
    {
    case type_data:
    case type_qstring:
    case type_decimal:
        {
            for (unsigned i=0; i < size; i++)
            {
                if (i)
                    out.append(",");
                queryBreakLine();
                out.append((unsigned)raw[i]);
            }
            break;
        }
    case type_string:
    case type_varstring:
    case type_utf8:
        {
            for (unsigned i = 0; i < size; i++)
            {
                if (i)
                    out.append(",");
                queryBreakLine();
                byte next = raw[i];
                switch (next)
                {
                case '\\': case '\'':
                    out.append("'\\").append((char)next).append('\'');
                    break;
                default:
                    if (isprint(next))
                        out.append('\'').append((char)next).append('\'');
                    else
                        out.append((int)next);
                    break;
                }
            }
            break;
        }
    case type_unicode:
    case type_varunicode:
        {
            unsigned max = size/2;
            for (unsigned i = 0; i < max; i++)
            {
                if (i)
                    out.append(",");
                queryBreakLine();
                UChar next = ((UChar *)raw)[i];
                switch (next)
                {
                case '\\': case '\'':
                    out.append("'\\").append((char)next).append('\'');
                    break;
                default:
                    if ((next >= 32) && (next <= 126))
                        out.append('\'').append((char)next).append('\'');
                    else
                        out.append((unsigned)next);
                    break;
                }
            }
            break;
        }
    default:
        throwUnexpected();
    }

    out.append(" }");
}


void HqlCppWriter::generateParamCpp(IHqlExpression * param, IHqlExpression * attrs)
{
    ITypeInfo *paramType = param->queryType();
    
    //Case is significant if these parameters are use for BEGINC++ sections
    IAtom * paramName = param->queryName();
    StringBuffer paramNameText;
    paramNameText.append(paramName).toLowerCase();

    bool isOut = false;
    bool isConst = false;
    unsigned maxAttr = param->numChildren();
    unsigned attrIdx;
    for (attrIdx = 0; attrIdx < maxAttr; attrIdx++)
    {
        IHqlExpression * attr = param->queryChild(attrIdx);
        if (attr->isAttribute())
        {
            if (attr->queryName() == constAtom)
                isConst = true;
            else if (attr->queryName() == outAtom)
                isOut = true;
        }
    }

    switch (paramType->getTypeCode())
    {
    case type_dictionary:
    case type_table:
    case type_groupedtable:
    case type_row:
        if (getBoolAttribute(attrs, passParameterMetaAtom, false))
        {
            out.append("IOutputMetaData & ");
            if (paramName)
                appendCapital(out.append(" meta"), paramNameText);
            out.append(",");
        }
        break;
    }

    switch (paramType->getTypeCode())
    {
    case type_string:
    case type_qstring:
    case type_data:
    case type_unicode:
    case type_utf8:
    case type_dictionary:
    case type_table:
    case type_groupedtable:
        if ((paramType->getSize() == UNKNOWN_LENGTH) && !hasStreamedModifier(paramType))
        {
            out.append("size32_t");
            if (isOut)
                out.append(" &");
            if (paramName)
            {
                if (hasOutOfLineModifier(paramType) || hasLinkCountedModifier(paramType))
                    appendCapital(out.append(" count"), paramNameText);
                else
                    appendCapital(out.append(" len"), paramNameText);
            }
            out.append(",");
        }
        break;
    case type_set:
        if (!queryAttribute(paramType, oldSetFormatAtom))
        {
            out.append("bool");
            if (paramName)
                appendCapital(out.append(" isAll"), paramNameText);
            out.append(",size32_t ");
            if(paramName)
                appendCapital(out.append(" len"), paramNameText);
        }
        else
        {
            out.append("unsigned");
            if(paramName)
                appendCapital(out.append(" num"), paramNameText);
        }
        out.append(",");
        break;
    case type_row:
        isConst = true;
        break;
    }
    
    bool nameappended = false;
    switch (paramType->getTypeCode())
    {
    case type_record:
        out.append("IOutputMetaData * ");
        break;
    case type_dictionary:
    case type_table:
    case type_groupedtable:
        if (isConst)
            out.append("const ");
        if (hasStreamedModifier(paramType))
            out.append("IRowStream *");
        else if (hasOutOfLineModifier(paramType) || hasLinkCountedModifier(paramType))
            out.append("byte * *");
        else
            out.append("void *");
        if (isOut)
            out.append(" &");
        break;
    case type_set:
        if (!queryAttribute(paramType, oldSetFormatAtom))
        {
            if (isConst)
                out.append("const ");
            out.append("void *");
            if (isOut)
                out.append(" &");
            break;
        }
        else
        {
            ITypeInfo* childType = paramType->queryChildType();
            if(!childType)
                break;
            if(isStringType(childType)) {
                // process stringn and varstringn specially.
                if(childType->getSize() > 0) {
                    out.append("char ");
                    if(paramName) {
                        out.append(paramNameText);
                        nameappended = true;
                    }
                    out.append("[][");
                    unsigned setlen = childType->getSize();
                    out.append(setlen);
                    out.append("]");
                }
                // Process string and varstring specially
                else {
                    out.append("char *");
                    if(paramName) {
                        out.append(paramNameText);
                        nameappended = true;
                    }
                    out.append("[]");
                }
            }
            else
            {
                OwnedITypeInfo pointerType = makePointerType(LINK(childType));
                generateType(pointerType, NULL);
            }
            break;
        }
        // Other set types just fall through and will be treated like other types.
    case type_qstring: case type_string: case type_varstring: case type_data:
    default:
        {
            if (isConst)
                out.append("const ");
            Owned<ITypeInfo> argType = LINK(paramType);
            if (argType->getTypeCode() == type_function)
                argType.setown(makePointerType(LINK(argType)));
            if (isTypePassedByAddress(argType))
                argType.setown(makeReferenceModifier(LINK(argType)));
            generateType(argType, str(paramName));
            nameappended = true;
            if (isOut)
                out.append(" &");
            break;
        }
    }
    if (paramName && !nameappended)
        out.append(" ").append(paramNameText);
}

void HqlCppWriter::generateFunctionReturnType(StringBuffer & params, ITypeInfo * retType, IHqlExpression * attrs)
{
    type_t tc = retType->getTypeCode();
    switch (tc)
    {
    case type_varstring:
    case type_varunicode:
        if (retType->getSize() == UNKNOWN_LENGTH)
        {
            generateType(retType, NULL);
            break;
        }
        //fall through
    case type_qstring:
    case type_string:
    case type_data:
    case type_unicode:
    case type_utf8:
        {
            OwnedITypeInfo ptrType = makeReferenceModifier(LINK(retType));
            out.append("void");
            if (retType->getSize() == UNKNOWN_LENGTH)
            {
                if (retType->getTypeCode() != type_varstring)
                    params.append("size32_t & __lenResult,");

    //          if (hasConstModifier(retType))
    //              params.append("const ");
                generateType(params, retType, NULL);
                params.append(" & __result");
            }
            else
            {
                generateType(params, ptrType, "__result");
            }
            break;
        }
    case type_transform:
        out.append("size32_t");
        params.append("ARowBuilder & __self");
        break;
    case type_dictionary:
    case type_table:
    case type_groupedtable:
        if (hasStreamedModifier(retType))
        {
            out.append("IRowStream *");
            params.append("IEngineRowAllocator * _resultAllocator");
        }
        else if (hasLinkCountedModifier(retType))
        {
            out.append("void");
            params.append("size32_t & __countResult,");
    //      if (hasConstModifier(retType))
    //          params.append("const ");
            params.append("byte * * & __result");
            if (hasNonNullRecord(retType) && getBoolAttribute(attrs, allocatorAtom, true))
                params.append(", IEngineRowAllocator * _resultAllocator");
        }
        else
        {
            out.append("void");
            params.append("size32_t & __lenResult,");
    //      if (hasConstModifier(retType))
    //          params.append("const ");
            params.append("void * & __result");
        }
        break;
    case type_set:
        {
            out.append("void");
            if (!getBoolAttribute(attrs, oldSetFormatAtom))
            {
                params.append("bool & __isAllResult,");
                params.append("size32_t & __lenResult,");
            }
            else
                params.append("unsigned & __numResult,");

//          if (hasConstModifier(retType))
//              params.append("const ");
            params.append("void * & __result");
            break;
        }
    case type_row:
        if (hasLinkCountedModifier(retType))
        {
            out.append("byte *");
            if (hasNonNullRecord(retType) && getBoolAttribute(attrs, allocatorAtom, true))
                params.append("IEngineRowAllocator * _resultAllocator");
        }
        else
        {
            out.append("void");
            params.append("byte * __result");
        }
        break;
    default:
        generateType(retType, NULL);
        break;
    }
}


StringBuffer & HqlCppWriter::generateExprCpp(StringBuffer & result, IHqlExpression * expr)
{
    return ::generateExprCpp(result, expr, compiler);
}

StringBuffer & HqlCppWriter::generateExprCpp(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
        case no_constant:
            {
                unsigned prevLen = out.length();
                expr->queryValue()->generateCPP(out, compiler);
                outputLineNum += memcount(out.length()-prevLen,out.str()+prevLen,'\n');
                break;
            }
        case no_eq:
        case no_ne:
        case no_lt:
        case no_le:
        case no_gt:
        case no_ge:
        case no_mul: 
        case no_div: 
        case no_modulus: 
        case no_add:
        case no_sub:
        case no_and: 
        case no_or:
        case no_xor:
        case no_comma:
        case no_compound:
        case no_band:
        case no_bor:
        case no_bxor:
        case no_lshift:
        case no_rshift:
            {
                unsigned numArgs = expr->numChildren();
                for (unsigned index = 0; index < numArgs; index++)
                {
                    if (index != 0)
                        out.append(' ').append(getOpText(op)).append(' ');
                    //would be nicer if it broke at PREFERRED_LINE_LIMIT if not inside a ()
                    queryBreakLine();
                    generateChildExpr(expr, index);
                }
                break;
            }
        case no_pselect:
        case no_select:
            generateExprCpp(expr->queryChild(0));
            out.append(getOpText(op));
            generateExprCpp(expr->queryChild(1));
            break;
        case no_if:
            {
                generateChildExpr(expr, 0);
                out.append(" ? ");
                queryBreakLine();
                generateChildExpr(expr, 1);
                out.append(" : ");
                queryBreakLine();
                generateChildExpr(expr, 2);
                break;
            }
        case no_list:
            {
                out.append('{');
                generateCommaChildren(expr);
                out.append('}');
                break;
            }
        case no_externalcall:
            {
                IHqlExpression *funcdef = expr->queryExternalDefinition();
                IHqlExpression *props = funcdef->queryChild(0);
                unsigned firstArg = 0;
                unsigned numArgs = expr->numChildren();

                if (props->hasAttribute(ctxmethodAtom))
                {
                    out.append("ctx->");
                }
                else if (props->hasAttribute(gctxmethodAtom))
                {
                    out.append("gctx->");
                }
                else if (props->hasAttribute(methodAtom))
                {
                    generateExprCpp(expr->queryChild(firstArg)).append("->");
                    ++firstArg;
                }
                else if (props->hasAttribute(omethodAtom))
                {
                    generateExprCpp(expr->queryChild(firstArg)).append(".");
                    ++firstArg;
                }
                if (props->hasAttribute(namespaceAtom))
                {
                    getAttribute(props, namespaceAtom, out);
                    out.append("::");
                }
                if (props->hasAttribute(entrypointAtom))
                    getAttribute(props, entrypointAtom, out);
                else
                    out.append(str(funcdef->queryBody()->queryId()));
                out.append('(');
                if (functionBodyUsesContext(props))
                {
                    out.append("ctx");
                    if (numArgs)
                        out.append(',');
                }
                else if (props->hasAttribute(globalContextAtom))
                {
                    out.append("gctx");
                    if (numArgs)
                        out.append(',');
                }
                for (unsigned index = firstArg; index < numArgs; index++)
                {
                    if (index != firstArg) out.append(',');
                    generateExprCpp(expr->queryChild(index));
                }
                out.append(')');
                break;
            }
        case no_cast:
        case no_implicitcast:
            {
                ITypeInfo * type = expr->queryType();
                IHqlExpression * child = expr->queryChild(0);
                if (hasWrapperModifier(child->queryType()))
                {
                    generateChildExpr(expr, 0).append(".");
                    switch (type->getTypeCode())
                    {
                    case type_string:
                    case type_varstring:
                    case type_qstring:
                    case type_utf8:
                        out.append("getstr()");
                        break;
                    case type_data:
                        out.append("getdata()");
                        break;
                    case type_row:
                        out.append("getbytes()");
                        break;
                    case type_set:
                    case type_dictionary:
                    case type_table:
                    case type_groupedtable:
                        if (hasStreamedModifier(type))
                            out.append("get()");
                        else if (hasLinkCountedModifier(type))
                            out.append("queryrows()");
                        else
                            out.append("getbytes()");
                        break;
                    case type_unicode:
                    case type_varunicode:
                        out.append("getustr()");
                        break;
                    default:
                        UNIMPLEMENTED;
                    }
                    break;
                }
                
                out.append("(");
                generateType(type, NULL);
                out.append(")");

                generateChildExpr(expr, 0);
            }
            break;
        case no_typetransfer:
            generateExprCpp(expr->queryChild(0));
            break;
        case no_translated:
#ifdef _DEBUG
            out.append("$translated$");//Cause a compile error.
#endif
            generateExprCpp(expr->queryChild(0));
            break;
        case no_order:
            generateOrderExpr(expr->queryChild(0), expr->queryChild(1));
            break;
        case no_index:
            generateChildExpr(expr, 0);
            out.append('[');
            generateChildExpr(expr, 1);
            out.append(']');
            break;
        case no_postinc:
        case no_postdec:
            generateChildExpr(expr, 0);
            out.append(getOpText(op));
            break;
        case no_reference:
            {
                IHqlExpression * child = expr->queryChild(0);
                generateChildExpr(expr, 0);
                if (hasWrapperModifier(child->queryType()))
                {
                    bool extend = expr->hasAttribute(extendAtom);
                    out.append(".");
                    ITypeInfo * type = expr->queryType();
                    switch (type->getTypeCode())
                    {
                    case type_string:
                    case type_varstring:
                    case type_qstring:
                    case type_utf8:
                        if (extend)
                            out.append("refexstr()");
                        else
                            out.append("refstr()");
                        break;
                    case type_data:
                        if (extend)
                            out.append("refexdata()");
                        else
                            out.append("refdata()");
                        break;
                    case type_row:
                        assertex(!extend);
                        out.append("getbytes()");
                        break;
                    case type_set:
                    case type_dictionary:
                    case type_table:
                    case type_groupedtable:
                        assertex(!extend);
                        if (hasLinkCountedModifier(type))
                            out.append("refrows()");
                        else
                            out.append("refdata()");
                        break;
                    case type_unicode:
                    case type_varunicode:
                        if (extend)
                            out.append("refexustr()");
                        else
                            out.append("refustr()");
                        break;
                    }
                }
                break;
            }
        case no_address:
            {
                IHqlExpression * child = expr->queryChild(0);
                ITypeInfo * childType = child->queryType();
                if (hasWrapperModifier(childType))
                {
                    generateChildExpr(expr, 0).append(".");
                    switch (childType->getTypeCode())
                    {
                    case type_string:
                    case type_varstring:
                    case type_qstring:
                    case type_utf8:
                        out.append("addrstr()");
                        break;
                    case type_data:
                        out.append("addrdata()");
                        break;
                    case type_row:
                        throwUnexpected();
                        out.append("getbytes()");       //????
                        break;
                    case type_set:
                    case type_dictionary:
                    case type_table:
                    case type_groupedtable:
                        if (hasLinkCountedModifier(childType))
                            out.append("addrrows()");
                        else
                            out.append("addrdata()");
                        break;
                    case type_unicode:
                    case type_varunicode:
                        out.append("addrustr()");
                        break;
                    default:
                        UNIMPLEMENTED;
                    }
                }
                else
                {
                    out.append(getOpText(op));
                    generateChildExpr(expr, 0);
                }
                break;
            }
        case no_negate: 
        case no_not: 
        case no_bnot: 
        case no_deref:
        case no_preinc:
        case no_predec:
            out.append(getOpText(op));
            generateChildExpr(expr, 0);
            break;
        case no_quoted:
        case no_variable:
            return expr->toString(out);
        case no_field:
            return expr->toString(out);     //MORE!!!
        case no_create_initializer:
            generateInitializer(expr->queryChild(0));
            break;
        case no_pure:
        case no_impure:
            generateExprCpp(expr->queryChild(0));
            break;
        case no_param:
            generateParamCpp(expr, NULL);
            break;
        case no_callback:
            {
                IHqlDelayedCodeGenerator * generator = (IHqlDelayedCodeGenerator *)expr->queryUnknownExtra();
                generator->generateCpp(out);
                break;
            }
        case no_funcdef:
            {
                IHqlExpression * body = expr->queryChild(0);
                assertex(body->getOperator() == no_external);
                IHqlExpression * entrypoint = queryAttributeChild(body, entrypointAtom, 0);
                getStringValue(out, entrypoint);
                break;
            }
        case no_nullptr:
            out.append("NULL");
            break;
//      case no_self:    return out.append("self");
        default:
            return expr->toString(out.append("<?")).append("?>");
    }
    return out;
}

StringBuffer & HqlCppWriter::generateChildExpr(IHqlExpression * expr, unsigned childIndex)
{
    IHqlExpression * child = expr->queryChild(childIndex);
    unsigned p = getPrecedence(expr);
    unsigned cp = getPrecedence(child);

    bool needBra = true;
    if (p < cp)
        needBra = false;
    else if (p == cp)
    {
        if (isCast(expr) && isCast(child))
            needBra = false;

        node_operator op = expr->getOperator();
        if (op == child->getOperator())
        {
            switch (op)
            {
            case no_and:
            case no_or:
            case no_add:
            case no_band:
            case no_bor:
                needBra = false;
                break;
            }
        }
    }

    if (!needBra)
        return generateExprCpp(child);
    out.append('(');
    return generateExprCpp(child).append(')');
}

StringBuffer & HqlCppWriter::generateCommaChildren(IHqlExpression * expr)
{
    unsigned numArgs = expr->numChildren();
    unsigned startLength = out.length();
    for (unsigned index = 0; index < numArgs; index++)
    {
        if (index != 0)
            out.append(',');
        unsigned newLength = out.length();
        if (newLength - startLength > PREFERRED_LINE_LIMIT)
        {
            newline().append("\t");
            startLength = out.length();
        }
        generateExprCpp(expr->queryChild(index));
    }
    return out;
}

StringBuffer & HqlCppWriter::generateExprAsChar(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
        case no_constant:
            {
                StringBuffer temp;
                IValue * value = expr->queryValue();
                value->getStringValue(temp);
                out.append((int)temp.charAt(0));
            }
            break;
        default:
            //create an indexed node and generate that - much better
            generateExprCpp(expr).append("[0]");
            break;
    }
    return out;
}


void HqlCppWriter::generateOrderExpr(IHqlExpression * left, IHqlExpression * right)
{
    ITypeInfo * lType = left->queryType();
    ITypeInfo * rType = right->queryType();
    ITypeInfo * lBaseType = lType;
    ITypeInfo * rBaseType = rType;

    if (lType->getTypeCode() == type_pointer)
        lBaseType = lType->queryChildType();
    if (rType->getTypeCode() == type_pointer)
        rBaseType = rType->queryChildType();

    assertex(isSameBasicType(lBaseType, rBaseType));
    switch (lBaseType->getTypeCode())
    {
        case type_string:
        case type_data:
        case type_qstring:
        case type_varstring:
        case type_unicode:
        case type_varunicode:
        case type_utf8:
            {
                throwUnexpectedType(lBaseType);
                break;
            }
        default:
            out.append("(");
            generateExprCpp(left).append(" < ");
            generateExprCpp(right).append(" ? -1 : ");
            generateExprCpp(left).append(" > ");
            generateExprCpp(right).append(" ? +1 : 0)");
            break;
    }
}


//---------------------------------------------------------------------------
HqlCppWriter::HqlCppWriter(StringBuffer & _out, CompilerType _compiler) : out(_out)
{ 
    curIndent = 0; 
    startOffset = 0;
    compiler = _compiler;
    outputLineNum = 1;
}

HqlCppWriter::HqlCppWriter(CompilerType _compiler) : out(defaultOut)
{ 
    curIndent = 0; 
    startOffset = 0;
    compiler = _compiler;
    outputLineNum = 1;
}


StringBuffer & HqlCppWriter::indent()
{
#ifdef INDENT_SOURCE
    unsigned i = curIndent;
    for (;i > 10;i-=10)
        out.append("\t\t\t\t\t\t\t\t\t\t");
    out.append(i, "\t\t\t\t\t\t\t\t\t\t");
#endif
    return out;
}

void HqlCppWriter::flush()
{
    if (target)
    {
        target->write(out.length(), out.str());
        out.clear();
        startOffset = 0;
    }
}

void HqlCppWriter::generateStmtForPass(IHqlStmt * stmt, unsigned pass)
{
    switch (stmt->getStmt())
    {
    case pass_stmt:
        if (stmt->queryExpr(0)->queryValue()->getIntValue() == pass)
            generateStmt(stmt);
        break;
    case indirect_stmt:
        {
            ForEachChild(i, stmt)
                generateStmtForPass(stmt->queryChild(i), pass);
            break;
        }
    default:
        generateStmt(stmt);
        break;
    }
}

void HqlCppWriter::generateStatementsForPass(HqlStmts & stmts, unsigned delta, unsigned pass)
{
    indent((int)delta);
    ForEachItemIn(i, stmts)
        generateStmtForPass(&stmts.item(i), pass);
    indent(-(int)delta);
    flush();
}

void HqlCppWriter::generate(HqlStmtArray & stmts)
{
    ForEachItemIn(idx, stmts)
    {
        IHqlStmt & cur = stmts.item(idx);
        generateStmt(&cur);
        switch (cur.getStmt())
        {
        case break_stmt:
        case return_stmt:
        case goto_stmt:
        case continue_stmt:
            //After these, any other expressions are irrelevant..
            return;
        }
    }
}

void HqlCppWriter::generateChildren(IHqlStmt * stmt, bool addBraces)
{
    if (addBraces)
    {
        queryIndent().append("{");
        newline();
        indent(1);
    }

    unsigned count = stmt->numChildren();
    for (unsigned index = 0; index < count; index++)
        generateStmt(stmt->queryChild(index));

    if (addBraces)
    {
        indent(-1);
        indent().append("}");
        newline();
    }
}

void HqlCppWriter::generateStmt(IHqlStmt * stmt)
{
    if (!stmt->isIncluded())
        return;

    unsigned kind = stmt->getStmt();
    switch (kind)
    {
        case assign_stmt:
            generateStmtAssign(stmt, false);
            break;
        case assign_link_stmt:
            generateStmtAssign(stmt, true);
            break;
        case block_stmt:
            generateChildren(stmt, true);
            break;
        case break_stmt:
            indent().append("break;");
            newline();
            break;
        case continue_stmt:
            indent().append("continue;");
            newline();
            break;
        case case_stmt:
            generateStmtCase(stmt);
            break;
        case declare_stmt:
        case external_stmt:
            generateStmtDeclare(stmt);
            break;
        case default_stmt:
            indent().append("default:");
            generateChildren(stmt, true);
            break;
        case expr_stmt:
            indent();
            generateExprCpp(stmt->queryExpr(0)).append(';');
            newline();
            break;
        case filter_stmt:
            generateStmtFilter(stmt);
            break;
        case goto_stmt:
            indent().append("goto ");
            generateExprCpp(stmt->queryExpr(0)).append(';');
            newline();
            break;
        case function_stmt:
            generateStmtFunction(stmt);
            break;
        case alias_stmt:
        case group_stmt:
        case pass_stmt:
        case indirect_stmt:
            generateChildren(stmt, false);
            break;
        case label_stmt:
            generateExprCpp(stmt->queryExpr(0)).append(": ;");
            newline();
            break;
        case loop_stmt:
            generateStmtLoop(stmt);
            break;
        case line_stmt:
            generateStmtLine(stmt);
            break;
        case quote_compoundopt_stmt:
        case quote_stmt:
        case quote_compound_stmt:
            {
                indent();
                unsigned prevLen = out.length();
                stmt->getTextExtra(out);
                outputLineNum += memcount(out.length()-prevLen,out.str()+prevLen,'\n');
                if (kind != quote_stmt)
                {
                    out.append(" {");
                    newline();
                    indent(1);
                    generateChildren(stmt, false);
                    indent(-1);
                    indent().append("}");
                    IHqlExpression * extra = stmt->queryExpr(0);
                    if (extra)
                        generateExprCpp(extra);
                    newline();
                }
                else
                    newline();
                break;
            }
        case return_stmt:
            {
                IHqlExpression * value = stmt->queryExpr(0);
                if (value)
                {
                    indent().append("return ");
                    generateExprCpp(stmt->queryExpr(0)).append(';');
                }
                else
                    indent().append("return;");
                newline();
                break;
            }
        case switch_stmt:
            generateStmtSwitch(stmt);
            break;
        case try_stmt:
            indent().append("try");
            generateChildren(stmt, true);
            break;
        case catch_stmt:
            generateStmtCatch(stmt);
            break;
        case throw_stmt:
            {
                IHqlExpression * value = stmt->queryExpr(0);
                if (value)
                {
                    indent().append("throw ");
                    generateExprCpp(stmt->queryExpr(0)).append(';');
                }
                else
                    indent().append("throw;");
                newline();
                break;
            }
        case assigninc_stmt:
        case assigndec_stmt:
            generateStmtAssignModify(stmt);
            break;
    }
}


void HqlCppWriter::generateSimpleAssign(IHqlExpression * target, IHqlExpression * source)
{
    indent();
    generateExprCpp(target).append(" = ");
    generateExprCpp(source).append(";");
}

void HqlCppWriter::generateStmtAssign(IHqlStmt * assign, bool link)
{
    IHqlExpression * target = assign->queryExpr(0);
    IHqlExpression * source = assign->queryExpr(1);

    ITypeInfo * type = target->queryType();
    const char * setFunction = link ? ".set(" : ".setown(";

    switch (type->getTypeCode())
    {
        case type_char:
        case type_int:
        case type_swapint:
        case type_packedint:
        case type_real:
        case type_boolean:
        case type_bitfield:
        case type_pointer:
        case type_enumerated:
        case type_record:
            generateSimpleAssign(target, source);
            break;
        case type_varstring:
        case type_varunicode:
            if (hasModifier(type, typemod_ref))
               generateSimpleAssign(target, source);
            else if (type->getSize() == UNKNOWN_LENGTH)
            {
                indent();
                generateExprCpp(target).append(setFunction);
                generateExprCpp(source).append(");");
            }
            else
                throwUnexpected();
            break;
        case type_dictionary:
        case type_table:
        case type_groupedtable:
            if (hasWrapperModifier(type))
            {
                if (hasLinkCountedModifier(type) && !hasStreamedModifier(type))
                {
                    assertex(source->getOperator() == no_complex);
                    indent();
                    generateExprCpp(target).append(setFunction);
                    generateExprCpp(source->queryChild(0)).append(",");
                    generateExprCpp(source->queryChild(1)).append(");");
                }
                else
                {
                    indent();
                    generateExprCpp(target).append(setFunction);
                    generateExprCpp(source).append(");");
                }
            }
            else
                generateSimpleAssign(target, source);
            break;
        case type_set:
        case type_row:
            if (hasWrapperModifier(type))
            {
                indent();
                generateExprCpp(target).append(setFunction);
                generateExprCpp(source).append(");");
            }
            else
               generateSimpleAssign(target, source);
            break;
        case type_qstring:
        case type_string:
        case type_data:
        case type_unicode:
        case type_utf8:
            if (hasModifier(type, typemod_ref))
               generateSimpleAssign(target, source);
            else
                throwUnexpected();
            break;
        default:
            if (hasModifier(type, typemod_ref))
               generateSimpleAssign(target, source);
            else
            {
                type->getTypeCode();
                assertex(!"Unexpected type assignment!");
                generateSimpleAssign(target, source); 
                out.append("$$BadType$$");
            }
            break;
    }
    newline();
}


void HqlCppWriter::generateStmtAssignModify(IHqlStmt * assign)
{
    IHqlExpression * target = assign->queryExpr(0);
    IHqlExpression * source = assign->queryExpr(1);

    ITypeInfo * type = target->queryType();

    switch (type->getTypeCode())
    {
        case type_row:
        case type_dictionary:
        case type_table:
        case type_groupedtable:
            //check it is a pointer increment
            assertex(hasReferenceModifier(type));
        case type_int:
        case type_real:
        case type_boolean:
        case type_pointer:
            indent();
            generateExprCpp(target);
            if (assign->getStmt() == assigninc_stmt)
                out.append(" += ");
            else
                out.append(" -= ");
            generateExprCpp(source).append(";");
            break;
        default:
            throwUnexpected();
            break;
    }
    newline();
}


void HqlCppWriter::generateStmtCase(IHqlStmt * stmt)
{
    queryIndent().append("case ");
    generateExprCpp(stmt->queryExpr(0)).append(":");

    unsigned childCount = stmt->numChildren();
    switch (childCount)
    {
    case 0:
        //if case label contains nothing then it is commoned up with the next case
        break;
    case 1:
        newline();
        indent(1);
        generateChildren(stmt, false);
        if (stmt->queryChild(childCount-1)->getStmt() != return_stmt)
        {
            indent().append("break;");
            newline();
        }
        indent(-1);
        break;
    default:
        generateChildren(stmt, true);
        if (stmt->queryChild(childCount-1)->getStmt() != return_stmt)
        {
            indent().append("break;");
            newline();
        }
    }
}

void HqlCppWriter::generateStmtCatch(IHqlStmt * stmt)
{
    IHqlExpression * caught = stmt->queryExpr(0);
    indent().append("catch (");
    if (caught)
    {
        generateParamCpp(caught, NULL);
    }
    else
        out.append("...");

    out.append(")");
    generateChildren(stmt, true);
}

void HqlCppWriter::generateStmtDeclare(IHqlStmt * declare)
{
    IHqlExpression * name = declare->queryExpr(0);
    IHqlExpression * value = declare->queryExpr(1);
    
    ITypeInfo * type = name->queryType();

    StringBuffer targetName;
    assertex(name->getOperator() == no_variable);
    name->toString(targetName);

    if (declare->getStmt() == external_stmt)
        out.append("extern ");

    indent();
    if (hasModifier(type, typemod_mutable))
        out.append("mutable ");

    size32_t typeSize = type->getSize();
    if (hasWrapperModifier(type))
    {
        ITypeInfo * builderModifier = queryModifier(type, typemod_builder);
        if (builderModifier)
        {
            IHqlExpression * size = static_cast<IHqlExpression *>(builderModifier->queryModifierExtra());
            if (size)
            {
                unsigned fixedSize = getIntValue(size);
                if (fixedSize == 0)
                    out.append("rtlEmptyRowBuilder ").append(targetName);
                else
                    out.append("rtlFixedRowBuilder<").append(fixedSize).append("> ").append(targetName);
            }
            else
                out.append("rtlRowBuilder ").append(targetName);
        }
        else if (hasStreamedModifier(type))
        {
            out.append("Owned<IRowStream> ").append(targetName);
        }
        else if (hasLinkCountedModifier(type))
        {
            if (type->getTypeCode() == type_row)
                out.append("rtlRowAttr ").append(targetName);
            else
                out.append("rtlRowsAttr ").append(targetName);
        }
        else if (typeSize != UNKNOWN_LENGTH)
            out.append("rtlFixedSizeDataAttr<").append(typeSize).append("> ").append(targetName);
        else
            out.append("rtlDataAttr ").append(targetName);
        if (value)
        {
            out.append("(");
            generateExprCpp(value);
            out.append(")");
            value = NULL;
        }
    }
    else
    {
        generateType(type, targetName.str());
    }

    if (value)
    {
        out.append(" = ");
        generateExprCpp(value);
    }
    out.append(";");
    
    newline();
}

void HqlCppWriter::generateStmtFilter(IHqlStmt * stmt)
{
    IHqlExpression * condition = stmt->queryExpr(0);
    IValue * value = condition->queryValue();
    //optimize generation of if (false) which can't be optimized earlier.
    if (value)
    {
        IHqlStmt * folded = (value->getBoolValue()) ? stmt->queryChild(0) : stmt->queryChild(1);
        if (folded)
        {
            if (folded->getStmt() == block_stmt)
                generateChildren(folded, false);
            else
                generateStmt(folded);
        }
    }
    else
    {
        indent().append("if (");
        generateExprCpp(condition).append(")");
        generateStmt(stmt->queryChild(0));

        IHqlStmt * elseCode = stmt->queryChild(1);
        if (elseCode)
        {
            indent().append("else");
            generateStmt(elseCode);
        }
    }
}


void HqlCppWriter::generateStmtFunction(IHqlStmt * stmt)
{
    IHqlExpression * funcdef = stmt->queryExpr(0);
    indent();
    generateFunctionPrototype(funcdef);
    generateChildren(stmt, true);
}


void HqlCppWriter::generateStmtLoop(IHqlStmt * stmt)
{
    IHqlExpression * cond = stmt->queryExpr(0);
    IHqlExpression * inc = stmt->queryExpr(1);
    bool atEnd = false;
    if (inc)
    {
        if (inc->isAttribute())
        {
            atEnd = true;
            inc = NULL;
        }
        else
        {
            if (stmt->queryExpr(2) != NULL)
                atEnd = true;
        }
    }

    if (atEnd)
    {
        indent().append("do {").newline();
        indent(1);
        generateChildren(stmt, false);
        if (inc)
        {
            indent();
            generateExprCpp(inc).append(";");
            newline();
        }
        indent(-1);
        indent().append("} while ("); 
        generateExprCpp(cond);
        out.append(");");
        newline();
    }
    else
    {
        indent().append("for (;"); 
        if (cond)
            generateExprCpp(cond);
        out.append(";");
        if (inc)
            generateExprCpp(inc);
        out.append(")");
        generateChildren(stmt, true);
    }
}

void HqlCppWriter::generateStmtLine(IHqlStmt * stmt)
{
    IHqlExpression * filename = stmt->queryExpr(0);
    IHqlExpression * line = stmt->queryExpr(1);

    if (filename && line)
    {
        out.append("#line ");
        generateExprCpp(line).append(" ");
        generateExprCpp(filename);
        newline();
    }
    else
    {
        //NB: Sets the line number of the next line...
        const char * targetFilename = targetFile->queryFilename();
        out.append("#line ").append(outputLineNum+1).append(" \"");
        appendStringAsCPP(out, strlen(targetFilename), targetFilename, false).append("\"");
        newline();
    }
}

void HqlCppWriter::generateStmtSwitch(IHqlStmt * stmt)
{
    indent().append("switch (");
    generateExprCpp(stmt->queryExpr(0)).append(")");
    generateChildren(stmt, true);
}



StringBuffer & HqlCppWriter::newline()
{
    outputLineNum++;
    out.newline();
    if (target && (out.length() > FILE_CHUNK_SIZE))
        flush();
    startOffset = out.length();
    return out;
}


StringBuffer & HqlCppWriter::queryIndent()
{
    if (out.length() - startOffset > REASONABLE_LINE_LIMIT)
        newline();
    if (out.length() == startOffset)
        return indent();
    return out.append(" ");
}

StringBuffer & HqlCppWriter::queryBreakLine()
{
    if (out.length() - startOffset > REASONABLE_LINE_LIMIT)
    {
        newline();
        indent().append("\t");
    }
    return out;
}

void HqlCppWriter::queryNewline()
{
    if (out.length() != startOffset)
        newline();
}

void HqlCppWriter::setOutput(IFile * _targetFile, IIOStream * _target)
{
    flush();
    targetFile.set(_targetFile);
    target.set(_target);
    out.ensureCapacity(FILE_CHUNK_SIZE + 2 * REASONABLE_LINE_LIMIT);
}

void HqlCppSectionWriter::generateSection(unsigned delta, IAtom * section, unsigned pass)
{
    HqlStmts * match = instance.querySection(section);
    if (match)
        writer.generateStatementsForPass(*match, delta, pass);
}

//---------------------------------------------------------------------------

ITemplateExpander * createTemplateExpander(IFile * output, const char * filename, const char *dir)
{
    Owned<CppWriterTemplate> expander = new CppWriterTemplate;
    if (expander->loadTemplate(filename, dir) || expander->loadTemplate(filename, ""))
    {
        expander->setOutput(output);
        return expander.getClear();
    }
    return NULL;
}

ISectionWriter * createCppWriter(IHqlCppInstance & _instance, CompilerType compiler)
{
    return new HqlCppSectionWriter(_instance, compiler);
}

extern HQLCPP_API StringBuffer & generateExprCpp(StringBuffer & out, IHqlExpression * expr, CompilerType compiler)
{
    HqlCppWriter writer(out, compiler);
    writer.generateExprCpp(expr);
    return out;
}

extern HQLCPP_API StringBuffer & generateTypeCpp(StringBuffer & out, ITypeInfo * type, const char * name, CompilerType compiler)
{
    HqlCppWriter writer(out, compiler);
    writer.generateType(type, name);
    return out;
}

void generateFunctionReturnType(StringBuffer & prefix, StringBuffer & params, ITypeInfo * retType, IHqlExpression * attrs, CompilerType compiler)
{
    HqlCppWriter writer(prefix, compiler);
    writer.generateFunctionReturnType(params, retType, attrs);
}


bool generateFunctionPrototype(StringBuffer & out, IHqlExpression * funcdef, CompilerType compiler)
{
    HqlCppWriter writer(out, compiler);
    return writer.generateFunctionPrototype(funcdef);
}
