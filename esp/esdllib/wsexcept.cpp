/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#include "platform.h"
#include "wsexcept.hpp"

class DECL_EXCEPTION CWsException : implements IWsException, public CInterface
{
public:
    IMPLEMENT_IINTERFACE

    CWsException (const char* source, WsErrorType errorType )
    {
        if (source)
            source_.append(source);
        errorType_ = errorType;
    }
    CWsException( IMultiException& me, WsErrorType errorType )
    {
        append(me);
        errorType_ = errorType;

        const char* source = me.source();

        if (source)
            source_.append(source);
    }
    CWsException( IException& e, const char* source, WsErrorType errorType )
    {
        IMultiException* me = dynamic_cast<IMultiException*>(&e);
        if ( me ) {
            append(*me);
        } else
            append(e);
        errorType_ = errorType;

        if (source)
            source_.append(source);
    }

    //convenience methods for handling this as an array
    virtual aindex_t ordinality() const
    {
        synchronized block(m_mutex);
        return array_.ordinality();
    }
    virtual IException& item(aindex_t pos) const
    {
        synchronized block(m_mutex);
        return array_.item(pos);
    }
    virtual const char* source() const
    {
        synchronized block(m_mutex);
        return source_.str();
    }

    //for complete control...caller is responsible for thread safety!
    virtual IArrayOf<IException>& getArray()     { return array_;              }

    // add another exception. Pass ownership to this obj:
    // i.e., caller needs to make sure that e has one consumable ref count
    virtual void append(IException& e)
    {
        synchronized block(m_mutex);
        array_.append(e);
    }
    virtual void append(IMultiException& me)
    {
        synchronized block(m_mutex);

        IArrayOf<IException>& exceptions = me.getArray();
        const char* source = me.source();
        ForEachItemIn(i, exceptions)
        {
            IException& e = exceptions.item(i);
            if (source && *source)
            {
                StringBuffer msg;
                msg.appendf("[%s] ",source);
                e.errorMessage(msg);
                array_.append(*MakeStringExceptionDirect(e.errorAudience(), e.errorCode(), msg));
            }
            else
                array_.append(*LINK(&e));
        }
    }


    StringBuffer& serialize(StringBuffer& buffer, unsigned indent = 0, bool simplified=false, bool root=true) const
    {
        synchronized block(m_mutex);

        if (root)
            buffer.append("<Exceptions>");

        if (!simplified)
        {
            if (indent) buffer.append("\n\t");
            buffer.appendf("<Source>%s</Source>", source_.str());
        }

        ForEachItemIn(i, array_)
        {
            IException& exception = array_.item(i);

            if (indent) buffer.append("\n\t");
            buffer.append("<Exception>");

            if (indent) buffer.append("\n\t\t");
            buffer.appendf("<Code>%d</Code>", exception.errorCode());

            if (indent) buffer.append("\n\t\t");
            buffer.appendf("<Audience>%s</Audience>", serializeMessageAudience( exception.errorAudience() ));

            if (simplified)
            {
                if (indent) buffer.append("\n\t\t");
                StringBuffer msg;
                buffer.appendf("<Source>%s</Source>", source_.str());
            }

            if (indent) buffer.append("\n\t\t");

            StringBuffer msg;
            StringBuffer encoded;
            encodeXML(exception.errorMessage(msg).str(), encoded);
            buffer.appendf("<Message>%s</Message>", encoded.str());

            if (indent) buffer.append("\n\t");
            buffer.append("</Exception>");
        }

        if (root)
            buffer.append("</Exceptions>");
        return buffer;
    }

    virtual int errorCode() const
    {
        synchronized block(m_mutex);
        return ordinality() == 1 ? item(0).errorCode() : -1;
    }
    virtual StringBuffer& errorMessage(StringBuffer &msg) const
    {
        synchronized block(m_mutex);
        ForEachItemIn(i, array_)
        {
            IException& e = item(i);

            StringBuffer buf;
            msg.appendf("[%3d: %s] ", e.errorCode(), e.errorMessage(buf).str());
        }
        return msg;
    }
    virtual MessageAudience errorAudience() const
    {
        synchronized block(m_mutex);
        return ordinality() == 1 ? item(0).errorAudience() : MSGAUD_unknown;
    }
    virtual WsErrorType errorType() const
    {
        synchronized block(m_mutex);
        return errorType_;
    }
private:
    CWsException( const CWsException& );
    IArrayOf<IException> array_;
    StringBuffer         source_;
    mutable Mutex        m_mutex;
    WsErrorType          errorType_;
};

IWsException esdl_decl *makeWsException(IMultiException& me, WsErrorType errorType)
{
    return new CWsException(me, errorType);
}
IWsException esdl_decl *makeWsException(IException& e, WsErrorType errorType, const char* source)
{
    return new CWsException(  e, source, errorType );
}
IWsException esdl_decl *makeWsException(int errorCode, WsErrorType errorType, const char* source, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IException *e = MakeStringExceptionVA(errorCode, format, args);
    va_end(args);

    return new CWsException(  *e, source, errorType );
}
IWsException esdl_decl *makeWsException(const char *source, WsErrorType errorType)
{
    return new CWsException(source, errorType);
}
