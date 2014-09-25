#include "platform.h"
#include "wsexcept.hpp"

class CWsException : public CInterface, implements IWsException
{
public:
    IMPLEMENT_IINTERFACE

    CWsException (const char* source, WsErrorType errorType )
    {
        if (source)
            m_source.append(source);
        m_errorType = errorType;
    }

    CWsException( IMultiException& me, WsErrorType errorType )
    {
        append(me);
        m_errorType = errorType;

        const char* source = me.source();

        if (source)
            m_source.append(source);
    }
    CWsException( IException& e, const char* source, WsErrorType errorType )
    {
        IMultiException* me = dynamic_cast<IMultiException*>(&e);
        if ( me ) {
            append(*me);
        } else
            append(e);
        m_errorType = errorType;

        if (source)
            m_source.append(source);
    }

    //convenience methods for handling this as an array
    virtual aindex_t ordinality() const
    {
        synchronized block(m_mutex);
        return m_array.ordinality();
    }
    virtual IException& item(aindex_t pos) const
    {
        synchronized block(m_mutex);
        return m_array.item(pos);
    }
    virtual const char* source() const
    {
        synchronized block(m_mutex);
        return m_source.str();
    }

    //for complete control...caller is responsible for thread safety!
    virtual IArrayOf<IException>& getArray()     { return m_array;              }

    // add another exception. Pass ownership to this obj:
    // i.e., caller needs to make sure that e has one consumable ref count
    virtual void append(IException& e)
    {
        synchronized block(m_mutex);
        m_array.append(e);
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
                m_array.append(*MakeStringExceptionDirect(e.errorAudience(), e.errorCode(), msg));
            }
            else
                m_array.append(*LINK(&e));
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
            buffer.appendf("<Source>%s</Source>", m_source.str());
        }

        ForEachItemIn(i, m_array)
        {
            IException& exception = m_array.item(i);

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
                buffer.appendf("<Source>%s</Source>", m_source.str());
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
        ForEachItemIn(i, m_array)
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
        return m_errorType;
    }
private:
    CWsException( const CWsException& );
    IArrayOf<IException> m_array;
    StringBuffer         m_source;
    mutable Mutex        m_mutex;
    WsErrorType          m_errorType;
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
