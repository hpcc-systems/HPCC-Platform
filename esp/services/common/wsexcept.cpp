#include "platform.h"
#include "wsexcept.hpp"

class DECL_EXCEPTION CWsException : public CInterface, implements IWsException
{
public:
    IMPLEMENT_IINTERFACE

    CWsException (const char* source, WsErrorType errorType )
    {
        if (source)
            exSource.append(source);
        exErrorType = errorType;
    }

    CWsException( IMultiException& me, WsErrorType errorType )
    {
        append(me);
        exErrorType = errorType;

        const char* source = me.source();

        if (source)
            exSource.append(source);
    }
    CWsException( IException& e, const char* source, WsErrorType errorType )
    {
        IMultiException* me = dynamic_cast<IMultiException*>(&e);
        if ( me ) {
            append(*me);
        } else
            append(e);
        exErrorType = errorType;

        if (source)
            exSource.append(source);
    }

    //convenience methods for handling this as an array
    virtual aindex_t ordinality() const
    {
        synchronized block(mutex);
        return exceptions.ordinality();
    }
    virtual IException& item(aindex_t pos) const
    {
        synchronized block(mutex);
        return exceptions.item(pos);
    }
    virtual const char* source() const
    {
        synchronized block(mutex);
        return exSource.str();
    }

    //for complete control...caller is responsible for thread safety!
    virtual IArrayOf<IException>& getArray()     { return exceptions;              }

    // add another exception. Pass ownership to this obj:
    // i.e., caller needs to make sure that e has one consumable ref count
    virtual void append(IException& e)
    {
        synchronized block(mutex);
        exceptions.append(e);
    }
    virtual void append(IMultiException& me)
    {
        synchronized block(mutex);

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
                exceptions.append(*MakeStringExceptionDirect(e.errorAudience(), e.errorCode(), msg));
            }
            else
                exceptions.append(*LINK(&e));
        }
    }


    StringBuffer& serialize(StringBuffer& buffer, unsigned indent = 0, bool simplified=false, bool root=true) const
    {
        synchronized block(mutex);

        if (root)
            buffer.append("<Exceptions>");

        if (!simplified)
        {
            if (indent) buffer.append("\n\t");
            buffer.appendf("<Source>%s</Source>", exSource.str());
        }

        ForEachItemIn(i, exceptions)
        {
            IException& exception = exceptions.item(i);

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
                buffer.appendf("<Source>%s</Source>", exSource.str());
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
        synchronized block(mutex);
        return ordinality() == 1 ? item(0).errorCode() : -1;
    }
    virtual StringBuffer& errorMessage(StringBuffer &msg) const
    {
        synchronized block(mutex);
        ForEachItemIn(i, exceptions)
        {
            IException& e = item(i);

            StringBuffer buf;
            msg.appendf("[%3d: %s] ", e.errorCode(), e.errorMessage(buf).str());
        }
        return msg;
    }
    virtual MessageAudience errorAudience() const
    {
        synchronized block(mutex);
        return ordinality() == 1 ? item(0).errorAudience() : MSGAUD_programmer;
    }
    virtual WsErrorType errorType() const
    {
        synchronized block(mutex);
        return exErrorType;
    }
private:
    CWsException( const CWsException& );
    IArrayOf<IException> exceptions;
    StringBuffer         exSource;
    mutable Mutex        mutex;
    WsErrorType          exErrorType;
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
