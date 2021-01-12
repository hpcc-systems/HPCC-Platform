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

#include "platform.h"

#include "jlib.hpp"
#include "jlog.hpp"
#include "jsocket.hpp"
#include "jbuff.hpp"

#include "rmtsmtp.hpp"

class CSMTPValidator
{
public:
    CSMTPValidator() : value(NULL), finger(NULL), label(NULL), scanlist(false) {}

    void validateValue(char const * _value, char const * _label)
    {
        value = finger = _value;
        label = _label;
        while(*finger)
        {
            if(badChar(*finger))
                fail("illegal character");
            ++finger;
        }
    }

    void validateAddress(char const * _address, char const * _label)
    {
        value = finger = _address;
        label = _label;
        scanlist = false;
        validateLocalPart();
        validateDomain();
    }

    void scanAddressListStart(char const * _addrlist, char const * _label)
    {
        value = finger = _addrlist;
        label = _label;
        if(!skipListSep())
            fail("empty address list");
        scanlist = true;
    }

    bool scanAddressListNext(StringBuffer & out)
    {
        if(!scanlist)
            return false;
        char const * start = finger;
        validateLocalPart();
        scanlist = validateDomain();
        out.append(finger-start, start);
        if(scanlist)
            scanlist = skipListSep();
        return true;
    }

    void escapeQuoted(char const * in, StringBuffer & out, char const * _label)
    {
        value = finger = in;
        label = _label;
        while(*finger)
        {
            if(badChar(*finger))
                fail("illegal character");
            else if((*finger == '"') || (*finger == '\\'))
            {
                if(finger>in)
                    out.append(finger-in, in);
                out.append('\\');
                in = finger;
            }
            ++finger;
        }
        if(finger>in)
            out.append(finger-in, in);
    }

private:
    bool skipListSep()
    {
        while(*finger && isListSep(*finger))
            ++finger;
        return (*finger != 0);
    }

    void validateLocalPart()
    {
        if(*finger == '"')
            validateQuotedLocal();
        else
            validateDotStringLocal();
    }

    bool validateDomain()
    {
        if(*finger == '[')
            return validateAddressLiteral();
        else
            return validateNamedDomain();
    }

    void validateQuotedLocal()
    {
        ++finger;
        while(*finger != '"')
        {
            if(*finger == '\\')
            {
                ++finger;
                if(!*finger)
                    fail("unexpected end-of-string in quoted local part");
                else if(badChar(*finger))
                    fail("illegal escaped character in quoted local part");
            }
            else if(!*finger)
                fail("unexpected end-of-string in quoted local part");
            else if(badQuotedChar(*finger))
                fail("illegal character in quoted local part (may need escaping)");
            ++finger;
        }
        ++finger;
        if(!*finger)
            fail("address had quoted local part but no domain (reached end-of-string)");
        else if(*finger != '@')
            fail("quoted local part was not followed by @");
        ++finger;
    }

    void validateDotStringLocal()
    {
        enum { Start, StartAtom, Main };
        unsigned mode = Start;
        while(*finger != '@')
        {
            if(*finger == '.')
                switch(mode)
                {
                case Start:
                    fail("illegal . at start of local part");
                case StartAtom:
                    fail("illegal .. in local part");
                case Main:
                    mode = StartAtom;
                    break;
                }
            else if(!*finger)
                if(mode == Start)
                    fail("blank address (reached end-of-string)");
                else
                    fail("address had dotted-atom-string local part but no domain (reached end-of-string)");
            else if(scanlist && isListSep(*finger))
                if(mode == Start)
                    fail("blank address (reached comma/semicolon/space indicating next address in list)");
                else
                    fail("address had dotted-atom-string local part but no domain (reached comma/semicolon/space indicating next address in list)");
            else if(badAtomChar(*finger))
                fail("illegal character in dotted-atom-string local part (may need quoting)");
            else
                mode = Main;
            ++finger;
        }
        switch(mode)
        {
        case Start:
            fail("empty local part");
        case StartAtom:
            fail("illegal . at end of local part");
        }
        ++finger;
    }

    bool validateAddressLiteral()
    {
        ++finger;
        unsigned digitcount = 0;
        unsigned groupcount = 0;
        while(*finger != ']')
        {
            if(isdigit(*finger))
                if(digitcount == 3)
                    fail("more than three digits in octet in address literal");
                else
                    ++digitcount;
            else if(*finger == '.')
                if(digitcount == 0)
                {
                    fail("empty octet in address literal");
                }
                else
                {
                    digitcount = 0;
                    ++groupcount;
                    if(groupcount == 4)
                        fail("too many octets in address literal (sorry, only IPv4 supported)");
                }
            else if(!*finger)
                fail("unexpected end-of-string in address literal");
            else
                fail("illegal character in address literal (sorry, only IPv4 supported)");
            ++finger;
        }
        if(digitcount == 0)
            fail("empty octet in address literal");
        digitcount = 0;
        ++groupcount;
        if(groupcount < 4)
            fail("too few octets in address literal");
        ++finger;
        if(scanlist && isListSep(*finger))
            return true;
        if(*finger)
            fail("unexpected character after end of address literal");
        return false;
    }

    bool validateNamedDomain()
    {
        unsigned subcount = 0;
        unsigned charcount = 0;
        bool ret = false;
        while(*finger)
        {
            if(isalnum(*finger))
                ++charcount;
            else if(*finger == '_' || *finger == '-')
                if(charcount == 0)
                    fail("illegal character at start of subdomain");
                else if(!*(finger+1) || (*(finger+1) == '.') || (scanlist && isListSep(*(finger+1))))
                    fail("illegal character at end of subdomain");
                else
                    ++charcount;
            else if(*finger == '.')
                if(charcount == 0)
                    if(subcount == 0)
                        fail("illegal . at start of domain");
                    else
                        fail("illegal .. in domain");
                else
                {
                    ++subcount;
                    charcount = 0;
                }
            else if(scanlist && isListSep(*finger))
            {
                ret = true;
                break;
            }
            else
                fail("illegal character in domain");
            ++finger;
        }
        if(charcount == 0)
        {
            if(subcount == 0)
                fail("empty domain");
            else
                fail("illegal . at end of domain");
        }
        ++subcount;
        if(subcount < 2)
            fail("domain has only 1 subdomain");
        return ret;
    }

    void fail(char const * msg)
    {
        throw MakeStringException(0, "bad %s (%s at character %u): %s", label, msg, (unsigned) (finger-value), value);
    }

    bool badAtomChar(char c)
    {
        if((c<33) || (c>126)) return true;
        switch(c)
        {
        case '"':
        case '(':
        case ')':
        case ',':
        case '.':
        case ':':
        case ';':
        case '<':
        case '>':
        case '@':
        case '[':
        case '\\':
        case ']':
            return true;
        }
        return false;
    }

    bool badQuotedChar(char c)
    {
        if((c < 1) || (c>126)) return true;
        switch(c)
        {
        case '\t':
        case '\r':
        case '\n':
        case ' ':
        case '"':
        case '\\':
            return true;
        }
        return false;
    }

    bool badChar(char c)
    {
        if((c < 1) || (c>126)) return true;
        switch(c)
        {
        case '\r':
        case '\n':
            return true;
        }
        return false;
    }

    bool isListSep(char c)
    {
        switch(c)
        {
        case ',':
        case ';':
        case ' ':
            return true;
        }
        return false;
    }

private:
    char const * value;
    char const * finger;
    char const * label;
    bool scanlist;
};

// escapes text for mail transfer, returns true if quoted-printable encoding was required

bool mailEncode(char const * in, StringBuffer & out)
{
    bool esc = false;
    size32_t outlinelen = 0;
    char const * finger = in;
    while(*finger)
    {
        //max line length 76, use soft line break =\r\n to split (RFC 1521 section 5.1 rule #5)
        if(outlinelen+finger-in == 75)
        {
            out.append(finger-in, in).append("=\r\n");
            outlinelen = 0;
            in = finger;
            esc = true;
        }

        //printable chars except = and - and . are left alone (RFC 1521 section 5.1 rule #2)
        if((*finger >= 33) && (*finger <= 126) && (*finger != '=') && (*finger != '-') && (*finger != '.'))
        {
            ++finger;
            continue;
        }

        //- is left alone, except for -- at start of line to protect multipart boundary (RFC 1341 section 7.2.1)
        if(*finger == '-')
        {
            if((outlinelen != 0) || (*(finger+1) != '-'))
            {
                ++finger;
                continue;
            }
        }

        //. is left alone, except that an extra . is added when at start of line to protect SMTP 'end of data' signal (RFC 8211 section 4.5.2)
        if(*finger == '.')
        {
            if(outlinelen == 0)
            {
                out.append('.');
                ++outlinelen;
            }
            ++finger;
            continue;
        }

        //tab and space are left alone except at EOL (RFC 1521 section 5.1 rule #3)
        if((*finger == '\t') || (*finger == ' '))
        {
            char nxt = *(finger+1);
            if(nxt && (nxt != 10) && (nxt != 13))
            {
                ++finger;
                continue;
            }
        }

        //CR, LF, and CRLF are all converted to CRLF (RFC 1521 section 5.1 rule #4)
        if(*finger == 10)
        {
            if(finger>in)
                out.append(finger-in, in);
            ++finger;
            if(*finger == 13)
                ++finger;
            out.append("\r\n");
            outlinelen = 0;
            in = finger;
            continue;
        }
        if(*finger == 13)
        {
            if(finger>in)
                out.append(finger-in, in);
            ++finger;
            out.append("\r\n");
            outlinelen = 0;
            in = finger;
            continue;
        }

        //everything else is escaped (RFC 1521 section 5.1 rule #1)
        if(finger>in)
            out.append(finger-in, in);
        if(outlinelen+finger-in > 72)
        {
            out.append("=\r\n");
            outlinelen = 3;
        }
        else
        {
            outlinelen += (finger-in)+3;
        }
        out.appendf("=%02X", (unsigned char)*finger);
        in = ++finger;
        esc = true;
    }
    if(finger > in)
        out.append(finger-in, in);
    return esc;
}

//#define SMTP_TRACE

class CMailInfo
{
    StringArray *warnings;
    StringArray recipients;
    StringBuffer to;
    StringBuffer cc;
    StringBuffer bcc;
    StringAttr subject;
    StringAttr mailServer;
    unsigned port;
    StringAttr sender;
    Owned<ISocket> socket;
    StringBuffer lastAction;
    char inbuff[200];
    unsigned inlen;
    bool highPriority;

    static char const * toHeader;
    static char const * ccHeader;
    static char const * subjectHeader;
    static char const * senderHeader;
public:
    CMailInfo(char const * _to, char const * _cc, char const * _bcc, char const * _subject, char const * _mailServer, unsigned _port, char const * _sender, StringArray *_warnings, bool _highPriority)
        : subject(_subject), mailServer(_mailServer), port(_port), sender(_sender), lastAction("process initialization"), inlen(0), highPriority(_highPriority)
    {
        warnings = _warnings;
        CSMTPValidator validator;
        if(strlen(senderHeader) + sender.length() > 998)
            throw MakeStringException(0, "email sender address too long: %" I64F "u characters",  static_cast<__uint64>(sender.length()));
        validator.validateAddress(sender.get(), "email sender address");

        getRecipients(validator, _to, to);

        if (_cc && _cc[0])
            getRecipients(validator, _cc, cc);

        if (_bcc && _bcc[0])
            getRecipients(validator, _bcc, bcc);

        if(strlen(subjectHeader) + subject.length() > 998)
            throw MakeStringException(0, "Email subject too long: %" I64F "u characters",  static_cast<__uint64>(subject.length()));
        validator.validateValue(subject.get(), "email subject");
    }

    void open()
    {
        SocketEndpoint address(mailServer.get());
        if (address.isNull())
            throw MakeStringException(MSGAUD_operator, 0, "Could not resolve mail server address %s in SendEmail*", mailServer.get());
        address.port = port;
        try
        {
            socket.setown(ISocket::connect(address));
        }
        catch(IException *E)
        {
            E->Release();
            throw MakeStringException(MSGAUD_operator, 0, "Failed to connect to mail server at %s:%u in SendEmail*", mailServer.get(), port);
        }
        lastAction.clear().append("connection to server");
    }

    void write(char const * out, size32_t len, char const * action = NULL)
    {
        if(action)
            lastAction.clear().append(action);
        else
            lastAction.clear().append(len, out).clip();
        try
        {
            socket->write(out, len);
#ifdef SMTP_TRACE
            DBGLOG("SMTP write: [%s]", out);
#endif
        }
        catch(IException * e)
        {
            int code = e->errorCode();
            StringBuffer buff;
            e->errorMessage(buff);
            e->Release();
            throw MakeStringException(MSGAUD_operator, 0, "Exception %d (%s) in SendEmail* while writing %s to mail server %s:%u", code, buff.str(), lastAction.str(), mailServer.get(), port);
        }
    }

    void read()
    {
        try
        {
            socket->read(inbuff,1,sizeof(inbuff),inlen);
            //MORE: the following is somewhat primitive and not RFC compliant (see bug 25951) - but it is a lot better than nothing
            if((*inbuff == '4') || (*inbuff == '5'))
            {
                StringBuffer b;
                b.append("Negative reply from mail server at ").append(mailServer.get()).append(":").append(port).append(" after writing ").append(lastAction.str()).append(" in SendEmail*: ").append(inlen, inbuff).clip();
                WARNLOG("%s", b.str());
                if (warnings)
                    warnings->append(b.str());
            }
#ifdef SMTP_TRACE
            else
            {
                StringBuffer b(inlen, inbuff);
                b.clip();
                DBGLOG("SMTP read: [%s]", b.str());
            }
#endif
        }
        catch(IException * e)
        {
            int code = e->errorCode();
            StringBuffer buff;
            e->errorMessage(buff);
            e->Release();
            throw MakeStringException(MSGAUD_operator, 0, "Exception %d (%s) in SendEmail* while reading from mail server %s:%u following %s", code, buff.str(), mailServer.get(), port, lastAction.str());
        }
    }

    void getHeader(StringBuffer & header) const
    {
        header.append(senderHeader).append(sender.get()).append("\r\n");
        header.append(toHeader).append(to.str()).append("\r\n");
        if (!cc.isEmpty())
            header.append(ccHeader).append(cc.str()).append("\r\n");
        // Do not append bcc (that's what makes it "blind")
        header.append(subjectHeader).append(subject.get()).append("\r\n");
        if (highPriority)
        {
            header.append("X-Priority: 1\r\n");
            header.append("Importance: high\r\n");
        }
        header.append("MIME-Version: 1.0\r\n");
    }

    void getHelo(StringBuffer & out) const
    {
        out.append("HELO ").append(mailServer.get()).append("\r\n");
    }

    void getMailFrom(StringBuffer & out) const
    {
        out.append("MAIL FROM:<").append(sender.get()).append(">\r\n");
    }

    unsigned numRecipients() const
    {
        return recipients.ordinality();
    }

    void getRecipient(unsigned i, StringBuffer & out) const
    {
        char const * rcpt = recipients.item(i);
        out.append("RCPT TO:<").append(rcpt).append(">\r\n");
    }

private:
    void getRecipients(CSMTPValidator & validator, char const * _to, StringBuffer &destBuffer)
    {
        StringBuffer rcpt;
        validator.scanAddressListStart(_to, "recipient email address list");
        while(validator.scanAddressListNext(rcpt.clear()))
        {
            if(!destBuffer.isEmpty())
                destBuffer.append(",");
            destBuffer.append(rcpt.str());
            recipients.append(rcpt.str());
        }
    }

};

char const * CMailInfo::toHeader = "To: ";
char const * CMailInfo::ccHeader = "Cc: ";
char const * CMailInfo::subjectHeader = "Subject: ";
char const * CMailInfo::senderHeader = "From: ";

class CMailPart
{
public:
    CMailPart(char const * mimeType, char const * filename)
    {
        if(strlen(mimeTypeHeader) + strlen(mimeType) > 998)
            throw MakeStringException(0, "Email attachment mime type too long: %u characters", (unsigned) strlen(mimeType));
        CSMTPValidator validator;
        validator.validateValue(mimeType, "email attachment mime type");
        mime.append(mimeType);

        if(filename)
        {
            StringBuffer qfilename;
            validator.escapeQuoted(filename, qfilename, "email attachment filename");
            if(strlen(dispositionHeader) + strlen("attachment; filename=\"\"") + qfilename.length() > 998)
                throw MakeStringException(0, "Email attachment filename too long: %u characters", (unsigned) strlen(filename));
            disposition.append("attachment; filename=\"").append(qfilename.str()).append("\"");
        }
        else
        {
            disposition.append("inline");
        }

        encoding = NULL;
    }

    void getHeader(StringBuffer & header) const
    {
        header.append(mimeTypeHeader).append(mime.str()).append("\r\n");
        header.append(dispositionHeader).append(disposition).append("\r\n");
        if(encoding)
            header.append(encodingHeader).append(encoding).append("\r\n");
    }

    virtual void write(CMailInfo & info) const = 0;

protected:
    char const * encoding;
    StringBuffer mime;
    StringBuffer disposition;

private:
    static char const * mimeTypeHeader;
    static char const * dispositionHeader;
    static char const * encodingHeader;
};

char const * CMailPart::mimeTypeHeader = "Content-Type: ";
char const * CMailPart::dispositionHeader = "Content-Disposition: ";
char const * CMailPart::encodingHeader = "Content-Transfer-Encoding: ";

class CTextMailPart : public CMailPart
{
public:
    CTextMailPart(char const * text, char const * mimeType, char const * filename) : CMailPart(mimeType, filename)
    {
        if(mailEncode(text, buff))
            encoding = "quoted-printable";
    }

    void write(CMailInfo & info) const
    {
        info.write(buff.str(), buff.length(), "mail body");
    }

private:
    StringBuffer buff;
};

class CDataMailPart : public CMailPart
{
public:
    CDataMailPart(size32_t len, const void * data, char const * mimeType, char const * filename) : CMailPart(mimeType, filename)
    {
        JBASE64_Encode(data, len, buff, true);
        encoding = "base64";
    }

    void write(CMailInfo & info) const
    {
        info.write(buff.str(), buff.length(), "mail body");
    }

private:
    StringBuffer buff;
};

class CMultiMailPart : public CMailPart
{
public:
    CMultiMailPart(CMailPart const & _inlined, CMailPart const & _attachment) : CMailPart("multipart/mixed", NULL), inlined(_inlined), attachment(_attachment)
    {
        unsigned char rndm[12];
        for(unsigned i=0; i<12; ++i)
            rndm[i] = getRandom() % 256;
        JBASE64_Encode(rndm, 12, boundary, true);
        mime.append("; boundary=\"").append(boundary).append("\"");
    }

    void write(CMailInfo & info) const
    {
        writePart(inlined, info);
        writePart(attachment, info);
        writePartEnd(info);
    }

private:
    void writePart(CMailPart const & part, CMailInfo & info) const
    {
        StringBuffer outbuff;
        outbuff.append("\r\n").append("--").append(boundary).append("\r\n");
        part.getHeader(outbuff);
        outbuff.append("\r\n");
        info.write(outbuff.str(), outbuff.length(), "mail body");
        part.write(info);
    }

    void writePartEnd(CMailInfo & info) const
    {
        StringBuffer outbuff;
        outbuff.append("\r\n").append("--").append(boundary).append("--").append("\r\n");
        info.write(outbuff.str(), outbuff.length(), "mail body");
    }

private:
    StringBuffer boundary;
    CMailPart const & inlined;
    CMailPart const & attachment;
};

static const char *data="DATA\r\n";
static const char *endMail="\r\n\r\n.\r\n";
static const char *quit="QUIT\r\n";

static void doSendEmail(CMailInfo & info, CMailPart const & part)
{
    info.open();
    StringBuffer outbuff;

    info.read();
    info.getHelo(outbuff);
    info.write(outbuff.str(), outbuff.length());
    info.read();

    info.getMailFrom(outbuff.clear());
    info.write(outbuff.str(), outbuff.length());
    info.read();

    unsigned numRcpt = info.numRecipients();
    for(unsigned i=0; i<numRcpt; ++i)
    {
        info.getRecipient(i, outbuff.clear());
        info.write(outbuff.str(), outbuff.length());
        info.read();
    }

    info.write(data, strlen(data));
    info.read();
    info.getHeader(outbuff.clear());
    part.getHeader(outbuff);
    outbuff.append("\r\n");
    info.write(outbuff.str(), outbuff.length(), "mail header");
    part.write(info);
    info.write(endMail, strlen(endMail), "end of mail body");
    info.read();

    info.write(quit, strlen(quit));
    info.read();
}

void sendEmail(const char * to, const char * cc, const char * bcc, const char * subject, const char * body, const char * mailServer, unsigned port, const char * sender, StringArray *warnings, bool highPriority)
{
    CMailInfo info(to, cc, bcc, subject, mailServer, port, sender, warnings, highPriority);
    CTextMailPart bodyPart(body, "text/plain; charset=ISO-8859-1", NULL);
    doSendEmail(info, bodyPart);
}

void sendEmail(const char * to, const char * subject, const char * body, const char * mailServer, unsigned port, const char * sender, StringArray *warnings, bool highPriority)
{
    sendEmail(to, nullptr, nullptr, subject, body, mailServer, port, sender, warnings, highPriority);
}

void sendEmailAttachText(const char * to, const char * cc, const char * bcc, const char * subject, const char * body, const char * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender, StringArray *warnings, bool highPriority)
{
    CMailInfo info(to, cc, bcc, subject, mailServer, port, sender, warnings, highPriority);
    CTextMailPart inlinedPart(body, "text/plain; charset=ISO-8859-1", NULL);
    CTextMailPart attachmentPart(attachment, mimeType, attachmentName);
    CMultiMailPart multiPart(inlinedPart, attachmentPart);
    doSendEmail(info, multiPart);
}

void sendEmailAttachText(const char * to, const char * subject, const char * body, const char * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender, StringArray *warnings, bool highPriority)
{
    sendEmailAttachText(to, nullptr, nullptr, subject, body, attachment, mimeType, attachmentName, mailServer, port, sender, warnings, highPriority);
}

void sendEmailAttachData(const char * to, const char * cc, const char * bcc, const char * subject, const char * body, size32_t lenAttachment, const void * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender, StringArray *warnings, bool highPriority)
{
    CMailInfo info(to, cc, bcc, subject, mailServer, port, sender, warnings, highPriority);
    CTextMailPart inlinedPart(body, "text/plain; charset=ISO-8859-1", NULL);
    CDataMailPart attachmentPart(lenAttachment, attachment, mimeType, attachmentName);
    CMultiMailPart multiPart(inlinedPart, attachmentPart);
    doSendEmail(info, multiPart);
}

void sendEmailAttachData(const char * to, const char * subject, const char * body, size32_t lenAttachment, const void * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender, StringArray *warnings, bool highPriority)
{
    sendEmailAttachData(to, nullptr, nullptr, subject, body, lenAttachment, attachment, mimeType, attachmentName, mailServer, port, sender, warnings, highPriority);
}

