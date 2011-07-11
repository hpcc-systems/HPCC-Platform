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

#ifndef _ESPSESSION_IPP__
#define _ESPSESSION_IPP__
#include "esp.hpp"
#include "jliball.hpp"

class CEspCookie : public CInterface, implements IInterface
{
private:
    int m_version;

    StringAttr m_host;

    StringAttr m_name;
    StringAttr m_value;
    StringAttr m_comment;
    StringAttr m_commentURL;
    StringAttr m_domain;
    StringAttr m_ports;
    int        m_maxage;
    StringAttr m_path;
    bool       m_secure;
    bool       m_discard;

public:
    IMPLEMENT_IINTERFACE;

    CEspCookie(const char* name, const char* value, int version=0)
    {
        m_name.set(name);
        m_value.set(value);

        m_version = version;

        m_secure = false;
        m_discard = false;
        m_path.set("/");
    }
    
    void getSetCookieHeaderName(StringBuffer& headername)
    {
        if(m_version >= 1)
            headername.append("Set-Cookie2");
        else
            headername.append("Set-Cookie");
    }

    void getCookieHeaderName(StringBuffer& headername)
    {
        if(m_version >= 1)
            headername.append("Cookie2");
        else
            headername.append("Cookie");
    }

    const char* getName()
    {
        return m_name.get();
    }

    const char* getValue()
    {
        return m_value.get();
    }

    void setValue(const char* newValue)
    {
        m_value.set(newValue);
    }

    const char* getComment()
    {
        return m_comment.get();
    }
    void setComment(const char* purpose)
    {
        m_comment.set(purpose);
    }

    const char* getCommentURL()
    {
        return m_commentURL.get();
    }
    void setCommentURL(const char* curl)
    {
        m_commentURL.set(curl);
    }

    const char* getDomain()
    {
        return m_domain.get();
    }
    void setDomain(const char* domain)
    {
        m_domain.set(domain);
    }

    const char* getPorts()
    {
        return m_ports.get();
    }
    void setPorts(const char* ports)
    {
        m_ports.set(ports);
    }

    int getMaxAge()
    {
        return m_maxage;
    }
    void setMaxAge(int age)
    {
        m_maxage = age;
    }

    const char* getPath()
    {
        return m_path.get();
    }
    void setPath(const char* path)
    {
        m_path.set(path);
    }

    const char* getHost()
    {
        return m_host.get();
    }
    void setHost(const char* host)
    {
        m_host.set(host);
    }

    bool getSecure()
    {
        return m_secure;
    }
    void setSecure(bool flag)
    {
        m_secure = flag;
    }

    bool getDiscard()
    {
        return m_discard;
    }
    void setDiscard(bool flag)
    {
        m_discard = flag;
    }
    
    int getVersion()
    {
        return m_version;
    }
    void setVersion(int version)
    {
        m_version = version;
    }

    void appendToRequestHeader(StringBuffer& buf)
    {
        buf.append(m_name.get()).append("=").append(m_value.get());
        if(m_version >= 1)
        {
            if(m_path.length() > 0)
                buf.append("; $Path=").append(m_path.get());
            if(m_domain.length() > 0)
                buf.append("; $Domain=").append(m_domain.get());
            if(m_ports.length() > 0)
                buf.append("; $Port=").append('"').append(m_ports.get()).append('"');
        }
    }

    void appendToResponseHeader(StringBuffer& buf)
    {
        buf.append(m_name.get()).append("=").append(m_value.get());
        if(m_path.length() > 0)
            buf.append("; Path=").append(m_path.get());
        if(m_domain.length() > 0)
            buf.append("; Domain=").append(m_domain.get());
        if(m_secure)
            buf.append("; Secure");
        if(m_version >= 1)
        {
            buf.append("; Version=").append(m_version);     
            if(m_ports.length() > 0)
                buf.append("; Port=").append('"').append(m_ports.get()).append('"');
            if(m_comment.length() > 0)
                buf.append("; Comment=").append(m_comment.get());
            if(m_commentURL.length() > 0)
                buf.append("; CommentURL=").append(m_commentURL.get());
            if(m_discard)
                buf.append("; Discard");
            if(m_maxage >= 0)
                buf.append("; Max-Age=").append(m_maxage);
        }
    }
};

#endif
