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

#pragma warning (disable : 4786)

#ifdef WIN32
#ifdef ESPHTTP_EXPORTS
    #define esp_http_decl __declspec(dllexport)
#endif
#endif

//Jlib
#include "jliball.hpp"

//SCM Interface definition includes:
#include "esp.hpp"
#include "espthread.hpp"

//ESP Bindings
#include "http/platform/httpprot.hpp"
#include "http/platform/httptransport.ipp"
#include "http/platform/httpservice.hpp"
#include "SOAP/Platform/soapservice.hpp"

//openssl
#include <openssl/rsa.h>
#include <openssl/crypto.h>


#ifdef WIN32
#define HTMLPAGE_EXPORT _declspec(dllexport)
#else
#define HTMLPAGE_EXPORT
#endif

#include "htmlpage.hpp"

//  ===========================================================================
CHtmlEntity & CHtmlContainer::appendContent(CHtmlEntity & content)
{
    content.Link();
    m_content.append(content);
    return content;
}

CHtmlEntity * CHtmlContainer::appendContent(CHtmlEntity * content)
{
    m_content.append(*content);
    return content;
}

StringBuffer & CHtmlContainer::getContentHtml(StringBuffer & result)
{
    ForEachItemIn(i, m_content)
    {
        CHtmlEntity &entity = m_content.item(i);
        StringBuffer entityHtml;
        result.append(entity.getHtml(entityHtml).str());
    }
    return result;
}

StringBuffer & CHtmlContainer::getContentHtml(StringBuffer & result, const char * tag)
{
    ForEachItemIn(i, m_content)
    {
        CHtmlEntity &entity = m_content.item(i);
        StringBuffer entityHtml;
        result.appendf("<%s>%s</%s>", tag, entity.getHtml(entityHtml).str(), tag);
    }
    return result;
}

StringBuffer & CHtmlContainer::getContentHtml(StringBuffer & result, const char * prefix, const char * postfix)
{
    ForEachItemIn(i, m_content)
    {
        CHtmlEntity &entity = m_content.item(i);
        StringBuffer entityHtml;
        result.appendf("%s%s%s", prefix, entity.getHtml(entityHtml).str(), postfix);
    }
    return result;
}
//  ===========================================================================
HtmlPage::HtmlPage(const char * title)
{
    m_title.append(title);
}

const char * const HTML_PAGE = "\
<html>\
\
<head>\
<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\">\
<title>%s</title>\
</head>\
\
<body>\
%s\
</body>\
\
</html>\
";

StringBuffer & HtmlPage::getHtml(StringBuffer & result)
{
    StringBuffer body;
    result.appendf(HTML_PAGE, m_title.str(), getContentHtml(body).str());
    return result;
}
//  ===========================================================================
CHtmlText::CHtmlText(const char * txt)
{
    m_text.append(txt);
}

void CHtmlText::setText(const char * txt)
{
    m_text.clear();
    m_text.append(txt);
}

StringBuffer & CHtmlText::getHtml(StringBuffer & result)
{
    result.append(m_text.str());
    return result;
}
//  ===========================================================================
CHtmlHeader::CHtmlHeader(HTML_HEADER_SIZE size)
{
    setSize(size);
}

CHtmlHeader::CHtmlHeader(HTML_HEADER_SIZE size, const char * text)
{
    setSize(size);
    setText(text);
}

void CHtmlHeader::setSize(HTML_HEADER_SIZE size)
{
    m_size.clear();
    switch(size)
    {
    case H1:
        m_size.append("h1");
        break;
    case H2:
        m_size.append("h2");
        break;
    case H3:
        m_size.append("h3");
        break;
    case H4:
        m_size.append("h4");
        break;
    case H5:
        m_size.append("h5");
        break;
    case H6:
        m_size.append("h6");
        break;
    }
}

const char * const HTML_HEADER = "<%s>%s</%s>";
StringBuffer & CHtmlHeader::getHtml(StringBuffer & result)
{
    StringBuffer body;
    result.appendf(HTML_HEADER, m_size.str(), m_text.str(), m_size.str());
    return result;
}

//  ===========================================================================
CHtmlParagraph::CHtmlParagraph(const char * txt)
{
    appendContent(new CHtmlText(txt));
}

const char * const HTML_PARAGRAPH = "<p>%s</p>";
StringBuffer & CHtmlParagraph::getHtml(StringBuffer & result)
{
    StringBuffer body;
    result.appendf(HTML_PARAGRAPH, getContentHtml(body).str());
    return result;
}
//  ===========================================================================
CHtmlLink::CHtmlLink(const char * text, const char * link)
{
    setText(text);
    setLink(link);
}
void CHtmlLink::setLink(const char * txt)
{
    m_link.clear();
    m_link.append(txt);
}

const char * const HTML_LINK = "<a href=\"%s\">%s</a>";
StringBuffer & CHtmlLink::getHtml(StringBuffer & result)
{
    result.appendf(HTML_LINK, m_link.str(), m_text.str());
    return result;
}
//  ===========================================================================
const char * const HTML_LIST = "<ul>%s</ul>";
StringBuffer & CHtmlList::getHtml(StringBuffer & result)
{
    StringBuffer body;
    result.appendf(HTML_LIST, getContentHtml(body, "li").str());
    return result;
}
//  ===========================================================================
