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

#pragma warning (disable : 4786)

#include "esphttp.hpp"

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
