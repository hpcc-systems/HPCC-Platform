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

#ifndef __HTMLPAGE_HPP_
#define __HTMLPAGE_HPP_

//Jlib
#include "jliball.hpp"

#ifndef HTMLPAGE_EXPORT
#define HTMLPAGE_EXPORT
#endif

class HTMLPAGE_EXPORT CHtmlEntity : public CInterface
{
protected:
public:
    IMPLEMENT_IINTERFACE;

    virtual StringBuffer & getHtml(StringBuffer & result) = 0;
};

class HTMLPAGE_EXPORT CHtmlContainer : public CHtmlEntity
{
protected:
    CIArrayOf<CHtmlEntity> m_content;
    StringBuffer & getContentHtml(StringBuffer & result);
    StringBuffer & getContentHtml(StringBuffer & result, const char * tag);
    StringBuffer & getContentHtml(StringBuffer & result, const char * prefix, const char * postfix);

public:
    CHtmlEntity & appendContent(CHtmlEntity & content);
    CHtmlEntity * appendContent(CHtmlEntity * content);
};

class HTMLPAGE_EXPORT HtmlPage : public CHtmlContainer
{
protected:
    StringBuffer m_title;

public:
    HtmlPage(const char * title);
    virtual StringBuffer & getHtml(StringBuffer & result);
};

class HTMLPAGE_EXPORT CHtmlParagraph : public CHtmlContainer
{
public:
    CHtmlParagraph() {};
    CHtmlParagraph(const char * txt);

    virtual StringBuffer & getHtml(StringBuffer & result);
};

class HTMLPAGE_EXPORT CHtmlText : public CHtmlEntity
{
protected:
    StringBuffer m_text;

public:
    CHtmlText() {};
    CHtmlText(const char * txt);

    void setText(const char * txt);
    virtual StringBuffer & getHtml(StringBuffer & result);
};

class HTMLPAGE_EXPORT CHtmlLink : public CHtmlText
{
protected:
    StringBuffer m_link;
public:
    CHtmlLink(){};
    CHtmlLink(const char * text, const char * link);
    void setLink(const char * txt);
    virtual StringBuffer & getHtml(StringBuffer & result);
};

enum HTML_HEADER_SIZE
{
    H1,
    H2,
    H3,
    H4,
    H5,
    H6 
};

class HTMLPAGE_EXPORT CHtmlHeader : public CHtmlText
{
protected:
    StringBuffer m_size;

public:
    CHtmlHeader() {};
    CHtmlHeader(HTML_HEADER_SIZE size);
    CHtmlHeader(HTML_HEADER_SIZE size, const char * text);

    void setSize(HTML_HEADER_SIZE size);

    virtual StringBuffer & getHtml(StringBuffer & result);
};

class HTMLPAGE_EXPORT CHtmlList : public CHtmlContainer
{
public:
    virtual StringBuffer & getHtml(StringBuffer & result);
};

#endif //__HTMLPAGE_HPP_

