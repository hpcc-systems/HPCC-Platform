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

