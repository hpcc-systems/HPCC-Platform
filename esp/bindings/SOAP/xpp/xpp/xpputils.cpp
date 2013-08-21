#include "jlib.hpp"
#include "XmlPullParser.h"
#include "xpputils.h"

using namespace xpp;

IMultiException *xppMakeException(XmlPullParser &xppx)
{
    StringBuffer msg;
    StringBuffer sourcestr;
    int code = -1;

    int level = 1;
    int type = XmlPullParser::END_TAG;

    StartTag stag;

    while(level > 0)
    {
        type = xppx.next();
        switch(type)
        {
            case XmlPullParser::START_TAG:
            {
                xppx.readStartTag(stag);
                ++level;
                const char *tag = stag.getLocalName();
                if (!stricmp(tag, "Message"))
                {
                    readFullContent(xppx, msg);
                }
                else if (!stricmp(tag, "Code"))
                {
                    StringBuffer codestr;
                    readFullContent(xppx, codestr);
                    code = atoi(codestr.str());
                }
                else if (!stricmp(tag, "Source"))
                {
                    readFullContent(xppx, sourcestr);
                }
                break;
            }
            case XmlPullParser::END_TAG:
                --level;
            break;

            case XmlPullParser::END_DOCUMENT:
                level=0;
            break;
        }
    }

    IMultiException * me = MakeMultiException(sourcestr.str());
    me->append(*MakeStringException(code, msg.str()));

    return me;
}


void xppToXmlString(XmlPullParser &xpp, StartTag &stag, StringBuffer & buffer)
{
    int level = 1; //assumed due to the way gotonextdataset works.
    int type = XmlPullParser::END_TAG;
    const char * content = "";
    const char *tag = NULL;
    EndTag etag;

    tag = stag.getLocalName();
    if (tag)
    {
        buffer.appendf("<%s", tag);

        for (int idx=0; idx<stag.getLength(); idx++)
        {
            buffer.appendf(" %s=\"", stag.getRawName(idx));
            buffer.append(stag.getValue(idx));
            buffer.append('\"');
        }

        buffer.append(">");
    }
    do
    {
        type = xpp.next();
        switch(type)
        {
            case XmlPullParser::START_TAG:
            {
                xpp.readStartTag(stag);
                ++level;

                tag = stag.getLocalName();
                if (tag)
                {
                    buffer.appendf("<%s", tag);
                    for (int idx=0; idx<stag.getLength(); idx++)
                    {
                        buffer.appendf(" %s=\"", stag.getRawName(idx));
                        buffer.append(stag.getValue(idx));
                        buffer.append('\"');
                    }
                    buffer.append(">");
                }
                break;
            }
            case XmlPullParser::END_TAG:
                xpp.readEndTag(etag);
                tag = etag.getLocalName();
                if (tag)
                    buffer.appendf("</%s>", tag);
                --level;
            break;
            case XmlPullParser::CONTENT:
                content = xpp.readContent();
                encodeXML(content, buffer);
                break;
            case XmlPullParser::END_DOCUMENT:
                level=0;
            break;
        }
    }
    while (level > 0);
}

bool xppGotoTag(XmlPullParser &xppx, const char *tagname, StartTag &stag)
{
    int level = 1;
    int type = XmlPullParser::END_TAG;

    do
    {
        type = xppx.next();
        switch(type)
        {
            case XmlPullParser::START_TAG:
            {
                xppx.readStartTag(stag);
                ++level;
                const char *tag = stag.getLocalName();
                if (!stricmp(tag, tagname))
                    return true;
                else if (!stricmp(tag, "Exception"))
                    throw xppMakeException(xppx);
                break;
            }
            case XmlPullParser::END_TAG:
                --level;
            break;

            case XmlPullParser::END_DOCUMENT:
                level=0;
            break;
        }
    }
    while (level > 0);

    return false;
}
