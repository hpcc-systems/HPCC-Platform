// -*-c++-*- --------------74-columns-wide-------------------------------|
#ifndef XPP_UTILS_H_
#define XPP_UTILS_H_

#pragma warning(disable:4290)

using namespace xpp;

IMultiException *xppMakeException(XmlPullParser &xppx);

void xppToXmlString(XmlPullParser &xpp, StartTag &stag, StringBuffer & buffer);
bool xppGotoTag(XmlPullParser &xppx, const char *tagname, StartTag &stag);

#endif // XPP_UTILS_H_
