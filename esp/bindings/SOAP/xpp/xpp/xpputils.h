// -*-c++-*- --------------74-columns-wide-------------------------------|
#ifndef XPP_UTILS_H_
#define XPP_UTILS_H_

#pragma warning(disable:4290)

using namespace xpp;

IMultiException *xppMakeException(IXmlPullParser &xppx);

void xppToXmlString(IXmlPullParser &xpp, StartTag &stag, StringBuffer & buffer);
bool xppGotoTag(IXmlPullParser &xppx, const char *tagname, StartTag &stag);

#endif // XPP_UTILS_H_
