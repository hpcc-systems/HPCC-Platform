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

#ifndef WUWEBVIEW_INCL
#define WUWEBVIEW_INCL

#ifdef _WIN32
    #ifdef WUWEBVIEW_EXPORTS
        #define WUWEBVIEW_API __declspec(dllexport)
    #else
        #define WUWEBVIEW_API __declspec(dllimport)
    #endif
#else
    #define WUWEBVIEW_API
#endif

#define WWV_OMIT_XML_DECLARATION        0x0001
#define WWV_USE_DISPLAY_XSLT            0x0002
#define WWV_OMIT_RESULT_TAG             0x0004
#define WWV_ADD_SOAP                    0x0008
#define WWV_ADD_RESULTS_TAG             0x0010
#define WWV_ADD_RESPONSE_TAG            0x0020
#define WWV_OMIT_SCHEMAS                0x0040
#define WWV_CDATA_SCHEMAS               0x0080
#define WWV_INCL_NAMESPACES             0x0100
#define WWV_INCL_GENERATED_NAMESPACES   0x0200

interface IWuWebView : extends IInterface
{
    virtual void getResultViewNames(StringArray &names)=0;
    virtual void renderResults(const char *viewName, const char *xml, StringBuffer &html)=0;
    virtual void renderResults(const char *viewName, StringBuffer &html)=0;
    virtual void renderSingleResult(const char *viewName, const char *resultname, StringBuffer &html)=0;
    virtual void applyResultsXSLT(const char *file, const char *xml, StringBuffer &html)=0;
    virtual void applyResultsXSLT(const char *file, StringBuffer &html)=0;
    virtual StringBuffer &aggregateResources(const char *type, StringBuffer &content)=0;
    virtual void expandResults(const char *xml, StringBuffer &out, unsigned flags)=0;
    virtual void expandResults(StringBuffer &out, unsigned flags)=0;
    virtual void addInputsFromPTree(IPropertyTree *pt)=0;
    virtual void addInputsFromXml(const char *xml)=0;
};

extern WUWEBVIEW_API IWuWebView *createWuWebView(IConstWorkUnit &wu, const char *queryname, const char*dir, bool mapEspDir);
extern WUWEBVIEW_API IWuWebView *createWuWebView(const char *wuid, const char *queryname, const char*dir, bool mapEspDir);

#endif
