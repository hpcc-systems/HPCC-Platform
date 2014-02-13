/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
    virtual void getResourceURLs(StringArray &urls, const char *prefix)=0;
    virtual void renderResults(const char *viewName, const char *xml, StringBuffer &html)=0;
    virtual void renderResults(const char *viewName, StringBuffer &html)=0;
    virtual void renderSingleResult(const char *viewName, const char *resultname, StringBuffer &html)=0;
    virtual void renderResultsJSON(StringBuffer &out, const char *jsonp)=0;
    virtual void applyResultsXSLT(const char *file, const char *xml, StringBuffer &html)=0;
    virtual void applyResultsXSLT(const char *file, StringBuffer &html)=0;
    virtual StringBuffer &aggregateResources(const char *type, StringBuffer &content)=0;
    virtual void expandResults(const char *xml, StringBuffer &out, unsigned flags)=0;
    virtual void expandResults(StringBuffer &out, unsigned flags)=0;
    virtual void addInputsFromPTree(IPropertyTree *pt)=0;
    virtual void addInputsFromXml(const char *xml)=0;
    virtual void createWuidResponse(StringBuffer &out, unsigned flags)=0;
    virtual bool getResourceByPath(const char *path, MemoryBuffer &mb)=0;
    virtual StringBuffer &getManifest(StringBuffer &mf)=0;

};

extern WUWEBVIEW_API IWuWebView *createWuWebView(IConstWorkUnit &wu, const char *queryname, const char*dir, bool mapEspDir);
extern WUWEBVIEW_API IWuWebView *createWuWebView(const char *wuid, const char *queryname, const char*dir, bool mapEspDir);

extern WUWEBVIEW_API void getWuResourceByPath(const char *path, MemoryBuffer &mb, StringBuffer &mimetype);

static inline bool isPathSeparator(char sep)
{
    return (sep=='\\')||(sep=='/');
}

static inline const char *skipPathNodes(const char *&s, int skip)
{
    if (s) {
        while (*s) {
            if (isPathSeparator(*s++))
                if (!skip--)
                    return s;
        }
    }
    return NULL;
}

static inline const char *nextPathNode(const char *&s, StringBuffer &node, int skip=0)
{
    if (skip)
        skipPathNodes(s, skip);
    if (s) while (*s) {
        if (isPathSeparator(*s))
            return s++;
        node.append(*s++);
    }
    return NULL;
}

static inline const char *firstPathNode(const char *&s, StringBuffer &node)
{
    if (s && isPathSeparator(*s))
        s++;
    return nextPathNode(s, node);
}

#endif
