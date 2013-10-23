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

#ifndef _UTILS_HPP__
#define _UTILS_HPP__

#include "jliball.hpp"
#include "esp.hpp"

#include "xslprocessor.hpp"

#ifndef BINDUTIL_EXPORT
#define BINDUTIL_EXPORT
#endif

class BINDUTIL_EXPORT Utils
{
public:
    static const char* getWord(const char* oneline, StringBuffer& oneword);
    static char* getWord(char* oneline, char* & oneword, const char* separators, bool tws=false);
    static void parseNVPair(const char* nv, StringBuffer& name, StringBuffer& value);
    static int getLine(int total_len, int cur_len, const char* buf, int & oneline_len);
    static int strncasecmp(const char* s1, const char* s2, register size32_t n);
    static int strcasecmp(const char* s1, const char* s2);
    static const char *stristr(const char *haystack, const char *needle);
    static StringBuffer& base64encode(const void *data, long length, StringBuffer& result);
    static int base64decode(int inlen, const char *in, StringBuffer& data);
    static unsigned int hash (register const char *str, register unsigned int len);
    static StringBuffer &url_encode(const char* url, StringBuffer& encoded_url);
    static int url_decode(const char* param, StringBuffer& result);
    static  void SplitURL(const char* url, StringBuffer& Protocol,StringBuffer& Name,StringBuffer& Password,StringBuffer& FQDN, StringBuffer& Port, StringBuffer& Path);
};


BINDUTIL_EXPORT IPropertyTree * getBindingConfig(IPropertyTree *tree, const char *binding=NULL, const char *process=NULL);
BINDUTIL_EXPORT IPropertyTree * getProcessConfig(IPropertyTree *tree, const char *process=NULL);
BINDUTIL_EXPORT IPropertyTree * getProtocolConfig(IPropertyTree *tree, const char *prot, const char *process);
BINDUTIL_EXPORT IPropertyTree * getServiceConfig(IPropertyTree *tree, const char *srv, const char *process);


#ifndef _WIN32

inline char* strupr(char* str)
{
    if(str != NULL)
    {
        for (int i=0; str[i]!=0; i++)
            if(str[i] >= 'a' && str[i] <= 'z')
                str[i] = str[i] - ('a' - 'A');
    }

    return str;
}

#endif

BINDUTIL_EXPORT IEspStringIntMap * createStringIntMap();

BINDUTIL_EXPORT void buildArrayHint(StringBuffer& s, int indents, const char* tag, const char* cls);
BINDUTIL_EXPORT const char *mimeTypeFromFileExt(const char *ext);


void xslTransformHelper(IXslProcessor *xslp, const char* xml, const char* xslFile, StringBuffer& output, IProperties *params);

class EspTimeSection
{
public:
    EspTimeSection(const char * _title) : title(_title)
    {
        start_time = get_cycles_now();
    }
  
    ~EspTimeSection()
    {
        if (title)
        {
            cycle_t end_time = get_cycles_now();
            display_time(title, end_time-start_time);
        }
    }

    void clear(){title=NULL;}

protected:
  const char *    title;
  cycle_t         start_time;
};


#endif
