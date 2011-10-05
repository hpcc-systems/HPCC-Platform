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
