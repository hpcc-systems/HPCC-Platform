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

#pragma warning( disable : 4786)

#ifdef _WIN32
#define BINDUTIL_EXPORT _declspec(dllexport)
#else
#define BINDUTIL_EXPORT
#endif

#include <map>
#include <string>

#include "esp.hpp"
#include "bindutil.hpp"
#include "espcontext.hpp"

#define URL_MAX  512

static const unsigned char charmap[] = {
    '\000', '\001', '\002', '\003', '\004', '\005', '\006', '\007',
    '\010', '\011', '\012', '\013', '\014', '\015', '\016', '\017',
    '\020', '\021', '\022', '\023', '\024', '\025', '\026', '\027',
    '\030', '\031', '\032', '\033', '\034', '\035', '\036', '\037',
    '\040', '\041', '\042', '\043', '\044', '\045', '\046', '\047',
    '\050', '\051', '\052', '\053', '\054', '\055', '\056', '\057',
    '\060', '\061', '\062', '\063', '\064', '\065', '\066', '\067',
    '\070', '\071', '\072', '\073', '\074', '\075', '\076', '\077',
    '\100', '\141', '\142', '\143', '\144', '\145', '\146', '\147',
    '\150', '\151', '\152', '\153', '\154', '\155', '\156', '\157',
    '\160', '\161', '\162', '\163', '\164', '\165', '\166', '\167',
    '\170', '\171', '\172', '\133', '\134', '\135', '\136', '\137',
    '\140', '\141', '\142', '\143', '\144', '\145', '\146', '\147',
    '\150', '\151', '\152', '\153', '\154', '\155', '\156', '\157',
    '\160', '\161', '\162', '\163', '\164', '\165', '\166', '\167',
    '\170', '\171', '\172', '\173', '\174', '\175', '\176', '\177',
    '\200', '\201', '\202', '\203', '\204', '\205', '\206', '\207',
    '\210', '\211', '\212', '\213', '\214', '\215', '\216', '\217',
    '\220', '\221', '\222', '\223', '\224', '\225', '\226', '\227',
    '\230', '\231', '\232', '\233', '\234', '\235', '\236', '\237',
    '\240', '\241', '\242', '\243', '\244', '\245', '\246', '\247',
    '\250', '\251', '\252', '\253', '\254', '\255', '\256', '\257',
    '\260', '\261', '\262', '\263', '\264', '\265', '\266', '\267',
    '\270', '\271', '\272', '\273', '\274', '\275', '\276', '\277',
    '\300', '\301', '\302', '\303', '\304', '\305', '\306', '\307',
    '\310', '\311', '\312', '\313', '\314', '\315', '\316', '\317',
    '\320', '\321', '\322', '\323', '\324', '\325', '\326', '\327',
    '\330', '\331', '\332', '\333', '\334', '\335', '\336', '\337',
    '\340', '\341', '\342', '\343', '\344', '\345', '\346', '\347',
    '\350', '\351', '\352', '\353', '\354', '\355', '\356', '\357',
    '\360', '\361', '\362', '\363', '\364', '\365', '\366', '\367',
    '\370', '\371', '\372', '\373', '\374', '\375', '\376', '\377',
};

unsigned int Utils::hash (register const char *str, register unsigned int len)
{
  static unsigned short asso_values[] =
    {
       4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,    0,   10,
       125,   10,  360,    0,   65,  435,  555,  895, 4484, 4484,
      4484, 4484, 4484, 4484, 4484,    0,  330,    0,  130,    0,
       130,    0,  325,    0,    0,  400,    5,  315,  110,    0,
        65,    0,   20, 1195,    5, 4484, 4484, 4484, 4484,   20,
         5, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484, 4484,
      4484, 4484, 4484, 4484, 4484, 4484
    };
    
    unsigned int result = len;
    for(int i = len - 1; i >= 0; i--)
    {
        result += asso_values[(unsigned char)str[i]];
    }
    return result;

}

bool isLWS(char c)
{
    if(c == ' ' || c== '\t' || c == 13 || c == 10)
        return true;
    return false;
}

// oneline must be NULL terminated
const char* Utils::getWord(const char* oneline, StringBuffer& oneword)
{
    if(*oneline == 0)
        return oneline;

    while(*oneline && isLWS(*oneline))
        oneline++;

    while(*oneline && !isLWS(*oneline))
    {
        oneword.append(*oneline);
        oneline++;
    }

    return oneline;
}

bool isSeparator(char c, const char* separators)
{
    while(*separators != '\0')
    {
        if(*separators == c)
            return true;
        separators++;
    }

    return false;
}


// oneline must be NULL terminated
char* Utils::getWord(char* oneline, char* & oneword, const char* separators, bool tws)
{
    if(oneline == NULL || *oneline == 0)
    {
        oneword = NULL;
        return oneline;
    }

    while(*oneline != '\0' && (isSeparator(*oneline, separators) || (tws && isLWS(*oneline))))
        oneline++;

    oneword = oneline;

    while(*oneline != '\0' && !isSeparator(*oneline, separators))
        oneline++;

    if(*oneline != '\0')
    {
        *oneline = '\0';
        oneline++;
    }

    return oneline;
}

void Utils::parseNVPair(const char* nv, StringBuffer& name, StringBuffer& value)
{
    name.clear();
    value.clear();

    if(nv == NULL || *nv == '\0')
        return;

    const char* ptr = nv;
    while(ptr != NULL && *ptr == ' ')
    {
        ptr++;
    }

    if(ptr == NULL || *ptr == '\0')
        return;

    const char* nptr = ptr;
    const char* vptr = NULL;
    int nlen = 0;
    while(ptr != NULL && *ptr != '\0' && *ptr != '=')
    {
        ptr++;
        nlen++;
    }

    if(nlen <= 0)
        return;
    
    if(ptr != NULL && *ptr != '\0')
        vptr = ptr + 1;
    while(vptr != NULL && *vptr == ' ')
        vptr++;
    
    while(nlen > 0 && nptr[nlen - 1] == ' ')
        nlen--;
    if(nlen > 0)
        name.append(nlen, nptr);

    if(!vptr)
        return;
    if(vptr[0] == '"')
        vptr++;
    int vlen = strlen(vptr);
    while(vlen > 0 && vptr[vlen - 1] == ' ')
        vlen--;
    if(vlen > 0 && vptr[vlen - 1] == '"')
        vlen--;

    if(vlen > 0)
        value.append(vlen, vptr);
}

// buf DOESN'T have to be NULL terminated
int Utils::getLine(int total_len, int cur_pos, const char* buf, int& oneline_len)
{
    oneline_len = 0;
    if(cur_pos >= total_len)
    {
        return total_len;
    }

    while(cur_pos < total_len && buf[cur_pos] != '\r' && buf[cur_pos] != '\n')
    {
        oneline_len++;
        cur_pos++;
    }

    if(cur_pos == total_len)
        return total_len;

    if(buf[cur_pos] == '\r')
    {
        cur_pos++;
    }

    if(cur_pos == total_len)
        return total_len;

    if(buf[cur_pos] == '\n')
    {
        cur_pos++;
    }

    return cur_pos;
}

int Utils::strncasecmp(const char* s1, const char* s2, register size32_t n) 
{
    bool s1isnull = (s1 == NULL);
    bool s2isnull = (s2 == NULL);
    if(s1isnull || s2isnull)
        return s1isnull - s2isnull;

    if (n != 0) 
    {
        register const unsigned char *cm = charmap,
                *us1 = (const unsigned char *)s1,
                *us2 = (const unsigned char *)s2;

        do 
        {
            if (cm[*us1] != cm[*us2++])
                return (cm[*us1] - cm[*--us2]);
        }
        while ((*us1++ != '\0') && (--n != 0));
    }

    return (0);
}

const char *Utils::stristr(const char *haystack, const char *needle)
{
    if(!haystack || !*haystack || !needle || !*needle)
        return NULL;
    int len = strlen(needle);
    for(const char* ptr = haystack; *ptr; ptr++)
    {
        if(strncasecmp(ptr, needle, len) == 0)
            return ptr;
    }

    return NULL;
}

int Utils::strcasecmp(const char* s1, const char* s2) 
{
    bool s1isnull = (s1 == NULL);
    bool s2isnull = (s2 == NULL);
    if(s1isnull || s2isnull)
        return s1isnull - s2isnull;

    register const unsigned char *cm = charmap,
            *us1 = (const unsigned char *)s1,
            *us2 = (const unsigned char *)s2;

    do
    {
        if (cm[*us1] != cm[*us2++])
            return (cm[*us1] - cm[*--us2]);
    }
    while (*us1++ != '\0');

    return (0);
}

static const char BASE64_enc[65] =  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                    "abcdefghijklmnopqrstuvwxyz"
                                    "0123456789+/";

static const char pad = '=';


//
// Encode the input in a base64 format
//
// const void * data -> data to be encoded
// long length       -> length in bytes of this data
// returns StringBuffer&     -> the encoded string
//
/*static*/
StringBuffer& Utils::base64encode(const void *data, long length, StringBuffer& result)
{
   double olend = 1.5 *  (length + (length % 2));
   int   olen = (int)olend;
    char *out  = new char[olen + ((olen % 80) * 2) + 2];

    const unsigned char *in = static_cast<const unsigned char *>(data);

    unsigned char one;
    unsigned char two;
    unsigned char three;

    long i, j;
    for(i = 0, j = 0; i < length && length - i >= 3;)
    {
        one = *(in + i++);
        two = *(in + i++);
        three = *(in + i++);

        // 0x30 -> 0011 0000 b
        // 0x3c -> 0011 1100 b
        // 0x3f -> 0011 1111 b
        //
        out[j++] = BASE64_enc[one >> 2];
        out[j++] = BASE64_enc[((one << 4) & 0x30) | (two >> 4)];
        out[j++] = BASE64_enc[((two << 2)  & 0x3c) | (three >> 6)];
        out[j++] = BASE64_enc[three & 0x3f];

        if(i % 54 == 0)
        {
            out[j++] = '\n';
        }
    }

    switch(length - i)
    {
        case 2:
            one = *(in + i++);
            two = *(in + i++);

            out[j++] = BASE64_enc[one >> 2];
            out[j++] = BASE64_enc[((one << 4) & 0x30) | (two >> 4)];
            out[j++] = BASE64_enc[(two << 2)  & 0x3c];
            out[j++] = pad;
        break;

        case 1:
            one = *(in + i++);

            out[j++] = BASE64_enc[one >> 2];
            out[j++] = BASE64_enc[(one << 4) & 0x30];
            out[j++] = pad;
            out[j++] = pad;
        break;
    }

    out[j++] = '\n';
    out[j] = '\0';
    
     result.append(out);

     delete[] out;

    return result;
}

//
// Decode the input in a base64 format
// 
// const char *in   -> NUL terminated string to be decoded
// void * out       -> Decoded string here
//
/*static*/
int Utils::base64decode(int inlen, const char *in, StringBuffer& data)
{
    static unsigned char BASE64_dec[256] = {0};
    static bool initialized = false;

    if(!initialized)
    {
        for(int i = 0; i < 64; ++i)
        {
            BASE64_dec[BASE64_enc[i]] = i;
        }

        initialized = true;
    }

    //unsigned char *data = static_cast<unsigned char *>(out);
    unsigned char c1, c2, c3, c4;
    unsigned char d1, d2, d3, d4;

    int len = strlen(in), count = 0;
    for(int i = 0; i < len; )
    {
        if(in[i] == '\n')
        {
            ++i;

            continue;
        }

        c1 = in[i++];
        c2 = in[i++];
        c3 = in[i++];
        c4 = in[i++];
        d1 = BASE64_dec[c1];
        d2 = BASE64_dec[c2];
        d3 = BASE64_dec[c3];
        d4 = BASE64_dec[c4];

        //data[count++] = (d1 << 2) | (d2 >> 4);
          data.appendf("%c", (d1 << 2) | (d2 >> 4));
          count++;

        if(c3 == pad)
            break;

        //data[count++] = (d2 << 4) | (d3 >> 2);
          data.appendf("%c", (d2 << 4) | (d3 >> 2));
          count++;

        if(c4 == pad)
            break;

        //data[count++] = (d3 << 6) | d4;
          data.appendf("%c", (d3 << 6) | d4);
          count++;
    }

    //data[count] = '\0';
    return count;
}


IPropertyTree *getBindingConfig(IPropertyTree *tree, const char *binding, const char *process)
{
    if (tree==NULL)
        return NULL;

    const char *topname=tree->queryName();

    if (!strcmp("Environment", topname) || !strcmp("Software", topname))
    {
        if (process!=NULL && *process!=0 && binding!=NULL && *binding!=0)
        {
            StringBuffer xpath;
            xpath.appendf("//EspProcess[@name=\"%s\"]/EspBinding[@name=\"%s\"]", process, binding);
            return tree->getPropTree(xpath.str());
        }
    }
    else if (!strcmp(topname, "EspProcess"))
    {
        if (binding!=NULL && *binding!=0)
        {
            if (strcmp(tree->queryProp("@name"), process)==0)
            {
                StringBuffer xpath;
                xpath.appendf("EspBinding[@name=\"%s\"]", binding);
                return tree->getPropTree(xpath.str());
            }
        }
    }
    else if (!strcmp(topname, "EspBinding"))
    {
        if (strcmp(tree->queryProp("@name"), binding)==0)
        {
            tree->Link();
            return tree;
        }
    }

    return NULL;
}

IPropertyTree *getServiceConfig(IPropertyTree *tree, const char *srv, const char *process)
{
    if (tree==NULL)
        return NULL;

    const char *topname=tree->queryName();

    if (!strcmp("Environment", topname) || !strcmp("Software", topname))
    {
        if (process!=NULL && *process!=0 && srv && *srv)
        {
            StringBuffer xpath;
            xpath.appendf("//EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]", process, srv);
            return tree->getPropTree(xpath.str());
        }
    }
    else if (!strcmp(topname, "EspProcess"))
    {
        if (srv!=NULL && *srv!=0)
        {
            if (strcmp(tree->queryProp("@name"), process)==0)
            {
                StringBuffer xpath;
                xpath.appendf("EspService[@name=\"%s\"]", srv);
                return tree->getPropTree(xpath.str());
            }
        }
    }
    else if (!strcmp(topname, "EspService"))
    {
        if (strcmp(tree->queryProp("@name"), srv)==0)
        {
            tree->Link();
            return tree;
        }
    }

    return NULL;
}

IPropertyTree *getProcessConfig(IPropertyTree *tree, const char *process)
{
    if (tree==NULL)
        return NULL;

    const char *topname=tree->queryName();

    if (!strcmp("Environment", topname) || !strcmp("Software", topname))
    {
        if (process!=NULL && *process!=0)
        {
            StringBuffer xpath;
            xpath.appendf("//EspProcess[@name=\"%s\"]", process);
            return tree->getPropTree(xpath.str());
        }
    }
    else if (!strcmp(topname, "EspProcess"))
    {
        if (strcmp(tree->queryProp("@name"), process)==0)
            return LINK(tree);
    }

    return NULL;
}

IPropertyTree *getProtocolConfig(IPropertyTree *tree, const char *prot, const char *process)
{
    if (tree==NULL)
        return NULL;

    const char *topname=tree->queryName();

    if (!strcmp("Environment", topname) || !strcmp("Software", topname))
    {
        if (process && *process && prot && *prot)
        {
            StringBuffer xpath;
            xpath.appendf("//EspProcess[@name=\"%s\"]/EspProtocol[@name=\"%s\"]", process, prot);
            return tree->getPropTree(xpath.str());
        }
    }
    else if (!strcmp(topname, "EspProcess"))
    {
        if (prot && *prot)
        {
            if (strcmp(tree->queryProp("@name"), process)==0)
            {
                StringBuffer xpath;
                xpath.appendf("EspProtocol[@name=\"%s\"]", prot);
                return tree->getPropTree(xpath.str());
            }
        }
    }
    else if (!strcmp(topname, "EspProtocol"))
    {
        if (strcmp(tree->queryProp("@name"), prot)==0)
        {
            tree->Link();
            return tree;
        }
    }

    return NULL;
}


static unsigned char isValidUrlChar[96] =
{
    0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x1,0x0,0x0,0x1,0x1,0x0, /*  !"#$%&'()*+,-./   */
    0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x0,0x0,0x0,0x0,0x0,0x0, /* 0123456789:;<=>?   */
    0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1, /* @ABCDEFGHIJKLMNO   */
    0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x0,0x0,0x0,0x0,0x1, /* PQRSTUVWXYZ[\]^_   */
    0x0,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1, /* `abcdefghijklmno   */
    0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x1,0x0,0x0,0x0,0x0,0x0  /* pqrstuvwxyz{\}~DEL */
};

StringBuffer &Utils::url_encode(const char* url, StringBuffer& encoded_url)
{
    if(!url)
        return encoded_url;
    unsigned char c;
    int i = 0;
    while((c = url[i]) != 0)
    {
        if(c == ' ')
            encoded_url.append('+');
        else if(c >= 32 && c < 128 && isValidUrlChar[c - 32])
            encoded_url.append(c);
        else
            encoded_url.appendf("%%%02X", c);
        i++;
    }
    return encoded_url;
}

static char translateHex(char hex) {
    if(hex >= 'A')
        return (hex & 0xdf) - 'A' + 10;
    else
        return hex - '0';
}

int Utils::url_decode(const char* url, StringBuffer& result)
{
    if(!url || !*url)
        return 0;

    const char *finger = url;
    while (*finger)
    {
        char c = *finger++;
        if (c == '+')
            c = ' ';
        else if (c == '%')
        {
            if(*finger != '\0')
            {
                c = translateHex(*finger);
                finger++;
            }
            if(*finger != '\0')
            {
                c = (char)(c*16 + translateHex(*finger));
                finger++;
            }
        }
        result.append(c);
    }

    return 0;
}


void Utils::SplitURL(const char* url, StringBuffer& protocol,StringBuffer& UserName,StringBuffer& Password,StringBuffer& host, StringBuffer& port, StringBuffer& path)
{
    int protlen = 0;
    if(!url || strlen(url) <= 7)
        throw MakeStringException(-1, "Invalid URL %s", url);
    else if(Utils::strncasecmp(url, "HTTP://", 7) == 0)
    {
        protocol.append("HTTP");
        protlen = 7;
    }
    else if(Utils::strncasecmp(url, "HTTPS://", 8) == 0)
    {
        protocol.append("HTTPS");
        protlen = 8;
    }
    else
    {
        throw MakeStringException(-1, "Please specify protocol HTTP or HTTPS");
    }

    char buf[URL_MAX+1];
    int len = strlen(url);
    const char *param = strchr(url + protlen, '?');
    if (param)
        len = param - url;

    if(len > URL_MAX)
        len = URL_MAX;

    strncpy(buf, url, len);
    buf[len] = 0;

    char* hostptr;
    char *username = NULL;
    char* atsign = strchr(buf, '@'); 
    if(atsign)
    {
        username = buf + protlen;
        hostptr = atsign + 1;
        *atsign = '\0';
    }
    else
    {
        hostptr = buf + protlen;
    }

    char* pathptr = strchr(hostptr, '/');
    if(pathptr)
    {
        *pathptr = 0;
        pathptr++;
    }
    char* portptr = strchr(hostptr, ':');
    if(portptr)
    {
        *portptr = 0;
        portptr++;
    }

    if(username)
    {
        char* semicln = strchr(username, ':');
        if(semicln)
        {
            Password.append(semicln+1);
            *semicln = '\0';
        }
        UserName.append(username);
    }

    if(hostptr)
        host.append(hostptr);

    if(portptr)
        port.append(portptr);

    path.append("/");
    if(pathptr)
        path.append(pathptr);   

    if(param)
        path.append(param); 
}


class CStringIntMap : public CInterface,
    implements IEspStringIntMap
{
    std::map<std::string, int> c_map;

public:
    IMPLEMENT_IINTERFACE;

    int queryValue(const char *key){return c_map[key];}
    void setValue(const char *key, int value){c_map[key]=value;}

};


IEspStringIntMap *createStringIntMap()
{
    return new CStringIntMap;
}

void buildArrayHint(StringBuffer& s, int indents, const char* tag, const char* cls)
{
    s.pad(indents).appendf("<%s>\\n", tag);
    s.pad(indents+1).append("<RawArray>\\n");
    if (cls)
    {
        s.pad(indents+2).append("<Item>\\n");
        s.append(cls);
        s.pad(indents+2).append("</Item>\\n");
    }
    else
        s.pad(indents+2).append("<Item></Item>\\n");
    s.pad(indents+1).append("</RawArray>\\n");
    s.pad(indents).appendf("</%s>\\n", tag);
}

void xslTransformHelper(IXslProcessor *xslp, const char* xml, const char* xslFile, StringBuffer& output, IProperties *params)
{
    if (xslp)
    {
        Owned<IXslTransform> xform = xslp->createXslTransform();
        StringBuffer xslpath;
        if (!strnicmp(xslFile, "/esp/xslt/", 10))
            if (!checkFileExists(xslpath.append(getCFD()).append("smc_xslt/").append(xslFile+10).str()) && !checkFileExists(xslpath.append(getCFD()).append("xslt/").append(xslFile+10).str()))
                return;
        xform->loadXslFromFile((xslpath.length()) ? xslpath.str() : xslFile);
        xform->setXmlSource(xml, strlen(xml)+1);
        if (params) xform->copyParameters(params);
        xform->transform(output.clear());
    }
}

const char *mimeTypeFromFileExt(const char *ext)
{
    if (!ext)
        return "application/octet-stream";
    if (*ext=='.')
        ext++;
    if (strieq(ext, "html") || strieq(ext, "htm"))
        return "text/html";
    if (strieq(ext, "xml") || strieq(ext, "xsl") || strieq(ext, "xslt"))
       return "application/xml";
    if (strieq(ext, "js"))
       return "text/javascript";
    if (strieq(ext, "css"))
       return "text/css";
    if (strieq(ext, "jpeg") || strieq(ext, "jpg"))
       return "image/jpeg";
    if (strieq(ext, "gif"))
       return "image/gif";
    if (strieq(ext, "png"))
       return "image/png";
    if (strieq(ext, "svg"))
       return "image/svg+xml";
    if (strieq(ext, "txt") || strieq(ext, "text"))
       return "text/plain";
    if (strieq(ext, "zip"))
       return "application/zip";
    if (strieq(ext, "pdf"))
       return "application/pdf";
    if (strieq(ext, "xpi"))
       return "application/x-xpinstall";
    if (strieq(ext, "exe") || strieq(ext, "class"))
       return "application/octet-stream";
    return "application/octet-stream";
}

class CEspHttpException: public CInterface, public IEspHttpException
{
public:
    IMPLEMENT_IINTERFACE;

    CEspHttpException(int code, const char *_msg, const char* _httpstatus) : errcode(code), msg(_msg), httpstatus(_httpstatus){ };
    int errorCode() const { return (errcode); };
    StringBuffer &  errorMessage(StringBuffer &str) const
    {
        return str.append("CEspHttpException: (").append(msg).append(")");
    };
    MessageAudience errorAudience() const { return (MSGAUD_user); };
    virtual const char* getHttpStatus() {return httpstatus.get(); }

private:
    int errcode;
    StringAttr msg;
    StringAttr httpstatus;
};

IEspHttpException* createEspHttpException(int code, const char *_msg, const char* _httpstatus)
{
    return new CEspHttpException(code, _msg, _httpstatus);
}
