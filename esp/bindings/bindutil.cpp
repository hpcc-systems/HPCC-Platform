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
    0000, 0001, 0002, 0003, 0004, 0005, 0006, 0007,
    0010, 0011, 0012, 0013, 0014, 0015, 0016, 0017,
    0020, 0021, 0022, 0023, 0024, 0025, 0026, 0027,
    0030, 0031, 0032, 0033, 0034, 0035, 0036, 0037,
    0040, 0041, 0042, 0043, 0044, 0045, 0046, 0047,
    0050, 0051, 0052, 0053, 0054, 0055, 0056, 0057,
    0060, 0061, 0062, 0063, 0064, 0065, 0066, 0067,
    0070, 0071, 0072, 0073, 0074, 0075, 0076, 0077,
    0100, 0141, 0142, 0143, 0144, 0145, 0146, 0147,
    0150, 0151, 0152, 0153, 0154, 0155, 0156, 0157,
    0160, 0161, 0162, 0163, 0164, 0165, 0166, 0167,
    0170, 0171, 0172, 0133, 0134, 0135, 0136, 0137,
    0140, 0141, 0142, 0143, 0144, 0145, 0146, 0147,
    0150, 0151, 0152, 0153, 0154, 0155, 0156, 0157,
    0160, 0161, 0162, 0163, 0164, 0165, 0166, 0167,
    0170, 0171, 0172, 0173, 0174, 0175, 0176, 0177,
    0200, 0201, 0202, 0203, 0204, 0205, 0206, 0207,
    0210, 0211, 0212, 0213, 0214, 0215, 0216, 0217,
    0220, 0221, 0222, 0223, 0224, 0225, 0226, 0227,
    0230, 0231, 0232, 0233, 0234, 0235, 0236, 0237,
    0240, 0241, 0242, 0243, 0244, 0245, 0246, 0247,
    0250, 0251, 0252, 0253, 0254, 0255, 0256, 0257,
    0260, 0261, 0262, 0263, 0264, 0265, 0266, 0267,
    0270, 0271, 0272, 0273, 0274, 0275, 0276, 0277,
    0300, 0301, 0302, 0303, 0304, 0305, 0306, 0307,
    0310, 0311, 0312, 0313, 0314, 0315, 0316, 0317,
    0320, 0321, 0322, 0323, 0324, 0325, 0326, 0327,
    0330, 0331, 0332, 0333, 0334, 0335, 0336, 0337,
    0340, 0341, 0342, 0343, 0344, 0345, 0346, 0347,
    0350, 0351, 0352, 0353, 0354, 0355, 0356, 0357,
    0360, 0361, 0362, 0363, 0364, 0365, 0366, 0367,
    0370, 0371, 0372, 0373, 0374, 0375, 0376, 0377,
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

__int64 Utils::getLine(__int64 total_len, __int64 cur_pos, const char* buf, int& oneline_len)
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

int Utils::url_decode(const char* url, StringBuffer& result)
{
    appendDecodedURL(result, url);
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
