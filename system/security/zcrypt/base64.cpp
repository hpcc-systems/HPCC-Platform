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

#include "base64.ipp"

static const char BASE64_enc[65] =  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                    "abcdefghijklmnopqrstuvwxyz"
                                    "0123456789+/";

static const char pad = '=';

ZBuffer& base64_encode(int length, const void *data, ZBuffer& result)
{
    int estlen = (int)(length*4.0/3.0 + (length / 54) + 8);
    unsigned char *out  = (unsigned char *)malloc(estlen);

    const unsigned char *in = static_cast<const unsigned char *>(data);

    unsigned char one;
    unsigned char two;
    unsigned char three;

    long i, j;
    for(i = 0, j = 0; i <= length - 3;)
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

    result.setBuffer(j, out);

    return result;
}

ZBuffer& base64_decode(int inlen, const char *in, ZBuffer& data)
{
    if(!inlen || !in)
        return data;

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

    int bc = 0; 
    int estlen = (int)(inlen/4.0*3.0 + 2);
    unsigned char* buf = (unsigned char *) malloc(estlen);
    for(int i = 0; i < inlen; )
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

        buf[bc++] = (d1 << 2) | (d2 >> 4);

        if(c3 == pad)
            break;

        buf[bc++] = (d2 << 4) | (d3 >> 2);

        if(c4 == pad)
            break;

        buf[bc++] = (d3 << 6) | d4;
    }

    if(bc > 0)
        data.setBuffer(bc, buf);
    else
        free(buf);

    return data;
}
