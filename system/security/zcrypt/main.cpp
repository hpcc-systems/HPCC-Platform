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

#include "zcrypt.hpp"
#include <malloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

const char* privkeystr = 
"-----BEGIN RSA PRIVATE KEY-----\n"
"Proc-Type: 4,ENCRYPTED\n"
"DEK-Info: DES-EDE3-CBC,05364530B1FCC285\n"
"\n"
"zELKn/pb5fRnEhL1SdYxU4kGnULASMQKa1EtrIiZvh4DzHToUrI1DzDy8+nJcb89\n"
"Wd/RWu/J6oQNzXCTSIKT7cu10Swut38/8mp6Nzl0ck4pvIhMxnxef2NB2gf+uY1E\n"
"0W+qpmea7SIO85bIrFRtJ7k7LYq/haziXWO5FGKxbKp7S3pWV6yc/d0Gxa/7hbGB\n"
"Iy+V38iJmQwIbTUK+TDKVNWs8e3l/NlWrSRyDSh2kgUJFeMVJ+Tv8gj519TBsFve\n"
"OAtqFmBVkbtj0c6GQZ4nVPIAzZtKzejIVjuMsn1r6Gk+PGrfapqR5a7W+0QpL1yh\n"
"0C7n6Y88nAlp3zjZw/zaW8WTIhvIXWY8pm3vp6Z/IMNQlMyrHpfe4ruow6t/MTdf\n"
"5QFiGEyBycH0S1Qhkg/2fApBEfESMeAZT46MuqYkaDZZkY6QDSAPvS45jf4Fqrkc\n"
"ofAwdPMypwJhOoPZHVl4wWbv8QS1xrJQ0RxaiFnrc0m68m446NHryH5s9pi8YL+N\n"
"gwtaKnHywuCyne+aLeR1wjnhydVftgj13CTquBW66oDx5bQStTLGpysnLYbENeuR\n"
"NAYPr0iRP6cMJNwRtRQUu2m4qC8bUjktpC5yOBt3qJCws28UMmiluVlMfYrcPSTY\n"
"LVKFDZ02oi2PgYbJE8qtg/9a+LPjNf7qcxXrI3n3c4XyUGM/q1/yCgqEf7pgPnhi\n"
"g6AFyFI2b74QBjO1UUuMDpLokJYPMiVcV0hDWNpQj5KOMfiAWzS1CQIrDNCf2ZcE\n"
"RCQSlEhEyYr/0jnBcHPGEcko1rmVCjTTcgABPQUDu27G2s4ZxuKgoH//GiU2Xqsl\n"
"XAByuqmxQoQ61txKhMWb8B1DduMheTsRPLsBd10ZnTU1bQvNdrLMATioZZvdnLU+\n"
"2C0ENqKoEzI6DxaUnmVKqtBcq0+BFdv/RmPrGnnD7nF7WkGUTyDXvGSdbUwMiiX6\n"
"e89Er2YLUHHVvbGPuxuSUcslgSPZSS4T6d/7i3lS4hxR/1y3SBICqQIL//sb9XZZ\n"
"fDe9kDXD5OR5lULvVHjBi+txh54erAfVzSIzrLkfQoAcdkmM6ROxstBnZILUuMac\n"
"9U4F9Dc++MZDVYh1WCWZjPsfWaofkQ99qdAVmJXsaIqMDQy3GJnVupl6uGUa50XV\n"
"xqFk+ZsxRQvuO4vgYLXMRZbH3dLtI3ZTAhwq+YY/1rxXX59SZfIGA9SG/Ml2wSzQ\n"
"/FrBb6P8kDX/pYcIeEFQ4NT3CU54SkjamTI024wUzyQDnlm1XXj5DQjwEvjzYz/d\n"
"7tq7rT2ymFMNU+kx0Mj43dorC/2umvvmLyTHxRnXzZwQuRJAz6bNnaHLURIO0dCI\n"
"JuYWXIeCZwiqdMGdhjDbLeTHgY0oe1OCAgD6+aZ2qeSvaRAhgXACFQRrWmLEb49J\n"
"t6Qj6lXBlFT2luJbqVdYsKDpFxXBNYkt4u+qvLKz4QZ3kruuPZ8jzOsh4Z57AD89\n"
"WxvwRmXvZnqoPPrnfVS6lb/b8dXxExZASlwK0b8dQil3QFcqoE48WEveWbcrQIl2\n"
"bpbAJSnigO+wz3JgNKEkMBqcUliowqgMFOvS5MqvRYnTSxgNAwpN7A==\n"
"-----END RSA PRIVATE KEY-----\n";

const char* privkeystr1 = 
"-----BEGIN RSA PRIVATE KEY-----\n"
"MIIEowIBAAKCAQEA8QDpGCUDHVNANQmR81gaoxD4mJGD+YdRe2uohjfvN4GLLg8N\n"
"5a6MNBDYPhB8l24TgmtWUDzqSIIvpcTCi8FxeZEyQByB/0/u8iCz+Up0hkiqGU0k\n"
"N8AnBcrE2y+z0qy/dq+icBv9KmwRmCAoW2tBKRvQG3V0KE0wvGzQ55u6bUSh+qsb\n"
"uDQNkWYab+5ZnVt2WwF51aOLTjFdprc0ligczaz7mktNW9Mg6+FJjN1oVqfYh94W\n"
"76bEoso33GmFV2e7eerBY/lWZjMvTiIqktn5C44M82ipjndrORY8jxkstiSv7r10\n"
"9ocbeF895o+9LjAlh9TxKsy0DyZTF+01xKRFzQIDAQABAoIBACk93aWrF8hZ6b/p\n"
"vlclOZG0IsaBCFOYK4JyXulxAve9rGKaYuduIkH6q/aa/acwSBhmY+PhOLplxN+a\n"
"NyyRUujZxv6fokNdm2dF32aGrkAYiTtBLzR3JnZgR6W2mRAxTaZy0dpbf8xVqAEf\n"
"Z6iVRxZQ0yEPzWvkIbXs7SblSFbQNeBUlfd+2+e2mWP3/oHgQdU6wg+htx71pRbb\n"
"SW5mOzgzbc1ru/Xe6oPJiZQ3U8osZVLvGicw/z6b8ciWsheD8Dx/ESNYMYKTFbq1\n"
"BaAZZdBVUPZErERfNN/RmkYgAHeRlplMdavy9SyWsP0+GOpiIG/QPFANfMcu0qc1\n"
"9H7+9WUCgYEA+fKRtmyT+FSu6QrHvOoPaWzCUAIPrS1Oj+bGAJWAMKaJqLewDtpA\n"
"8NSsRRiSXlesvdkvQUN4dKNLx1RkKvyh2WPcJk6gGtLTjOrZFQcwCEB2bv+7FUil\n"
"603lIAnt566LjMUUQfaBGPrMiMcYe8ImEB9/uubHjO28PV1wJ7cBLWMCgYEA9tbl\n"
"u3yXoMNP+ATcDLacv8QG46YRCAujx0dFuS4Q0buQevjZntfhkeLu3OydNu4bmRDI\n"
"Jbf6k9j7LYXuyOYbDgRzapFKDm4Vjh9h0hFEaPVjDVUKrTLFKu9QBDdS+XAY00uO\n"
"m2BttUF5SdET0cQr/DVYhNnbufs8Efvw87w//w8CgYEAvxyH6ZIvucsWSj3h50KY\n"
"MiXklUReNC3WShVMBBpLf+d2jjiVN2YODZavec2F7PjgrfCoPyCVs6lAQdL3HB86\n"
"qXu/UtL/fEMDWlYfUgLC6SxQ4iJLK8T2iGpw7QRqkaFWNnZiPOV8ZFsvlM2WnNog\n"
"PGe5RHE81zbXnZwaK2O0VL0CgYAYQmyXrZoVYby1Snz5/uSO8EwhCYw49zPkfPu8\n"
"RGvAwSdk/pocw2jW9q+1JMgawvIRbBXPpzJIX5XoFnauZfcYvJU/TwIxQt55dlod\n"
"1Dad/if7AnWnKgs4ugZqM6nC/CJkedONL7/4hXPPLm49OoN8KR4HCIZQ1AFGXzWC\n"
"Luk3PwKBgCoO9J8eoJvs1aoH0ygOgdNDhiCXAYKnp2TTJPpot/KSkLVe94qlJHDh\n"
"x7TBm7lMAT47qthNul7Yf0ZKdnvfF6Nr6Uc2aTFnovSAme1yxyUZlod9+0Np79mI\n"
"GfYT92qrIFPkzyT0sf/gB/YKduKRJNzFPu5aUy5HhMWBsJ+col5O\n"
"-----END RSA PRIVATE KEY-----\n";


const char* pubkeystr = 
"-----BEGIN PUBLIC KEY-----\n"
"MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA8QDpGCUDHVNANQmR81ga\n"
"oxD4mJGD+YdRe2uohjfvN4GLLg8N5a6MNBDYPhB8l24TgmtWUDzqSIIvpcTCi8Fx\n"
"eZEyQByB/0/u8iCz+Up0hkiqGU0kN8AnBcrE2y+z0qy/dq+icBv9KmwRmCAoW2tB\n"
"KRvQG3V0KE0wvGzQ55u6bUSh+qsbuDQNkWYab+5ZnVt2WwF51aOLTjFdprc0ligc\n"
"zaz7mktNW9Mg6+FJjN1oVqfYh94W76bEoso33GmFV2e7eerBY/lWZjMvTiIqktn5\n"
"C44M82ipjndrORY8jxkstiSv7r109ocbeF895o+9LjAlh9TxKsy0DyZTF+01xKRF\n"
"zQIDAQAB\n"
"-----END PUBLIC KEY-----";


int loadFile(const char* fname, int& len, unsigned char* &buf, bool binary=true)
{
    len = 0;
    buf = NULL;

    FILE* fp = fopen(fname, binary?"rb":"rt");
    if (fp)
    {
        char* buffer[1024];
        int bytes;
        for (;;)
        {
            bytes = fread(buffer, 1, sizeof(buffer), fp);
            if (!bytes)
                break;
            buf = (unsigned char*)realloc(buf, len + bytes + 1);
            memcpy(buf + len, buffer, bytes);
            len += bytes;
        }
        fclose(fp);
    }
    else
    {
        printf("unable to open file %s\n", fname);
        return -1;
    }

    if(buf)
        buf[len] = '\0';

    return 0;
}

void usage(char* argv[])
{
    printf("usage: %s -e <file-to-encrypt> <trace-level> <iteration-times>\n", argv[0]);
    printf("       %s -d <key-file> <file-to-decrypt> <trace-level> <iteration-times>\n", argv[0]);
}

int main(int argc, char* argv[])
{
    if(argc < 5)
    {
        usage(argv);
        return 0;
    }

    bool isEnc;
    if(strcmp(argv[1], "-e") == 0)
        isEnc = true;
    else if(strcmp(argv[1], "-d") == 0)
        isEnc = false;
    else
    {
        usage(argv);
        return 0;
    }

    if(!isEnc && argc < 6)
    {
        usage(argv);
        return 0;
    }

    if(isEnc)
    {
        unsigned char* din = NULL;
        int inlen = 0;
        int ret = loadFile(argv[2], inlen, din);
        if(ret < 0)
            return ret;

        try
        {
            IZEncryptor* zc = createZEncryptor(pubkeystr);
            zc->setTraceLevel(atoi(argv[3]));
            //zc->setEncoding(false);
            IZDecryptor* zd = createZDecryptor(privkeystr1, NULL);
            zd->setTraceLevel(atoi(argv[3]));
            //zd->setEncoding(false);

            time_t start, end;

            start = time(&start);

            for(int i = 0; i < atoi(argv[4]); i++)
            {
                IZBuffer* buf = NULL;
                IZBuffer* key = NULL;
                zc->encrypt(inlen, din, key, buf);
                IZBuffer* result = zd->decrypt(key?key->length():0, key?key->buffer():NULL, buf->length(), buf->buffer());
                releaseIZ(result);
                //printf("sessionkey length=%d, sessionkey=\n%s\n", key->length(), key->buffer());
                releaseIZ(key);
                releaseIZ(buf);
            }

            time(&end);

            printf("Time taken: %d seconds\n", end - start);

            releaseIZ(zc);
            releaseIZ(zd);
        }
        catch(string str)
        {
            printf("%s\n", str.c_str());
        }

        if(din)
            delete din;
    }
    else
    {
        unsigned char* keyin = NULL;
        int keylen = 0;
        int ret = loadFile(argv[2], keylen, keyin, false);
        if(ret < 0)
            return ret;

        unsigned char* din = NULL;
        int inlen = 0;
        ret = loadFile(argv[3], inlen, din, false);
        if(ret < 0)
            return ret;

        try
        {
            IZDecryptor* zd = createZDecryptor(privkeystr1, NULL);
            zd->setTraceLevel(atoi(argv[4]));
            //zd->setEncoding(false);

            time_t start, end;
            start = time(&start);

            for(int i = 0; i < atoi(argv[5]); i++)
            {
                IZBuffer* result = zd->decrypt(keylen, keyin, inlen, din);
                releaseIZ(result);
            }

            time(&end);
            printf("Time taken: %d seconds\n", end - start);
            releaseIZ(zd);
        }
        catch(string str)
        {
            printf("%s\n", str.c_str());
        }

        if(keyin)
            delete keyin;
        if(din)
            delete din;
    }

    return 0;
}
