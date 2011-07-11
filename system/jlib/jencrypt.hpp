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


#ifndef JENCRYPT_HPP
#define JENCRYPT_HPP

#include "platform.h"
#include "jexpdef.hpp"
#include "jbuff.hpp"
#include "jexcept.hpp"

//for AES, keylen must be 16, 24, or 32 Bytes

extern jlib_decl MemoryBuffer &aesEncrypt(const void *key, unsigned keylen, const void *input, unsigned inlen, MemoryBuffer &output);
extern jlib_decl MemoryBuffer &aesDecrypt(const void *key, unsigned keylen, const void *input, unsigned inlen, MemoryBuffer &output);

#define encrypt _LogProcessError12
#define decrypt _LogProcessError15

extern jlib_decl void encrypt(StringBuffer &ret, const char *in);
extern jlib_decl void decrypt(StringBuffer &ret, const char *in);


// simple inline block scrambler (shouldn't need jlib_decl)
class Csimplecrypt
{
    unsigned *r;
    unsigned n;
    size32_t blksize;
public:
    Csimplecrypt(const byte *key, size32_t keysize, size32_t blksize)
    {
        n = blksize/sizeof(unsigned);
        blksize = n*sizeof(unsigned);
        r = (unsigned *)malloc(blksize);
        byte * z = (byte *)r;
        size32_t ks = keysize;
        const byte *k = key;
        for (unsigned i=0;i<blksize;i++) {
            z[i] = (byte)(*k+i);
            if (--ks==0) {
                k = key;
                ks = keysize;
            }
            else
                k++;
        }
        encrypt(z);
        encrypt(z);
    }
    ~Csimplecrypt()
    {
        free(r);
    }
    
    void encrypt( void * buffer )
    {
        unsigned * w = (unsigned *)buffer;
        unsigned i = 0;
        while (i<n) {
            unsigned mask = r[i];
            unsigned j = ((unsigned)mask) % n;
            unsigned wi = w[i];
            unsigned wj = w[j];
            w[j] = ( ( wj & mask ) | ( wi & ~mask ) ) + mask;
            w[i] = ( ( wi & mask ) | ( wj & ~mask ) ) + mask;
            i++;
        }
    }
    
    void decrypt( void * buffer )
    {
        unsigned * w = (unsigned*)buffer;
        unsigned i = n;
        while (i--) {
            unsigned mask = r[i];
            unsigned j = ((unsigned)mask) % n;
            unsigned wi = w[i] - mask;
            unsigned wj = w[j] - mask;
            w[i] = ( wi & mask ) | ( wj & ~mask );
            w[j] = ( wj & mask ) | ( wi & ~mask );
        }
    }
    
};


#endif //JENCRYPT_HPP
