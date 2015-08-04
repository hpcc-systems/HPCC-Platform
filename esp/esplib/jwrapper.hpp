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

#ifndef __JLIBPLUS_HPP
#define __JLIBPLUS_HPP

#include "jstring.hpp"

namespace esp
{

inline size32_t readFile(const char* name,StringBuffer& buf)
{
    size32_t sz = 0;
#ifdef _WIN32
    int fd = open(name,O_RDONLY|O_BINARY);
    if(fd)
    {
        struct _stat st;
        if(!_fstat(fd,&st))
        {
            void * data = buf.reserve((size32_t)st.st_size);
            sz = read(fd, data, st.st_size);
        }
        close(fd);
    }
#else
    int fd = open(name,O_RDONLY);
    if(fd)
    {
        struct stat st;
        if(!fstat(fd,&st))
        {
            void * data = buf.reserve((size32_t)st.st_size);
            sz = read(fd, data, st.st_size);
        }
        close(fd);
    }
#endif
    return sz;
}



}

#endif
