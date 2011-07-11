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
