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

#ifndef __ESDL_UTILS_HPP__
#define __ESDL_UTILS_HPP__

#include "esdldecl.hpp"
#include "platform.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>

// directory
bool es_checkDirExists(const char * filename);
void es_createDirectory(const char* dir);

// create file
int es_createFile(const char* src, const char* ext);
int es_createFile(const char* src, const char* tail, const char* ext);

// filenames
char * es_changeext(const char *fn,const char *ext);
char * es_changetail(const char *fn,const char *tail, const char *ext);
bool es_hasext(const char *fn,const char *ext);
char * es_gettail(const char *fn);

#endif
