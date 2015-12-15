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



#ifndef __JARGV__
#define __JARGV__

#include "jiface.hpp"
#include "jfile.hpp"

typedef IArrayOf<IFile> IFileArray;
//Supports wildcards and @ to indicate list of filenames provided in a file.
extern jlib_decl bool processArgvFilename(IFileArray & filenames, const char * filename);
extern jlib_decl bool processArgvFilenamesFromFile(IFileArray & filenames, const char * filename);
extern jlib_decl void expandCommaList(StringArray & target, const char * text);

class jlib_decl ArgvIterator
{
public:
    ArgvIterator(int _argc, const char* _argv[]) :
        argv(_argv), argc(_argc)
    {
        cur = 1;
    }

    bool matchFlag(StringAttr & value, const char * name);                  //-Xvalue, -X option
    bool matchFlag(bool & value, const char * name);                            //-X -X-
    bool matchOption(StringAttr & value, const char * name);                //-option[=value], -option value
    bool matchOption(unsigned & value, const char * name);                  //-option[=value], -option value
    bool matchPathFlag(StringBuffer & option, const char * name);           //-Ivalue, -I value

    inline const char * query() { return cur < argc ? argv[cur] : NULL; }
    inline bool hasMore(int num) { return (cur + num) < argc; }
    inline bool first() { cur = 1; return isValid(); }
    inline bool isValid() const { return cur < argc; }
    inline bool next() { cur++; return isValid(); }
    inline bool done() const { return cur >= argc; }

private:
    const char * const *argv;
    int argc;
    int cur;
};

#endif
