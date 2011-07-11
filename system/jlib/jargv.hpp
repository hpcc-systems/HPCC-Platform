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
      argc(_argc), argv(_argv)
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
