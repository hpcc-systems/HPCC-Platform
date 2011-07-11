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



#include "jargv.hpp"
#include "jlog.hpp"

//=========================================================================================

void expandCommaList(StringArray & target, const char * text)
{
    const char * cur = text;
    loop
    {
        const char * comma = strchr(cur, ',');
        if (!comma)
            break;
        if (comma != cur)
        {
            StringAttr temp(cur, comma-cur);
            target.append(temp);
        }
        cur = comma+1;
    }
    if (*cur)
        target.append(cur);
}


bool processArgvFilenamesFromFile(IFileArray & filenames, const char * filename)
{
    char buffer[255];
    FILE * in = fopen(filename, "r");
    if (!in)
    {
        ERRLOG("Error: File '%s' does not exist", filename);
        return false;
    }

    while (!feof(in))
    {
        if (fgets(buffer, sizeof(buffer), in))
        {
            unsigned len = strlen(buffer);
            while (len && !isprint(buffer[len-1]))
                len--;
            buffer[len] = 0;
            if (len)
                processArgvFilename(filenames, filename);           //Potential for infinite recursion if user is daft
        }
    }
    fclose(in);
    return true;
}

bool processArgvFilename(IFileArray & filenames, const char * filename)
{
    if (filename[0] == '@')
        return processArgvFilenamesFromFile(filenames, filename+1);

    if (containsFileWildcard(filename))
    {
        StringBuffer dirPath, dirWildcard;
        splitFilename(filename, &dirPath, &dirPath, &dirWildcard, &dirWildcard);
        Owned<IDirectoryIterator> iter = createDirectoryIterator(dirPath.str(), dirWildcard.str());
        ForEach(*iter)
        {
            IFile & cur = iter->query();
            if (cur.isFile() == foundYes)
                filenames.append(OLINK(cur));
        }
    }
    else
    {
        Owned<IFile> cur = createIFile(filename);
        if (cur->isFile() != foundYes)
        {
            ERRLOG("Error: File '%s' does not exist", filename);
            return false;
        }
        filenames.append(*cur.getClear());
    }
    return true;
}

//=========================================================================================

bool ArgvIterator::matchOption(StringAttr & value, const char * name)
{
    const char * arg = query();
    size_t len = strlen(name);
    if (memcmp(arg, name, len) != 0)
        return false;

    if (arg[len] == '=')
    {
        value.set(arg+len+1);
        return true;
    }

    //Not = => a different option
    if (arg[len] != 0)
        return false;

    if (hasMore(1))
    {
        next();
        value.set(query());
        return true;
    }

    ERRLOG("Error: Need to supply a value for %s", arg);
    return false;
}


bool ArgvIterator::matchOption(unsigned & value, const char * name)
{
    StringAttr text;
    if (!matchOption(text, name))
        return false;
    value = atoi(text);
    return true;
}

// -Xvalue, -X option
bool ArgvIterator::matchFlag(StringAttr & value, const char * name)
{
    const char * arg = query();
    size_t len = strlen(name);
    if (memcmp(arg, name, len) != 0)
        return false;

    if (arg[len] != 0)
    {
        value.set(arg+len);
        return true;
    }

    if (hasMore(1))
    {
        next();
        value.set(query());
        return true;
    }

    ERRLOG("Error: %s needs to supply an associated value", arg);
    return false;
}

bool ArgvIterator::matchFlag(bool & value, const char * name)
{
    const char * arg = query();
    size_t len = strlen(name);
    if (memcmp(arg, name, len) != 0)
        return false;

    char next = arg[len];
    if ((next == 0) || (next == '+'))
    {
        value = true;
        return true;
    }
    if (next == '-')
    {
        value = false;
        return true;
    }
    if (next == '=')
    {
        value = strToBool(arg+len+1);
        return true;
    }
    return false;
}

bool ArgvIterator::matchPathFlag(StringBuffer & option, const char * name)
{
    const char * arg = query();
    size_t len = strlen(name);
    if (memcmp(arg, name, len) != 0)
        return false;

    if (arg[len])
    {
        //Allow -xZ -x=Z
        if (arg[len] == '=')
            len++;
        option.append(ENVSEPCHAR).append(arg+len);
        return true;
    }
    
    if (hasMore(1))
    {
        //-x Z
        next();
        option.append(ENVSEPCHAR).append(query());
        return true;
    }

    ERRLOG("Error: %s needs to specify a directory", arg);
    return false;
}


