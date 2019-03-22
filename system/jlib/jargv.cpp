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



#include "jargv.hpp"
#include "jlog.hpp"

//=========================================================================================

void expandCommaList(StringArray & target, const char * text)
{
    const char * cur = text;
    for (;;)
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
        UERRLOG("Error: File '%s' does not exist", filename);
        return false;
    }

    while (!feof(in))
    {
        if (fgets(buffer, sizeof(buffer), in))
        {
            size_t len = strlen(buffer);
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
            UERRLOG("Error: File '%s' does not exist", filename);
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

    UERRLOG("Error: Need to supply a value for %s", arg);
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

    UERRLOG("Error: %s needs to supply an associated value", arg);
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

    UERRLOG("Error: %s needs to specify a directory", arg);
    return false;
}


