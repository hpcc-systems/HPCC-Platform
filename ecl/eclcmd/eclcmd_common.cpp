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

#include <stdio.h>
#include "jlog.hpp"
#include "jfile.hpp"
#include "jargv.hpp"
#include "build-config.h"

#include "ws_workunits.hpp"
#include "eclcmd_common.hpp"

bool extractEclCmdOption(StringBuffer & option, IProperties * globals, const char * envName, const char * propertyName, const char * defaultPrefix, const char * defaultSuffix)
{
    if (option.length())        // check if already specified via a command line option
        return true;
    if (propertyName && globals->getProp(propertyName, option))
        return true;
    if (envName && *envName)
    {
        const char * env = getenv(envName);
        if (env)
        {
            option.append(env);
            return true;
        }
    }
    if (defaultPrefix)
        option.append(defaultPrefix);
    if (defaultSuffix)
        option.append(defaultSuffix);
    return false;
}

bool extractEclCmdOption(StringAttr & option, IProperties * globals, const char * envName, const char * propertyName, const char * defaultPrefix, const char * defaultSuffix)
{
    if (option)
        return true;
    StringBuffer temp;
    bool ret = extractEclCmdOption(temp, globals, envName, propertyName, defaultPrefix, defaultSuffix);
    option.set(temp.str());
    return ret;
}

bool extractEclCmdOption(bool & option, IProperties * globals, const char * envName, const char * propertyName, bool defval)
{
    StringBuffer temp;
    bool ret = extractEclCmdOption(temp, globals, envName, propertyName, defval ? "1" : "0", NULL);
    option=(streq(temp.str(),"1")||strieq(temp.str(),"true"));
    return ret;
}

bool extractEclCmdOption(unsigned & option, IProperties * globals, const char * envName, const char * propertyName, unsigned defval)
{
    StringBuffer temp;
    bool ret = extractEclCmdOption(temp, globals, envName, propertyName, NULL, NULL);
    option = (ret) ? strtoul(temp.str(), NULL, 10) : defval;
    return ret;
}

static bool looksLikeOnlyAWuid(const char * wuid)
{
    if (!wuid)
        return false;
    if (wuid[0] != 'W')
        return false;
    if (!isdigit(wuid[1]) || !isdigit(wuid[2]) || !isdigit(wuid[3]) || !isdigit(wuid[4]))
        return false;
    if (!isdigit(wuid[5]) || !isdigit(wuid[6]) || !isdigit(wuid[7]) || !isdigit(wuid[8]))
        return false;
    if (wuid[9]!='-')
        return false;
    for (int i=10; wuid[i]; i++)
        if (wuid[i]!='-' && !isdigit(wuid[i]))
            return false;
    return true;
}

//=========================================================================================

static const char * skipWhitespace(const char * s)
{
    while (*s && strchr(" \t\r\n", *s))
        s++;
    return s;
}

static const char * skipXmlDeclaration(const char *s)
{
    while (*s)
    {
        s = skipWhitespace(s);
        if (*s!='<' || (s[1]!='?'))
            return s;
        while (*s && *s++!='>');
    }
    return s;
}

#define PE_OFFSET_LOCATION_IN_DOS_SECTION 0x3C

class determineEclObjParameterType
{
public:
    determineEclObjParameterType(const char *path, unsigned _accept=0x00) : accept(_accept)
    {
        file.setown(createIFile(path));
        io.setown(file->open(IFOread));
        read(io, 0, (size32_t) 1024, signature);
        signature.append((char)0);
    }
    bool isArchive()
    {
        const char *s = skipXmlDeclaration(signature.toByteArray());
        return (strnicmp("<Archive", s, 8)==0);
    }
    bool isElfFile()
    {
        const char *s = signature.toByteArray();
        return (s[0]==0x7F && s[1]=='E' && s[2]=='L' && s[3]=='F');
    }
    bool isPEFile()
    {
        if (file->size()<=PE_OFFSET_LOCATION_IN_DOS_SECTION)
            return false;
        const char *buffer = signature.toByteArray();
        if (memcmp("MZ", buffer, 2)!=0)
            return false;
        unsigned long PESectStart = *((unsigned long *)(buffer + PE_OFFSET_LOCATION_IN_DOS_SECTION));
        if (file->size()<=PESectStart)
            return false;
        unsigned len = signature.length()-1;
        if (len<PESectStart+2)
        {
            signature.setLength(len);
            read(io, len, (size32_t) PESectStart+2-len, signature);
            signature.append((char)0);
        }
        if (signature.length()<PESectStart+3)
            return false;
        buffer = signature.toByteArray()+PESectStart;
        if (memcmp("PE", buffer, 2)!=0)
            return false;
        return true;
    }
    eclObjParameterType checkType()
    {
        if ((!accept || accept & eclObjSharedObject) && isElfFile() || isPEFile())
            return eclObjSharedObject;
        if ((!accept || accept & eclObjArchive) && isArchive())
            return eclObjArchive;
        else if (!accept || accept & eclObjSource)
            return eclObjSource;
        else if (accept & eclObjQueryId)
            return eclObjQueryId;
        return eclObjTypeUnknown;
    }
public:
    Owned<IFile> file;
    Owned<IFileIO> io;
    MemoryBuffer signature;
    unsigned accept;
};


EclObjectParameter::EclObjectParameter(unsigned _accept) : type(eclObjTypeUnknown), accept(_accept)
{
}

eclObjParameterType EclObjectParameter::set(const char *_value)
{
    value.set(_value);
    if (checkFileExists(_value))
    {
        determineEclObjParameterType fileType(_value, accept);
        type = fileType.checkType();
    }
    else if (looksLikeOnlyAWuid(_value))
        type = eclObjWuid;
    return type;
}

const char *EclObjectParameter::queryTypeName()
{
    switch (type)
    {
    case eclObjWuid:
        return "WUID";
    case eclObjSource:
        return "ECL File";
    case eclObjArchive:
        return "ECL Archive";
    case eclObjSharedObject:
        return "ECL Shared Object";
    case eclObjTypeUnknown:
    default:
        return "Unknown";
    }
}

StringBuffer &EclObjectParameter::getDescription(StringBuffer &s)
{
    return s.append(queryTypeName()).append(' ').append(value.get());
}
