/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#include "jliball.hpp"
#include "hql.hpp"
#include "hqlcache.hpp"
#include "hqlcollect.hpp"
#include "hqlexpr.hpp"
#include "hqlutil.hpp"
#include "hqlerrors.hpp"
#include "junicode.hpp"
#include "hqlplugins.hpp"

//---------------------------------------------------------------------------------------------------------------------

void convertSelectsToPath(StringBuffer & filename, const char * eclPath)
{
    for(;;)
    {
        const char * dot = strchr(eclPath, '.');
        if (!dot)
            break;
        filename.appendLower(dot-eclPath, eclPath);
        addPathSepChar(filename);
        eclPath = dot + 1;
    }
    filename.appendLower(eclPath);
}

//---------------------------------------------------------------------------------------------------------------------

void getFileContentText(StringBuffer & result, IFileContents * contents)
{
    unsigned len = contents->length();
    const char * text = contents->getText();
    if ((len >= 3) && (memcmp(text, UTF8_BOM, 3) == 0))
    {
        len -= 3;
        text += 3;
    }
    result.append(len, text);
}

void setDefinitionText(IPropertyTree * target, const char * prop, IFileContents * contents, bool checkDirty)
{
    StringBuffer sillyTempBuffer;
    getFileContentText(sillyTempBuffer, contents);  // We can't rely on IFileContents->getText() being null terminated..
    target->setProp(prop, sillyTempBuffer);

    ISourcePath * sourcePath = contents->querySourcePath();
    target->setProp("@sourcePath", str(sourcePath));
    if (checkDirty && contents->isDirty())
    {
        target->setPropBool("@dirty", true);
    }

    timestamp_type ts = contents->getTimeStamp();
    if (ts)
        target->setPropInt64("@ts", ts);
}

//---------------------------------------------------------------------------------------------------------------------

static void extractFile(const char * path, const char * moduleName, const char * attrName, const char * text, timestamp_type ts)
{
    StringBuffer filename;
    filename.append(path);
    if (moduleName && *moduleName)
        convertSelectsToPath(filename, moduleName);
    if (attrName)
    {
        addPathSepChar(filename);
        convertSelectsToPath(filename, attrName);
        filename.append(".ecl");
    }
    else
        filename.append(".ecllib");
    recursiveCreateDirectoryForFile(filename);

    Owned<IFile> file = createIFile(filename);
    Owned<IFileIO> io = file->open(IFOcreate);
    if (text)
        io->write(0, strlen(text), text);
    io->close();
    io.clear();
    if (ts)
    {
        CDateTime timeStamp;
        timeStamp.setTimeStamp(ts);
        file->setTime(&timeStamp, &timeStamp, &timeStamp);
    }
}


extern HQL_API void expandArchive(const char * path, IPropertyTree * archive, bool includePlugins)
{
    StringBuffer baseFilename;
    makeAbsolutePath(path, baseFilename, false);
    addPathSepChar(baseFilename);
    unsigned int embeddedArchiveNum = 0;

    // Look for embedded archives and recursively expand them
    Owned<IPropertyTreeIterator> embeddedArchives = archive->getElements("Archive");
    ForEach(*embeddedArchives)
    {
        // Append the package value to the path, if it exists
        StringBuffer embeddedFilename(baseFilename);
        if (embeddedArchives->query().hasProp("@package"))
        {
            embeddedFilename.append(embeddedArchives->query().queryProp("@package"));
        }
        else
        {
            embeddedFilename.appendf("archive_%0*d", 6, ++embeddedArchiveNum);
        }
        expandArchive(embeddedFilename, &embeddedArchives->query(), includePlugins);
    }

    Owned<IPropertyTreeIterator> modules = archive->getElements("Module");
    ForEach(*modules)
    {
        IPropertyTree & curModule = modules->query();
        const char * moduleName = curModule.queryProp("@name");
        if (curModule.hasProp("Text"))
        {
            if (includePlugins || !curModule.hasProp("@plugin"))
                extractFile(baseFilename, moduleName, nullptr, curModule.queryProp("Text"), curModule.getPropInt64("@ts"));
        }
        else
        {
            Owned<IPropertyTreeIterator> attrs = curModule.getElements("Attribute");
            ForEach(*attrs)
            {
                IPropertyTree & curAttr = attrs->query();
                const char * attrName = curAttr.queryProp("@name");
                extractFile(baseFilename, moduleName, attrName, curAttr.queryProp(""), curAttr.getPropInt64("@ts"));
            }
        }
    }
}
