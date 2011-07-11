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

#ifndef FVRELATE_HPP
#define FVRELATE_HPP

#include "fileview.hpp"

#define CHEAP_UCHAR_DEF
#ifdef _WIN32
typedef wchar_t UChar;
#else //__WIN32
typedef unsigned short UChar;
#endif //__WIN32

typedef void (*stringFieldTransformerFunction)(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src);
typedef void (*utf8FieldTransformerFunction)(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src);
typedef void (*unicodeFieldTransformerFunction)(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, const UChar * src);

//Registry which can be used for registering transformations that can be included in the field lists.
//It is also used for plugins required by dlls loaded to translate alien data types
interface IViewTransformerRegistry
{
public:
    virtual void addFieldStringTransformer(const char * name, stringFieldTransformerFunction func) = 0;
    virtual void addFieldUtf8Transformer(const char * name, utf8FieldTransformerFunction func) = 0;
    virtual void addFieldUnicodeTransformer(const char * name, unicodeFieldTransformerFunction func) = 0;
    virtual void addPlugins(const char * pluginname) = 0;
};
extern FILEVIEW_API IViewTransformerRegistry & queryTransformerRegistry();

//---------------------------------------------------------------------------

struct ViewGatherOptions
{
    inline ViewGatherOptions()
    {
        primaryDepth = 100;
        secondaryDepth = 100;
        maximumDepth = 200;
        kind = NULL;            // defaults to link
        superDepth = 0;
        subDepth = 0;
        payload = NULL;
        requirePayload = false;
        explicitFilesOnly = false;
    }

    inline void setPayloadFilter(bool _value)
    {
        requirePayload = _value;
        payload = &requirePayload;
    }

    unsigned primaryDepth;              // How many secondary->primary links to follow
    unsigned secondaryDepth;            // How many primary->secondary links to follow
    unsigned maximumDepth;              // maximum number of links
    const char * kind;
    unsigned superDepth;                // How many super files to walk up
    unsigned subDepth;
    bool * payload;
    bool requirePayload;
    bool explicitFilesOnly;             // If true, only files supplied to gatherFile/gatherFilePattern will be included
};

//---------------------------------------------------------------------------

interface IFileTreeBrowser;
interface IFileRelationship;

interface IViewRelatedFile : public IInterface
{
    virtual IDistributedFile * queryDistributedFile() const = 0;
    virtual INewResultSet * queryResultSet() = 0;
};

interface IViewRelatedFileIterator: public IIteratorOf<IViewRelatedFile> {};

interface IViewRelation : public IInterface
{
    virtual IFileRelationship * queryFileRelationship() const = 0;
    virtual unsigned numMappingFields() const = 0;
    virtual unsigned queryMappingField(unsigned whichMapping, bool needPrimary) const = 0;
};

interface IViewFileWeb : public IInterface
{
public:
    virtual void gatherWeb(const char * rootFilename, const ViewGatherOptions & options) = 0;
    virtual void gatherWebFromPattern(const char * pattern, const ViewGatherOptions & options) = 0;
    virtual IViewRelatedFile * queryFile(unsigned i) = 0;
    virtual IViewRelatedFileIterator * getFileIterator() = 0;

    //Create a tree of related file cursors starting from a particular file.
    //Only files that can be reached will be added to the view tree.
    virtual IFileTreeBrowser * createBrowseTree(const char * browseRootFilename, bool isEfficient=true, unsigned maxRecursion=3) = 0;
};

interface IRelatedBrowseFile
{
public:
    virtual IViewRelatedFile * queryDefinition() const = 0;
    virtual IViewRelation * queryParentRelation() const = 0;
    virtual IRelatedBrowseFile * queryParent() const = 0;
    virtual IRelatedBrowseFile * queryChild(unsigned i) const = 0;
    virtual IResultSetCursor * queryCursor() = 0;
};

interface IFileTreeBrowser : public IInterface
{
    virtual IRelatedBrowseFile * queryRootFile() = 0;
    virtual IResultSetFilter * queryRootFilter() = 0;
};

extern FILEVIEW_API IViewFileWeb * createViewFileWeb(IResultSetFactory & resultSetFactory, const char * cluster);
extern FILEVIEW_API void createERdiagram(StringBuffer & xgmml, IViewFileWeb & web);

#endif
