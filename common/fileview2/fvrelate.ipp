/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef FVRELATE_IPP
#define FVRELATE_IPP

#include "fvrelate.hpp"
#include "dadfs.hpp"
#include "deftype.hpp"
#include "jiter.ipp"
#include "fvresultset.ipp"

class ViewFile;
class ViewRelation;
typedef IArrayOf<ViewFile> ViewFileArray;
typedef CIArrayOf<ViewRelation> ViewRelationArray;
typedef CopyCIArrayOf<ViewFile> ViewFileCopyArray;
typedef CopyCIArrayOf<ViewRelation> ViewRelationCopyArray;

//---------------------------------------------------------------------------

//Common options which are required by the ViewFile class - e.g., top create a INewResultSet
struct ViewProcessOptions
{
public:
    ViewProcessOptions(IResultSetFactory & _resultSetFactory, const char * _cluster)
        : resultSetFactory(&_resultSetFactory), cluster(_cluster)
    {
    }

public:
    Linked<IResultSetFactory> resultSetFactory;
    StringAttr cluster;
};


//---------------------------------------------------------------------------


//Note: All files and relationships are owned by the ViewFileWeb class - otherwise there would
//be circular link counted references, so it would never be released.
class FILEVIEW_API ViewFile : public CInterface, implements IViewRelatedFile
{
    friend class ViewFileWeb;
public:
    ViewFile(unsigned _uid, ViewProcessOptions & _options, IDistributedFile * _definition) : uid(_uid), options(_options), definition(_definition) {}
    IMPLEMENT_IINTERFACE

    virtual IDistributedFile * queryDistributedFile() const { return definition; }
    virtual INewResultSet * queryResultSet();

    void addRelation(ViewRelation & _relation);
    void addSuperFile(ViewFile & _superFile);
    virtual void addSubFile(ViewFile & _subFile);
    const char * queryFilename() const;

    inline IDistributedFile * queryDefinition() { return definition; }
    inline unsigned queryFirstFieldId() { return 0; }           // MORE: Look at the meta and deduce - not correct if it starts with a record
    inline unsigned queryId() const { return uid; }
    inline bool matches(const char * search) const { return stricmp(search, queryFilename()) == 0; }

protected:
    const ViewProcessOptions & options;
    Linked<IDistributedFile> definition;
    Linked<INewResultSet> cachedResultSet;
    unsigned uid;
    ViewRelationCopyArray relations;
    ViewFileCopyArray superFiles;
};

class FILEVIEW_API ViewSuperFile : public ViewFile
{
public:
    ViewSuperFile(unsigned _uid, ViewProcessOptions & _options, IDistributedFile * _definition) : ViewFile(_uid, _options, _definition) {}

    virtual void addSubFile(ViewFile & _subFile);

protected:
    ViewFileCopyArray subFiles;
};


class FILEVIEW_API ViewJoinColumnMapping : public CInterface
{
    friend class ViewRelation;
public:
    ViewJoinColumnMapping(ViewJoinColumn * _primary, ViewJoinColumn * _secondary, unsigned _secondaryIteratorColumn)
        : primary(_primary), secondary(_secondary), secondaryIteratorColumn(_secondaryIteratorColumn)
    {}

    void addFilterToPrimary(IFilteredResultSet * primaryResultSet, IResultSetCursor * secondaryCursor);
    void addFilterToSecondary(IFilteredResultSet * secondaryResultSet, IResultSetCursor * primaryCursor);

    bool canMapPrimaryToSecondary() const;
    bool canMapSecondaryToPrimary() const;

protected:
    void doAddFilterToPrimary(IFilteredResultSet * primaryResultSet, IResultSetCursor * secondaryCursor);

protected:
    Linked<ViewJoinColumn> primary;
    Linked<ViewJoinColumn> secondary;
    unsigned secondaryIteratorColumn;
};


class FILEVIEW_API ViewRelation : public CInterface, implements IViewRelation
{
public:
    ViewRelation(unsigned _uid, IFileRelationship * _definition, ViewFile * _primaryFile, ViewFile * _secondaryFile)
        : uid(_uid), definition(_definition), primaryFile(_primaryFile), secondaryFile(_secondaryFile)
    {
    }
    IMPLEMENT_IINTERFACE

    virtual IFileRelationship * queryFileRelationship() const { return definition; }
    virtual unsigned numMappingFields() const { return columnMappings.ordinality(); }
    virtual unsigned queryMappingField(unsigned whichMapping, bool needPrimary) const;

    void init();
    bool matches(IFileRelationship * _definition, ViewFile * _primaryFile, ViewFile * _secondaryFile) const;

    void queryPrimaryColumns(UnsignedArray & columns);
    void querySecondaryColumns(UnsignedArray & columns);

    void addFilterToPrimary(IFilteredResultSet * primaryResultSet, IResultSetCursor * secondaryCursor);
    void addFilterToSecondary(IFilteredResultSet * secondaryResultSet, IResultSetCursor * primaryCursor);

    bool canMapPrimaryToSecondary(bool isEfficient) const;
    bool canMapSecondaryToPrimary(bool isEfficient) const;

    inline IFileRelationship * queryDefinition() const { return definition; }
    inline unsigned queryId() const { return uid; }
    inline ViewFile * queryPrimary() const { return primaryFile; }
    inline ViewFile * querySecondary() const { return secondaryFile; }

protected:
    unsigned uid;
    Linked<IFileRelationship> definition;
    CIArrayOf<ViewJoinColumnMapping> columnMappings;
    ViewFile * primaryFile;
    ViewFile * secondaryFile;
};

interface IViewFileWebVisitor
{
public:
    virtual void visit(ViewFile * file) = 0;
    virtual void visit(ViewRelation * relation) = 0;
};

typedef CArrayIteratorOf<IViewRelatedFile, IViewRelatedFileIterator> CViewRelatedFileIterator;

class CFileTreeBrowser;
struct BrowseCreateInfo
{
public:
    inline BrowseCreateInfo(CFileTreeBrowser * _browser, bool _isEfficient, unsigned _maxRecursion) 
        : browser(_browser), isEfficient(_isEfficient), maxRecursion(_maxRecursion) {}

    unsigned getNestingCount(const ViewFile * search);
    
public:
    CFileTreeBrowser * browser;
    unsigned maxRecursion;
    bool isEfficient;
    CopyCIArrayOf<ViewFile> nestedFiles;
};

struct ViewWalkOptions : public ViewGatherOptions
{
    ViewWalkOptions(const ViewGatherOptions & _options) : ViewGatherOptions(_options) { isExplicitFile = false; }

    bool isExplicitFile;
};

class CRelatedBrowseFile;
class FILEVIEW_API ViewFileWeb : public CInterface, implements IViewFileWeb
{
public:
    ViewFileWeb(IResultSetFactory & resultSetFactory, const char * cluster, IUserDescriptor *user);
    IMPLEMENT_IINTERFACE

    virtual void gatherWeb(const char * rootFilename, const ViewGatherOptions & options);
    virtual void gatherWeb(const char * rootFilename, IDistributedFile * alreadyResolved, const ViewGatherOptions & options);
    virtual void gatherWebFromPattern(const char * filenamePattern, const ViewGatherOptions & options);
    virtual IViewRelatedFileIterator * getFileIterator();
    virtual IViewRelatedFile * queryFile(unsigned i);
    virtual IFileTreeBrowser * createBrowseTree(const char * browseRootFilename, bool isEfficient, unsigned maxRecursion);

    ViewFile * findFile(const char * filename);
    void walk(IViewFileWebVisitor & visitor);

protected:
    ViewFile * addFile(IDistributedFile * definition);
    ViewRelation * addRelation(IFileRelationship * _definition, ViewFile * _primaryFile, ViewFile * _secondaryFile);

    void gatherBrowseFiles(BrowseCreateInfo & info, ViewFile * file, CRelatedBrowseFile * parentFile, ViewRelation * parentRelation);
    ViewFile * walkFile(const char * filename, IDistributedFile * alreadyResolved, ViewWalkOptions & options);

protected:
    unsigned nextId;
    ViewProcessOptions viewOptions;
    IDistributedFileDirectory & directory;
    ViewFileArray files;
    ViewRelationArray relations;
    Owned<IUserDescriptor> udesc;
};

//---------------------------------------------------------------------------

class FilteredSecondaryResultSetCursor : public DelayedFilteredResultSetCursor
{
public:
    FilteredSecondaryResultSetCursor(INewResultSet * _resultSet, IResultSetCursor * _primary, ViewRelation * _relation);

    virtual void noteRelatedFileChanged();
    virtual void ensureFiltered();

protected:
    IResultSetCursor * primary;
    ViewRelation * relation;
};


class FilteredPrimaryResultSetCursor : public DelayedFilteredResultSetCursor
{
public:
    FilteredPrimaryResultSetCursor(INewResultSet * _resultSet, IResultSetCursor * _secondary, ViewRelation * _relation);

    virtual void noteRelatedFileChanged();
    virtual void ensureFiltered();

protected:
    IResultSetCursor * secondary;
    ViewRelation * relation;
};



class CRelatedBrowseFile : public CInterface, implements IRelatedBrowseFile, implements IResultSetFilter
{
public:
    CRelatedBrowseFile(ViewFile * _file, CRelatedBrowseFile * _parent, ViewRelation * _relation);
    IMPLEMENT_IINTERFACE

    void addChild(CRelatedBrowseFile * child);

    virtual IViewRelatedFile * queryDefinition() const;
    virtual IViewRelation * queryParentRelation() const;
    virtual IRelatedBrowseFile * queryParent() const;
    virtual IRelatedBrowseFile * queryChild(unsigned i) const;
    virtual IResultSetCursor * queryCursor();

    virtual void clearFilter(unsigned columnIndex)
    {
        filtered->clearFilter(columnIndex);
    }
    virtual void addFilter(unsigned columnIndex, const char * value)
    {
        filtered->addFilter(columnIndex, strlen(value), value);
    }
    virtual void clearFilters()
    {
        filtered->clearFilters();
    }
    virtual void addFilter(unsigned columnIndex, unsigned length, const char * utf8Value)
    {
        filtered->addFilter(columnIndex, length, utf8Value);
    }
    virtual void addNaturalFilter(unsigned columnIndex, unsigned length, const char * utf8Value)
    {
        filtered->addNaturalFilter(columnIndex, length, utf8Value);
    }

protected:
    ViewFile * file;
    CRelatedBrowseFile * parent;
    ViewRelation * relation;
    Owned<DelayedFilteredResultSetCursor> filtered;
    Owned<NotifyingResultSetCursor> notifier;
    CopyCIArrayOf<CRelatedBrowseFile> children;
};


class CFileTreeBrowser : public CInterface, implements IFileTreeBrowser
{
public:
    CFileTreeBrowser(ViewFileWeb * _web);
    IMPLEMENT_IINTERFACE

    CRelatedBrowseFile * addFile(ViewFile * file, CRelatedBrowseFile * parent, ViewRelation * parentRelation);

    virtual IRelatedBrowseFile * queryRootFile();
    virtual IResultSetFilter * queryRootFilter();

protected:
    Linked<ViewFileWeb> web;
    CIArrayOf<CRelatedBrowseFile> files;
};

//---------------------------------------------------------------------------

interface IErDiagramBuilder
{
    virtual void beginFile(const char * id, const char * name, IDistributedFile * definition) = 0;
    virtual void noteField(const char * id, const char * name, const char * type) = 0;
    virtual void endFile() = 0;

    virtual void beginCompound() = 0;
    virtual void endCompound() = 0;

    virtual void noteRelation(const char * id, const char * sourceId, const char * targetId, IFileRelationship * definition) = 0;
};

class FILEVIEW_API ViewERdiagramVisitor : public IViewFileWebVisitor, public ISchemaBuilder
{
public:
    ViewERdiagramVisitor(IErDiagramBuilder & _builder) : builder(_builder) { activeFieldCount = 0; }

// IViewFileWebVisitor
    virtual void visit(ViewFile * file);
    virtual void visit(ViewRelation * relation);

// ISchemaBuilder
    virtual void addField(const char * name, ITypeInfo & type);
    virtual void addSetField(const char * name, const char * itemname, ITypeInfo & type);
    virtual void beginIfBlock();
    virtual bool beginDataset(const char * name, const char * childname, bool mixedContent, unsigned *updateMixed);
    virtual void beginRecord(const char * name, bool mixedContent, unsigned *updateMixed);
    virtual void updateMixedRecord(unsigned updateMixed, bool mixed){}
    virtual void endIfBlock();
    virtual void endDataset(const char * name, const char * childname);
    virtual void endRecord(const char * name);
    virtual bool addSingleFieldDataset(const char * name, const char * childname, ITypeInfo & type) { return false; }

protected:
    virtual void noteNextField();

protected:
    IErDiagramBuilder & builder;
    StringBuffer activeFileId;
    StringBuffer activeFieldPrefix;
    StringBuffer activeFieldId;
    unsigned activeFieldCount;
    UnsignedArray savedFieldCounts; 
};

class FILEVIEW_API ViewXgmmlERdiagramVisitor : public IErDiagramBuilder
{
public:
    ViewXgmmlERdiagramVisitor(StringBuffer & _xgmml) : xgmml(_xgmml) { depth = 0; }

    virtual void beginFile(const char * id, const char * name, IDistributedFile * definition);
    virtual void noteField(const char * id, const char * name, const char * type);
    virtual void endFile();

    virtual void beginCompound();
    virtual void endCompound();

    virtual void noteRelation(const char * id, const char * sourceId, const char * targetId, IFileRelationship * definition);

    void beginDiagram();
    void endDiagram();

protected:
    ViewXgmmlERdiagramVisitor & addAttr(const char * name, const char * value);
    ViewXgmmlERdiagramVisitor & addAttrTag(const char * name, const char * value);

protected:
    StringBuffer & xgmml;
    unsigned depth;
};

#endif
