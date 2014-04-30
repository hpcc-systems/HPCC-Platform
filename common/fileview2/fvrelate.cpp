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

#include "jliball.hpp"
#include "eclrtl.hpp"

#include "fileview.hpp"
#include "fverror.hpp"
#include "fvrelate.ipp"
#include "deftype.hpp"
#include "fvresultset.ipp"
#include "dasess.hpp"

//---------------------------------------------------------------------------

void ViewFile::addRelation(ViewRelation & _relation) 
{ 
    relations.append(_relation); 
}

void ViewFile::addSuperFile(ViewFile & _superFile) 
{ 
    if (!superFiles.contains(_superFile))
        superFiles.append(_superFile); 
}

void ViewFile::addSubFile(ViewFile & _subFile) 
{
    throwUnexpected();
}

const char * ViewFile::queryFilename() const
{
    return definition->queryLogicalName();
}


INewResultSet * ViewFile::queryResultSet()
{
    if (!cachedResultSet)
    {
        //This used to protect against exceptions - but complicated indexes now complain when the cursor is created, so no need to protect this.
        cachedResultSet.setown(options.resultSetFactory->createNewFileResultSet(queryFilename(), options.cluster));
    }

    return cachedResultSet;
}


void ViewSuperFile::addSubFile(ViewFile & _subFile) 
{ 
    if (!subFiles.contains(_subFile)) 
        subFiles.append(_subFile); 
}

//---------------------------------------------------------------------------

void ViewJoinColumnMapping::addFilterToPrimary(IFilteredResultSet * primaryResultSet, IResultSetCursor * secondaryCursor)
{
    primary->clearFilter(primaryResultSet);
    if (secondaryIteratorColumn != NotFound)
    {
        Owned<IResultSetCursor> childCursor = secondaryCursor->getChildren(secondaryIteratorColumn);
        if (childCursor->first())
        {
            do
            {
                doAddFilterToPrimary(primaryResultSet, childCursor);
            } while (childCursor->next());
        }
    }
    else
        doAddFilterToPrimary(primaryResultSet, secondaryCursor);
}

void ViewJoinColumnMapping::addFilterToSecondary(IFilteredResultSet * secondaryResultSet, IResultSetCursor * primaryCursor)
{
    assertex(secondaryIteratorColumn == NotFound);
    MemoryAttr value;
    primary->getValue(value, primaryCursor);
    secondary->clearFilter(secondaryResultSet);
    secondary->addFilter(secondaryResultSet, value);
}

void ViewJoinColumnMapping::doAddFilterToPrimary(IFilteredResultSet * primaryResultSet, IResultSetCursor * secondaryCursor)
{
    MemoryAttr value;
    secondary->getValue(value, secondaryCursor);
    primary->addFilter(primaryResultSet, value);
}

bool ViewJoinColumnMapping::canMapPrimaryToSecondary() const
{
    return (secondaryIteratorColumn == NotFound) && primary->canGet() && secondary->canSet();
}

bool ViewJoinColumnMapping::canMapSecondaryToPrimary() const
{
    return primary->canSet() && secondary->canGet();
}

//---------------------------------------------------------------------------

inline bool isSameString(const char * left, const char * right)
{
    if (left == right)
        return true;
    if (left && right)
        return strcmp(left, right) == 0;
    return false;
}

void ViewRelation::addFilterToPrimary(IFilteredResultSet * primaryResultSet, IResultSetCursor * secondaryCursor)
{
    ForEachItemIn(i, columnMappings)
        columnMappings.item(i).addFilterToPrimary(primaryResultSet, secondaryCursor);
}

void ViewRelation::addFilterToSecondary(IFilteredResultSet * secondaryResultSet, IResultSetCursor * primaryCursor)
{
    ForEachItemIn(i, columnMappings)
        columnMappings.item(i).addFilterToSecondary(secondaryResultSet, primaryCursor);
}

unsigned ViewRelation::queryMappingField(unsigned whichMapping, bool needPrimary) const
{
    if (whichMapping >= columnMappings.ordinality())
        return NotFound;
    
    ViewJoinColumnMapping & mapping = columnMappings.item(whichMapping);
    if (needPrimary)
        return mapping.primary->queryBaseColumn();
    return mapping.secondary->queryBaseColumn();
}

//Not in the constructor so exceptions can be handled cleanly.
void ViewRelation::init()
{
    INewResultSet * primaryResultSet = primaryFile->queryResultSet();
    INewResultSet * secondaryResultSet = secondaryFile->queryResultSet();
    if (primaryResultSet && secondaryResultSet)
    {
        FieldTransformInfoArray primaryFields;
        FieldTransformInfoArray secondaryFields;
        
        parseColumnMappingList(primaryFields, primaryResultSet->getMetaData(), false, definition->queryPrimaryFields());
        parseColumnMappingList(secondaryFields, secondaryResultSet->getMetaData(), true, definition->querySecondaryFields());

        //MORE: Check that if a secondary dataset column is specified that there is only one of them.
        if (primaryFields.ordinality() == secondaryFields.ordinality())
        {
            ForEachItemIn(i, primaryFields)
            {
                FieldTransformInfo & curPrimary = primaryFields.item(i);
                FieldTransformInfo & curSecondary = secondaryFields.item(i);

                Owned<ViewJoinColumn> primaryColumn = new ViewJoinColumn(curPrimary.column, curPrimary.getTransforms, curPrimary.setTransforms);
                Owned<ViewJoinColumn> secondaryColumn = new ViewJoinColumn(curSecondary.column, curSecondary.getTransforms, curSecondary.setTransforms);
                columnMappings.append(* new ViewJoinColumnMapping(primaryColumn, secondaryColumn, curSecondary.datasetColumn));
            }
        }
        else
            throwError4(FVERR_NumJoinFieldsMismatchXY, primaryFields.ordinality(), secondaryFields.ordinality(), primaryFile->queryFilename(), secondaryFile->queryFilename());
    }
}


bool ViewRelation::matches(IFileRelationship * searchDefinition, ViewFile * searchPrimaryFile, ViewFile * searchSecondaryFile) const
{
    if ((searchPrimaryFile != primaryFile) || (searchSecondaryFile != secondaryFile))
        return false;

    if (!isSameString(definition->queryPrimaryFields(), searchDefinition->queryPrimaryFields()))
        return false;

    if (!isSameString(definition->querySecondaryFields(), searchDefinition->querySecondaryFields()))
        return false;

    if (!isSameString(definition->queryKind(), searchDefinition->queryKind()))
        return false;

    if (definition->isPayload() != searchDefinition->isPayload())
        return false;
    
    return true;
}

bool ViewRelation::canMapPrimaryToSecondary(bool isEfficient) const
{
    //MORE: Implement isEfficient!
    ForEachItemIn(i, columnMappings)
        if (!columnMappings.item(i).canMapPrimaryToSecondary())
            return false;

    return true;
}

bool ViewRelation::canMapSecondaryToPrimary(bool isEfficient) const
{
    ForEachItemIn(i, columnMappings)
        if (!columnMappings.item(i).canMapSecondaryToPrimary())
            return false;

    return true;
}


void ViewRelation::queryPrimaryColumns(UnsignedArray & columns)
{
    ForEachItemIn(i, columnMappings)
    {
        ViewJoinColumnMapping & mapping = columnMappings.item(i);
        columns.append(mapping.primary->queryBaseColumn());
    }
}

void ViewRelation::querySecondaryColumns(UnsignedArray & columns)
{
    ForEachItemIn(i, columnMappings)
    {
        ViewJoinColumnMapping & mapping = columnMappings.item(i);
        if (mapping.secondaryIteratorColumn != NotFound)
            columns.append(mapping.secondaryIteratorColumn);
        else
            columns.append(mapping.secondary->queryBaseColumn());
    }
}

//---------------------------------------------------------------------------

unsigned BrowseCreateInfo::getNestingCount(const ViewFile * search)
{
    unsigned count = 0;
    ForEachItemIn(i, nestedFiles)
    {
        if (&nestedFiles.item(i) == search)
            count++;
    }
    return count;
}

//---------------------------------------------------------------------------

ViewFileWeb::ViewFileWeb(IResultSetFactory & resultSetFactory, const char * cluster, IUserDescriptor *user)
: viewOptions(resultSetFactory, cluster), directory(queryDistributedFileDirectory()), udesc(user)
{
    nextId = 0;
}


ViewFile * ViewFileWeb::addFile(IDistributedFile * definition)
{
    ViewFile * file;
    if (definition->querySuperFile())
        file = new ViewSuperFile(++nextId, viewOptions, definition);
    else
        file = new ViewFile(++nextId, viewOptions, definition);
    files.append(*file);
    return file;
}


ViewRelation * ViewFileWeb::addRelation(IFileRelationship * definition, ViewFile * primaryFile, ViewFile * secondaryFile)
{
    ForEachItemIn(i, relations)
    {
        ViewRelation & cur = relations.item(i);
        if (cur.matches(definition, primaryFile, secondaryFile))
            return &cur;
    }

    Owned<ViewRelation> relation = new ViewRelation(++nextId, definition, primaryFile, secondaryFile);
    relation->init();
    relations.append(*LINK(relation));
    primaryFile->addRelation(*relation);
    secondaryFile->addRelation(*relation);
    return relation;
}


IFileTreeBrowser * ViewFileWeb::createBrowseTree(const char * browseRootFilename, bool isEfficient, unsigned maxRecursion)
{
    ViewFile * root = findFile(browseRootFilename);
    if (!root)
        throwError1(FVERR_CouldNotResolveX, browseRootFilename);

    CFileTreeBrowser * browser = new CFileTreeBrowser(this);
    BrowseCreateInfo info(browser, isEfficient, maxRecursion);
    gatherBrowseFiles(info, root, NULL, NULL);
    return browser;
}

ViewFile * ViewFileWeb::findFile(const char * filename)
{
    ForEachItemIn(i, files)
    {
        ViewFile & cur = files.item(i);
        if (cur.matches(filename))
            return &cur;
    }
    return NULL;
}

void ViewFileWeb::gatherBrowseFiles(BrowseCreateInfo & info, ViewFile * file, CRelatedBrowseFile * parentFile, ViewRelation * parentRelation)
{
    if (info.getNestingCount(file) >= info.maxRecursion)
        return;

    CRelatedBrowseFile * browseFile = info.browser->addFile(file, parentFile, parentRelation);
    info.nestedFiles.append(*file);
    ForEachItemIn(i2, file->relations)
    {
        ViewRelation & cur = file->relations.item(i2);
        if (&cur != parentRelation)
        {
            if (stricmp(cur.queryDefinition()->queryKind(), S_LINK_RELATIONSHIP_KIND) != 0)
                continue;

            //Check for secondary first - so that recursive definitions default to secondary->primary
            if (file == cur.querySecondary())
            {
                if (cur.canMapSecondaryToPrimary(info.isEfficient))
                    gatherBrowseFiles(info, cur.queryPrimary(), browseFile, &cur);
            }
            else if (file == cur.queryPrimary())
            {
                if (cur.canMapPrimaryToSecondary(info.isEfficient))
                    gatherBrowseFiles(info, cur.querySecondary(), browseFile, &cur);
            }
        }
    }
    info.nestedFiles.pop();
}


void ViewFileWeb::gatherWeb(const char * rootFilename, const ViewGatherOptions & options)
{
    gatherWeb(rootFilename, NULL, options);
}


void ViewFileWeb::gatherWeb(const char * rootFilename, IDistributedFile * alreadyResolved, const ViewGatherOptions & options)
{
    ViewWalkOptions localOptions(options);
    if (!localOptions.kind)
        localOptions.kind = S_LINK_RELATIONSHIP_KIND;
    localOptions.isExplicitFile = true;

    if (!walkFile(rootFilename, alreadyResolved, localOptions))
        throwError1(FVERR_CouldNotResolveX, rootFilename);

    //MORE: Should possibly percolate relations between superfiles down to files they contain.
}


void ViewFileWeb::gatherWebFromPattern(const char * filenamePattern, const ViewGatherOptions & options)
{
    ViewWalkOptions localOptions(options);
    if (!localOptions.kind)
        localOptions.kind = S_LINK_RELATIONSHIP_KIND;

    Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator(filenamePattern, false, udesc);
    if (iter->first())
    {
        do
        {
            IDistributedFile & cur = iter->query();
            localOptions.isExplicitFile = true;
            walkFile(cur.queryLogicalName(), &cur, localOptions);
        } while (iter->next());
    }
    else
        throwError1(FVERR_CouldNotResolveX, filenamePattern);

    //MORE: Should possibly percolate relations between superfiles down to files they contain.
}


IViewRelatedFileIterator * ViewFileWeb::getFileIterator()
{
    return new CViewRelatedFileIterator(files, 0, this);
}


IViewRelatedFile * ViewFileWeb::queryFile(unsigned i)
{
    if (files.isItem(i))
        return &files.item(i);
    return NULL;
}

void ViewFileWeb::walk(IViewFileWebVisitor & visitor)
{
    ForEachItemIn(i1, files)
        visitor.visit(&files.item(i1));

    ForEachItemIn(i2, relations)
        visitor.visit(&relations.item(i2));
}


ViewFile * ViewFileWeb::walkFile(const char * filename, IDistributedFile * alreadyResolved, ViewWalkOptions & options)
{
    ViewFile * thisFile = findFile(filename);
    if (thisFile)
        return thisFile;

    if (options.explicitFilesOnly)
    {
        if (!options.isExplicitFile)
            return NULL;
        options.isExplicitFile = false;
    }

    Owned<IDistributedFile> resolved = alreadyResolved ? LINK(alreadyResolved) : directory.lookup(filename,udesc,false,false,true); // lock super-owners
    if (!resolved)
        return NULL;

    thisFile = addFile(resolved);
    if (options.maximumDepth > 0)
    {
        options.maximumDepth--;

        if (options.primaryDepth > 0)
        {
            options.primaryDepth--;
            Owned<IFileRelationshipIterator> iter = directory.lookupFileRelationships(NULL, filename, NULL, NULL, options.kind, NULL, options.payload);
            ForEach(*iter)
            {
                IFileRelationship & cur = iter->query();
                ViewFile * otherFile = walkFile(cur.queryPrimaryFilename(), NULL, options);
                if (otherFile)
                {
                    addRelation(&cur, otherFile, thisFile);
                }
            }
            options.primaryDepth++;
        }

        if (options.secondaryDepth > 0)
        {
            options.secondaryDepth--;
            Owned<IFileRelationshipIterator> iter = directory.lookupFileRelationships(filename, NULL, NULL, NULL, options.kind, NULL, options.payload);
            ForEach(*iter)
            {
                IFileRelationship & cur = iter->query();
                ViewFile * otherFile = walkFile(cur.querySecondaryFilename(), NULL, options);
                if (otherFile)
                {
                    addRelation(&cur, thisFile, otherFile);
                }
            }
            options.primaryDepth++;
        }

        if ((options.superDepth > 0))
        {
            options.superDepth--;
            Owned<IDistributedSuperFileIterator> iter = resolved->getOwningSuperFiles();
            ForEach(*iter)
            {
                IDistributedSuperFile & cur = iter->query();
                //ViewFile * otherFile = walkFile(cur.queryLogicalName(), &cur, options):       // could pass in to save lookup
                ViewFile * otherFile = walkFile(cur.queryLogicalName(), &cur, options);
                if (otherFile)
                {
                    thisFile->addSuperFile(*otherFile);
                    otherFile->addSubFile(*thisFile);
                }
            }
            options.superDepth++;
        }

        IDistributedSuperFile * super = resolved->querySuperFile();
        if ((options.subDepth > 0) && super)
        {
            options.subDepth--;
            Owned<IDistributedFileIterator> iter = super->getSubFileIterator(false);
            ForEach(*iter)
            {
                IDistributedFile & cur = iter->query();
                ViewFile * otherFile = walkFile(cur.queryLogicalName(), &cur, options);
                if (otherFile)
                {
                    thisFile->addSubFile(*otherFile);
                    otherFile->addSuperFile(*thisFile);
                }
            }
            options.subDepth++;
        }
        options.maximumDepth--;
    }
    return thisFile;
}

//---------------------------------------------------------------------------

FilteredSecondaryResultSetCursor::FilteredSecondaryResultSetCursor(INewResultSet * _resultSet, IResultSetCursor * _primary, ViewRelation * _relation)
: DelayedFilteredResultSetCursor(_resultSet)
{
    primary = _primary;
    relation = _relation;
}

void FilteredSecondaryResultSetCursor::noteRelatedFileChanged()
{
    DelayedFilteredResultSetCursor::noteRelatedFileChanged();
    clearFilters();
}

void FilteredSecondaryResultSetCursor::ensureFiltered()
{
    relation->addFilterToSecondary(filtered, primary);
}


//---------------------------------------------------------------------------

FilteredPrimaryResultSetCursor::FilteredPrimaryResultSetCursor(INewResultSet * _resultSet, IResultSetCursor * _secondary, ViewRelation * _relation)
: DelayedFilteredResultSetCursor(_resultSet)
{
    secondary = _secondary;
    relation = _relation;
}

void FilteredPrimaryResultSetCursor::noteRelatedFileChanged()
{
    DelayedFilteredResultSetCursor::noteRelatedFileChanged();

    clearFilters();
}

void FilteredPrimaryResultSetCursor::ensureFiltered()
{
    relation->addFilterToPrimary(filtered, secondary);
}



//---------------------------------------------------------------------------

CRelatedBrowseFile::CRelatedBrowseFile(ViewFile * _file, CRelatedBrowseFile * _parent, ViewRelation * _relation)
{
    file = _file;
    parent = _parent;
    relation = _relation;

    INewResultSet * resultSet = file->queryResultSet();
    if (!resultSet)
        throwError1(FVERR_CannotBrowseFile, file->queryFilename());

    if (!parent)
    {
        filtered.setown(new DelayedFilteredResultSetCursor(resultSet));
    }
    else
    {
        if (file == relation->querySecondary())
        {
            filtered.setown(new FilteredSecondaryResultSetCursor(resultSet, parent->queryCursor(), relation));
        }
        else
        {
            filtered.setown(new FilteredPrimaryResultSetCursor(resultSet, parent->queryCursor(), relation));
        }
    }
    notifier.setown(new NotifyingResultSetCursor(filtered));
}

void CRelatedBrowseFile::addChild(CRelatedBrowseFile * child)
{
    children.append(*child);
    notifier->addDependent(*child->notifier);
}


IViewRelatedFile * CRelatedBrowseFile::queryDefinition() const
{
    return file;
}

IRelatedBrowseFile * CRelatedBrowseFile::queryParent() const
{
    return parent;
}

IViewRelation * CRelatedBrowseFile::queryParentRelation() const
{
    return relation;
}

IRelatedBrowseFile * CRelatedBrowseFile::queryChild(unsigned i) const
{
    if (children.isItem(i))
        return &children.item(i);
    return NULL;
}

IResultSetCursor * CRelatedBrowseFile::queryCursor()
{
    return notifier;
}

//---------------------------------------------------------------------------

CFileTreeBrowser::CFileTreeBrowser(ViewFileWeb * _web) : web(_web)
{
}

CRelatedBrowseFile * CFileTreeBrowser::addFile(ViewFile * file, CRelatedBrowseFile * parent, ViewRelation * relation)
{
    CRelatedBrowseFile * next = new CRelatedBrowseFile(file, parent, relation);
    files.append(*next);
    if (parent)
        parent->addChild(next);

    return next;
}

IRelatedBrowseFile * CFileTreeBrowser::queryRootFile()
{
    return &files.item(0);
}

IResultSetFilter * CFileTreeBrowser::queryRootFilter()
{
    return &files.item(0);
}

//---------------------------------------------------------------------------

void ViewERdiagramVisitor::visit(ViewFile * file)
{
    activeFileId.clear().append(file->queryId());
    activeFieldCount = 0;

    IDistributedFile * definition = file->queryDefinition();
    builder.beginFile(activeFileId, file->queryFilename(), definition);

    activeFieldPrefix.clear().append(activeFileId).append("_");

    INewResultSet * resultSet = file->queryResultSet();
    if (resultSet)
    {
        const IResultSetMetaData & imeta = resultSet->getMetaData();
        //Cheat and use an existing internal function
        const CResultSetMetaData & meta = static_cast<const CResultSetMetaData &>(imeta);
        meta.getXmlSchema(*this, false);
    }
    else
    {
        StringBuffer dummyFieldId;
        dummyFieldId.append(activeFieldPrefix).append(0);
        builder.noteField(dummyFieldId, "**field-information-unavailable**", "");
    }

    builder.endFile();
}

void ViewERdiagramVisitor::visit(ViewRelation * relation)
{
    UnsignedArray primaryColumns;
    UnsignedArray secondaryColumns;
    relation->queryPrimaryColumns(primaryColumns);
    relation->querySecondaryColumns(secondaryColumns);

    unsigned sourceFieldId;
    unsigned targetFieldId;
    if (primaryColumns.ordinality())
    {
        sourceFieldId = primaryColumns.item(0);
        targetFieldId = secondaryColumns.item(0);
    }
    else
    {
        sourceFieldId = relation->queryPrimary()->queryFirstFieldId();
        targetFieldId = relation->querySecondary()->queryFirstFieldId();
    }

    StringBuffer id, sourceId, targetId;
    id.append(relation->queryId());
    sourceId.append(relation->queryPrimary()->queryId()).append("_").append(sourceFieldId);
    targetId.append(relation->querySecondary()->queryId()).append("_").append(targetFieldId);
    builder.noteRelation(id, sourceId, targetId, relation->queryDefinition());
}


void ViewERdiagramVisitor::addField(const char * name, ITypeInfo & type)
{
    StringBuffer eclTypeName;
    noteNextField();
    type.getECLType(eclTypeName);
    if (name && (stricmp(name, "__fileposition__") == 0))
        builder.noteField(activeFieldId.str(), "{virtual(fileposition)}", eclTypeName);
    else
        builder.noteField(activeFieldId.str(), name, eclTypeName);
}

void ViewERdiagramVisitor::addSetField(const char * name, const char * itemname, ITypeInfo & type)
{
    addField(name, type);
}

void ViewERdiagramVisitor::beginIfBlock()
{
    activeFieldCount++;
}

bool ViewERdiagramVisitor::beginDataset(const char * name, const char * childname, bool mixed, unsigned *updateMixed)
{
    noteNextField();
    builder.noteField(activeFieldId.str(), name, "dataset");
    savedFieldCounts.append(activeFieldCount);
    savedFieldCounts.append(activeFieldPrefix.length());
    activeFieldPrefix.append(activeFieldCount).append("_");
    activeFieldCount = 0;
    return false;       // don't expand contents
}

void ViewERdiagramVisitor::beginRecord(const char * name, bool mixedContent, unsigned *updateMixed)
{
    noteNextField();
    builder.noteField(activeFieldId.str(), name, "record");
    builder.beginCompound();
}

void ViewERdiagramVisitor::endIfBlock()
{
    activeFieldCount++;
}

void ViewERdiagramVisitor::endDataset(const char * name, const char * childname)
{
    activeFieldPrefix.setLength(savedFieldCounts.pop());
    activeFieldCount = savedFieldCounts.pop();
}

void ViewERdiagramVisitor::endRecord(const char * name)
{
    builder.endCompound();
    activeFieldCount++;
}

void ViewERdiagramVisitor::noteNextField()
{
    activeFieldId.clear().append(activeFieldPrefix).append(activeFieldCount++);
}

//---------------------------------------------------------------------------

ViewXgmmlERdiagramVisitor & ViewXgmmlERdiagramVisitor::addAttr(const char * name, const char * value)
{
    xgmml.append(" ").append(name).append("=\"");
    encodeXML(value, xgmml, ENCODE_NEWLINES, (unsigned)-1, false);
    xgmml.append("\"");
    return *this;
}

ViewXgmmlERdiagramVisitor & ViewXgmmlERdiagramVisitor::addAttrTag(const char * name, const char * value)
{
    if (value && *value)
    {
        xgmml.append("<attr");
        addAttr("name", name).addAttr("value", value);
        xgmml.append("/>").newline();
    }
    return *this;
}

void ViewXgmmlERdiagramVisitor::beginFile(const char * id, const char * name, IDistributedFile * definition)
{
    xgmml.append(" <node");
    addAttr("id", id).addAttr("label", name);
    xgmml.append("><att>").newline();
    xgmml.append(" <graph>").newline();
}

void ViewXgmmlERdiagramVisitor::noteField(const char * id, const char * name, const char * type)
{
    xgmml.append(" <node id=\"").append(id).append("\" label=\"").append(name).append("\">").newline();
    addAttrTag("type", type);
    xgmml.append(" </node>").newline();
}

void ViewXgmmlERdiagramVisitor::endFile()
{
    xgmml.append(" </graph>").newline();
    xgmml.append(" </att></node>").newline();
}

void ViewXgmmlERdiagramVisitor::noteRelation(const char * id, const char * primaryId, const char * secondaryId, IFileRelationship * definition)
{
    StringBuffer temp;

    //Note we want the direction from the secondary to the primary, so need to reverse some things
    xgmml.append("  <edge");
    addAttr("id", id).addAttr("source", secondaryId).addAttr("target", primaryId);

    const char * kind = definition->queryKind();
    if (kind && (stricmp(kind, S_VIEW_RELATIONSHIP_KIND) == 0))
        addAttr("label", "view of");
    xgmml.append(">").newline();

    getInvertedCardinality(temp.clear(), definition->queryCardinality());
    addAttrTag("cardinality", temp.str());
    xgmml.append("  </edge>").newline();
}


void ViewXgmmlERdiagramVisitor::beginDiagram()
{
    xgmml.append("<xgmml>").newline();
    xgmml.append("<graph>").newline();
}

void ViewXgmmlERdiagramVisitor::endDiagram()
{
    xgmml.append("</graph>").newline();
    xgmml.append("</xgmml>").newline();
}


void ViewXgmmlERdiagramVisitor::beginCompound()
{
    depth++;
}

void ViewXgmmlERdiagramVisitor::endCompound()
{
    depth--;
}


IViewFileWeb * createViewFileWeb(IResultSetFactory & resultSetFactory, const char * cluster, IUserDescriptor *user)
{
    return new ViewFileWeb(resultSetFactory, cluster, user);
}

void createERdiagram(StringBuffer & xgmml, IViewFileWeb & _web)
{
    ViewFileWeb & web = static_cast<ViewFileWeb &>(_web);

    ViewXgmmlERdiagramVisitor builder(xgmml);
    ViewERdiagramVisitor visitor(builder);
    builder.beginDiagram();
    web.walk(visitor);
    builder.endDiagram();
}

