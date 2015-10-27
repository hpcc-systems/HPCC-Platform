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

#include "layouttrans.ipp"

//MORE: handle ifblocks
//MORE: handle non-trivial field translations (for expandable types)

char const * const scopeSeparator = ".";

static IAtom * internalFposAtom;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    internalFposAtom = createAtom("__internal_fpos__");
    return true;
}


RLTFailure * RLTFailure::appendScopeDesc(char const * scope)
{
    if(scope)
        detail.append(" in ").append(scope);
    return this;
}

RLTFailure * RLTFailure::appendFieldName(char const * scope, IDefRecordElement const * field)
{
    if(scope)
        detail.append(scope).append(scopeSeparator);
    detail.append(str(field->queryName()));
    return this;
}

RLTFailure * makeFailure(IRecordLayoutTranslator::Failure::Code code)
{
    return new RLTFailure(code);
}

FieldSearcher::FieldSearcher(IDefRecordElement const * elem)
{
    unsigned num = elem->numChildren();
    tab.reinit(num + num/2);
    for(unsigned i=0; i<num; ++i)
        tab.setValue(elem->queryChild(i)->queryName(), i);
}

bool FieldSearcher::search(IAtom * search, unsigned & pos) const
{
    unsigned * ret = tab.getValue(search);
    if(ret)
    {
        pos = *ret;
        return true;
    }
    return false;
}

MappingLevel::MappingLevel(FieldMapping::List & _mappings) : topLevel(true), mappings(_mappings)
{
}

MappingLevel::MappingLevel(MappingLevel * parent, char const * name, FieldMapping::List & _mappings) : topLevel(false), mappings(_mappings)
{
    StringAttrBuilder fullScope(scope);
    if(!parent->topLevel)
        fullScope.append(parent->scope).append(scopeSeparator);
    fullScope.append(name);
}

void MappingLevel::calculateMappings(IDefRecordElement const * diskRecord, unsigned numKeyedDisk, IDefRecordElement const * activityRecord, unsigned numKeyedActivity)
{
    if(diskRecord->getKind() != DEKrecord)
        throw makeFailure(IRecordLayoutTranslator::Failure::BadStructure)->append("Disk record metadata had unexpected structure (expected record)")->appendScopeDesc(topLevel ? NULL : scope.str());
    if(activityRecord->getKind() != DEKrecord)
        throw makeFailure(IRecordLayoutTranslator::Failure::BadStructure)->append("Activity record metadata had unexpected structure (expected record)")->appendScopeDesc(topLevel ? NULL : scope.str());

    unsigned numActivityChildren = activityRecord->numChildren();
    unsigned numDiskChildren = diskRecord->numChildren();
    bool activityHasInternalFpos = false;
    if(topLevel && (numActivityChildren > numKeyedActivity))
    {
        IDefRecordElement const * lastChild = activityRecord->queryChild(numActivityChildren-1);
        if((lastChild->queryName() == internalFposAtom) && (lastChild->queryType()->isInteger()))
            activityHasInternalFpos = true;
    }
    if((numActivityChildren - (activityHasInternalFpos ? 1 : 0)) > numDiskChildren) //if last activity field might be unmatched __internal_fpos__, should be more lenient by 1 as would fill that in (see below)
        throw makeFailure(IRecordLayoutTranslator::Failure::MissingDiskField)->append("Activity record requires more fields than index provides")->appendScopeDesc(topLevel ? NULL : scope.str());
    if(numKeyedActivity > numKeyedDisk)
        throw makeFailure(IRecordLayoutTranslator::Failure::UnkeyedDiskField)->append("Activity record requires more keyed fields than index provides")->appendScopeDesc(topLevel ? NULL : scope.str());

    BoolArray activityFieldMapped;
    activityFieldMapped.ensure(numActivityChildren);
    for(unsigned i=0; i<numActivityChildren; ++i)
        activityFieldMapped.append(false);

    FieldSearcher searcher(activityRecord);
    for(unsigned diskFieldNum = 0; diskFieldNum < numDiskChildren; ++diskFieldNum)
    {
        checkField(diskRecord, diskFieldNum, "Disk");
        bool diskFieldKeyed = (diskFieldNum < numKeyedDisk);
        unsigned activityFieldNum;
        if(searcher.search(diskRecord->queryChild(diskFieldNum)->queryName(), activityFieldNum))
        {
            bool activityFieldKeyed = (activityFieldNum < numKeyedActivity);
            if(activityFieldKeyed && !diskFieldKeyed)
                throw makeFailure(IRecordLayoutTranslator::Failure::UnkeyedDiskField)->append("Field ")->appendFieldName(topLevel ? NULL : scope.str(), activityRecord->queryChild(activityFieldNum))->append(" is keyed in activity but not on disk");
            checkField(activityRecord, activityFieldNum, "Activity");
            attemptMapping(diskRecord, diskFieldNum, diskFieldKeyed, activityRecord, activityFieldNum, activityFieldKeyed);
            activityFieldMapped.replace(true, activityFieldNum);
        }
        else
        {
            mappings.append(*new FieldMapping(FieldMapping::None, diskRecord, diskFieldNum, diskFieldKeyed, NULL, 0, false));
        }
    }

    for(unsigned activityFieldNum=0; activityFieldNum<numActivityChildren; ++activityFieldNum)
        if(!activityFieldMapped.item(activityFieldNum))
        {
            checkField(activityRecord, activityFieldNum, "Activity");
            if((activityFieldNum != numActivityChildren-1) || !activityHasInternalFpos) //if last activity field is unmatched __internal_fpos__, this is not an error, we need do nothing and it will get correctly set to zero
                throw makeFailure(IRecordLayoutTranslator::Failure::MissingDiskField)->append("Field ")->appendFieldName(topLevel ? NULL : scope.str(), activityRecord->queryChild(activityFieldNum))->append(" is required by activity but not present on disk index");
        }
}

void MappingLevel::attemptMapping(IDefRecordElement const * diskRecord, unsigned diskFieldNum, bool diskFieldKeyed, IDefRecordElement const * activityRecord, unsigned activityFieldNum, bool activityFieldKeyed)
{
    IDefRecordElement const * diskField = diskRecord->queryChild(diskFieldNum);
    IDefRecordElement const * activityField = activityRecord->queryChild(activityFieldNum);
    IDefRecordElement const * diskChild = NULL;
    IDefRecordElement const * diskBlob = NULL;
    queryCheckFieldChild(diskField, "Disk", diskChild, diskBlob);
    IDefRecordElement const * activityChild = NULL;
    IDefRecordElement const * activityBlob = NULL;
    queryCheckFieldChild(activityField, "Activity", activityChild, activityBlob);

    if(diskBlob)
    {
        if(!activityBlob)
            throw makeFailure(IRecordLayoutTranslator::Failure::UntranslatableField)->append("Field ")->appendFieldName(topLevel ? NULL : scope.str(), activityRecord->queryChild(activityFieldNum))->append(" is blob on disk but not in activity");
        if(!isSameBasicType(diskBlob->queryType(), activityBlob->queryType()))
            throw makeFailure(IRecordLayoutTranslator::Failure::UntranslatableField)->append("Blob field ")->appendFieldName(topLevel ? NULL : scope.str(), activityRecord->queryChild(activityFieldNum))->append(" has differing referenced types on disk and in activity");
    }
    else
    {
        if(activityBlob)
            throw makeFailure(IRecordLayoutTranslator::Failure::UntranslatableField)->append("Field ")->appendFieldName(topLevel ? NULL : scope.str(), activityRecord->queryChild(activityFieldNum))->append(" is blob in activity but not on disk");
    }

    if(diskChild)
    {
        if(!activityChild)
            throw makeFailure(IRecordLayoutTranslator::Failure::UntranslatableField)->append("Field ")->appendFieldName(topLevel ? NULL : scope.str(), activityRecord->queryChild(activityFieldNum))->append(" is child dataset on disk but not in activity");
        if(activityFieldKeyed)
            throw makeFailure(IRecordLayoutTranslator::Failure::BadStructure)->append("Activity record metadata had unexpected structure (keyed field ")->appendFieldName(topLevel ? NULL : scope.str(), activityRecord->queryChild(activityFieldNum))->append(" is child dataset)");
        if(*activityChild == *diskChild)
        {
            mappings.append(*new FieldMapping(FieldMapping::Simple, diskRecord, diskFieldNum, false, activityRecord, activityFieldNum, false));
        }
        else
        {
            Owned<FieldMapping> mapping(new FieldMapping(FieldMapping::ChildDataset, diskRecord, diskFieldNum, false, activityRecord, activityFieldNum, false));
            MappingLevel childMappingLevel(this, str(diskField->queryName()), mapping->queryChildMappings());
            childMappingLevel.calculateMappings(diskChild, 0, activityChild, 0);
            mappings.append(*mapping.getClear());
        }
    }
    else
    {
        if(activityChild)
            throw makeFailure(IRecordLayoutTranslator::Failure::UntranslatableField)->append("Field ")->appendFieldName(topLevel ? NULL : scope.str(), activityRecord->queryChild(activityFieldNum))->append(" is child dataset in activity but not on disk");
        if(!isSameBasicType(diskField->queryType(), activityField->queryType()))
            throw makeFailure(IRecordLayoutTranslator::Failure::UntranslatableField)->append("Field ")->appendFieldName(topLevel ? NULL : scope.str(), activityRecord->queryChild(activityFieldNum))->append(" has differing types on disk and in activity");
        mappings.append(*new FieldMapping(FieldMapping::Simple, diskRecord, diskFieldNum, diskFieldKeyed, activityRecord, activityFieldNum, activityFieldKeyed));
    }
}

void MappingLevel::checkField(IDefRecordElement const * record, unsigned num, char const * label)
{
    switch(record->queryChild(num)->getKind())
    {
    case DEKfield:
        break;
    case DEKifblock:
        throw makeFailure(IRecordLayoutTranslator::Failure::UntranslatableField)->append(label)->append(" record metadata field #")->append(num)->append(" is an ifblock which is not currently translatable");
    case DEKnone:
    case DEKrecord:
    case DEKattr:
    default:
        throw makeFailure(IRecordLayoutTranslator::Failure::BadStructure)->append(label)->append(" record metadata had unexpected structure (child #")->append(num)->append("was neither field nor ifblock)")->appendScopeDesc(topLevel ? NULL : scope.str());
    }
}

void MappingLevel::queryCheckFieldChild(IDefRecordElement const * field, char const * label, IDefRecordElement const * & child, IDefRecordElement const * & blob)
{
    if(field->getKind() != DEKfield)
        throw makeFailure(IRecordLayoutTranslator::Failure::BadStructure)->append(label)->append(" record metadata had unexpected structure (non-field found where field expected");
    type_t fieldType = field->queryType()->getTypeCode();
    switch(fieldType)
    {
    case type_table:
    case type_groupedtable:
        if(field->numChildren() != 1)
            throw makeFailure(IRecordLayoutTranslator::Failure::BadStructure)->append(label)->append(" record metadata had unexpected structure (expected exactly one child of table field ")->appendFieldName(topLevel ? NULL : scope.str(), field)->append(")");
        child = field->queryChild(0);
        if(child->getKind() != DEKrecord)
            throw makeFailure(IRecordLayoutTranslator::Failure::BadStructure)->append(label)->append(" record metadata had unexpected structure (unexpected non-record child of table field ")->appendFieldName(topLevel ? NULL : scope.str(), field)->append(")");
        break;

    case type_blob:
        if(field->numChildren() != 1)
            throw makeFailure(IRecordLayoutTranslator::Failure::BadStructure)->append(label)->append(" record metadata had unexpected structure (expected exactly one child of blob field ")->appendFieldName(topLevel ? NULL : scope.str(), field)->append(")");
        blob = field->queryChild(0);
        if(blob->getKind() != DEKfield)
            throw makeFailure(IRecordLayoutTranslator::Failure::BadStructure)->append(label)->append(" record metadata had unexpected structure (expected non-field child of blob field ")->appendFieldName(topLevel ? NULL : scope.str(), field)->append(")");
        break;

    default:
        if(field->numChildren() != 0)
            throw makeFailure(IRecordLayoutTranslator::Failure::BadStructure)->append(label)->append(" record metadata had unexpected structure (unexpected children of field ")->appendFieldName(topLevel ? NULL : scope.str(), field)->append(")");
    }
}

void RowTransformer::build(unsigned & seq, FieldMapping::List const & mappings)
{
    CIArrayOf<RowRecord> records;
    analyseMappings(mappings, records);
    keepFpos = false;
    copyToFpos = false;
    generateCopies(seq, records);
}

RowTransformer::RowRecord & RowTransformer::ensureItem(CIArrayOf<RowRecord> & arr, unsigned pos)
{
    while(arr.ordinality() <= pos) arr.append(*new RowRecord);
    return arr.item(pos);
}

void RowTransformer::createRowRecord(FieldMapping const & mapping, CIArrayOf<RowRecord> & records, size32_t diskOffset, unsigned numVarFields, bool & prevActivityField, unsigned & prevActivityFieldNum)
{
    size32_t diskSize = mapping.queryDiskFieldSize();
    unsigned activityFieldNum = mapping.queryActivityFieldNum();
    switch(mapping.queryType())
    {
    case FieldMapping::Simple:
        if(mapping.isDiskFieldFpos())
            if(mapping.isActivityFieldFpos())
            {
                ensureItem(records, activityFieldNum).setVals(0, 0, diskSize, false).setFpos(true, true);
                prevActivityField = false;
            }
            else
            {
                ensureItem(records, activityFieldNum).setVals(0, 0, diskSize, false).setFpos(false, true);
                prevActivityField = false;
            }
        else
            if(mapping.isActivityFieldFpos())
            {
                ensureItem(records, activityFieldNum).setVals(diskOffset, numVarFields, diskSize, false).setFpos(true, false);
                prevActivityField = false;
            }
            else
            {
                ensureItem(records, activityFieldNum).setVals(diskOffset, numVarFields, diskSize, (prevActivityField && (activityFieldNum == (prevActivityFieldNum+1))));
                prevActivityField = true;
                prevActivityFieldNum = activityFieldNum;
            }
        break;

    case FieldMapping::ChildDataset:
        ensureItem(records, activityFieldNum).setVals(diskOffset, numVarFields, diskSize, false).setChildMappings(&mapping.queryChildMappings());
        prevActivityField = false;
        break;

    case FieldMapping::None:
        prevActivityField = false;
        break;

    default:
        throwUnexpected();
    }
}

void RowTransformer::analyseMappings(FieldMapping::List const & mappings, CIArrayOf<RowRecord> & records)
{
    size32_t diskOffset = 0;
    unsigned numRowDiskFields = mappings.ordinality();
    bool prevActivityField = false;
    unsigned prevActivityFieldNum;
    for(unsigned diskFieldNum = 0; diskFieldNum < numRowDiskFields; ++diskFieldNum)
    {
        FieldMapping const & mapping = mappings.item(diskFieldNum);
        createRowRecord(mapping, records, diskOffset, diskVarFieldRelOffsets.ordinality(), prevActivityField, prevActivityFieldNum);
        size32_t diskSize = mapping.queryDiskFieldSize();
        if(diskSize == UNKNOWN_LENGTH)
        {
            diskVarFieldRelOffsets.append(diskOffset);
            diskVarFieldLenDisplacements.append(mapping.isDiskFieldSet() ? 1 : 0);
            diskOffset = 0;
        }
        else
        {
            diskOffset += diskSize;
        }
    }

    finalFixedSize = diskOffset;
}

void RowTransformer::generateSimpleCopy(unsigned & seq, RowRecord const & record)
{
    if(!record.queryFollowOn())
        copies.append(*new FieldCopy(sequence, record.queryRelOffset(), record.queryRelBase()));
    FieldCopy & copy = copies.tos();
    size32_t size = record.querySize();
    if(size == UNKNOWN_LENGTH)
        copy.addVarField(record.queryRelBase()+1);
    else
        copy.addFixedSize(size);
    FieldMapping::List const * childMappings = record.queryChildMappings();
    if(childMappings)
        copy.setChildTransformer(new RowTransformer(seq, *childMappings));
}

void RowTransformer::generateCopyToFpos(RowRecord const & record)
{
    copyToFpos = true;
    copyToFposRelOffset = record.queryRelOffset();
    copyToFposRelBase = record.queryRelBase();
    copyToFposSize = record.querySize();
    assertex(copyToFposSize != UNKNOWN_LENGTH);
    assertex(copyToFposSize <= sizeof(offset_t));
}

void RowTransformer::generateCopyFromFpos(RowRecord const & record)
{
    assertex(sequence == 0);
    copies.append(*new FieldCopy(static_cast<unsigned>(-1), 0, 0));
    size32_t size = record.querySize();
    assertex(size != UNKNOWN_LENGTH);
    assertex(size <= sizeof(offset_t));
    copies.tos().addFixedSize(size);
}

void RowTransformer::generateCopies(unsigned & seq, CIArrayOf<RowRecord> const & records)
{
    sequence = seq++;
    ForEachItemIn(fieldNum, records)
    {
        RowRecord const & record = records.item(fieldNum);
        if(record.isToFpos())
        {
            assertex(sequence == 0);
            if(record.isFromFpos())
                keepFpos = true;
            else
                generateCopyToFpos(record);
        }
        else
        {
            if(record.isFromFpos())
                generateCopyFromFpos(record);
            else
                generateSimpleCopy(seq, record);
        }
    }
}

void RowTransformer::transform(IRecordLayoutTranslator::RowTransformContext * ctx, byte const * in, size32_t inSize, size32_t & inOffset, IMemoryBlock & out, size32_t & outOffset) const
{
    ctx->set(sequence, 0, 0, in+inOffset);
    for(unsigned varIdx = 1; varIdx <= diskVarFieldRelOffsets.ordinality(); ++varIdx)
    {
        if(inOffset >= inSize)
            throw MakeStringException(0, "Disk row invalid during record layout translation");
        inOffset += diskVarFieldRelOffsets.item(varIdx-1);
        size32_t disp = diskVarFieldLenDisplacements.item(varIdx-1);
        size32_t size = *reinterpret_cast<size32_t const *>(in+inOffset+disp);
        // length prefix is little-endian
#if __BYTE_ORDER == __BIG_ENDIAN
        _rev(&size);
#endif
        size += disp;
        size += sizeof(size32_t);
        inOffset += size;
        ctx->set(sequence, varIdx, size, in+inOffset);
    }
    inOffset += finalFixedSize;

    ForEachItemIn(copyIdx, copies)
        copies.item(copyIdx).copy(ctx, out, outOffset);
}

void RowTransformer::getFposOut(IRecordLayoutTranslator::RowTransformContext const * ctx, offset_t & fpos) const
{
    if(copyToFpos)
    {
        fpos = 0;
        const byte * in = ctx->queryPointer(0, copyToFposRelBase) + copyToFposRelOffset;
        // integer field in row is big-endian
#if __BYTE_ORDER == __BIG_ENDIAN
        memcpy(reinterpret_cast<byte const *>(&fpos) + sizeof(offset_t) - copyToFposSize, in, copyToFposSize);
#else
        _cpyrevn(&fpos, in, copyToFposSize);
#endif
    }
    else if(!keepFpos)
    {
        fpos = 0;
    }
}

void RowTransformer::createRowTransformContext(IRecordLayoutTranslator::RowTransformContext * ctx) const
{
    ctx->init(sequence, diskVarFieldRelOffsets.ordinality()+1);
    ForEachItemIn(idx, copies)
    {
        RowTransformer const * child = copies.item(idx).queryChildTransformer();
        if(child)
            child->createRowTransformContext(ctx);
    }
}

void FieldCopy::copy(IRecordLayoutTranslator::RowTransformContext * ctx, IMemoryBlock & out, size32_t & outOffset) const
{
    if(sequence == static_cast<unsigned>(-1))
    {
        byte * target = out.ensure(outOffset+fixedSize);
        // integer field in row is big-endian
#if __BYTE_ORDER == __BIG_ENDIAN
        memcpy(target+outOffset, reinterpret_cast<byte const *>(ctx->queryFposIn()) + sizeof(offset_t) - fixedSize, fixedSize);
#else
        _cpyrevn(target+outOffset, ctx->queryFposIn(), fixedSize);
#endif
        outOffset += fixedSize;
        return;
    }

    size32_t diskFieldSize = fixedSize;
    ForEachItemIn(varIdx, varFields)
        diskFieldSize += ctx->querySize(sequence, varFields.item(varIdx));
    byte const * in = ctx->queryPointer(sequence, relBase) + relOffset;

    if(childTransformer)
    {
        size32_t inOffset = sizeof(size32_t);
        size32_t sizeOutOffset = outOffset;
        outOffset += sizeof(size32_t);
        size32_t startOutOffset = outOffset;
        while(inOffset < diskFieldSize)
            childTransformer->transform(ctx, in, diskFieldSize, inOffset, out, outOffset);

        //Now patch the length up - transform may have resized out...
        size32_t * outSizePtr = reinterpret_cast<size32_t *>(out.getMem()+sizeOutOffset);
        *outSizePtr = outOffset-startOutOffset;
    }
    else
    {
        byte * target = out.ensure(outOffset+diskFieldSize);
        memcpy(target+outOffset, in, diskFieldSize);
        outOffset += diskFieldSize;
    }
}

IRecordLayoutTranslator::RowTransformContext::RowTransformContext(unsigned _num) : num(_num)
{
    sizes = new unsigned *[num];
    ptrs = new byte const * *[num];
    for(unsigned i=0; i<num; ++i)
    {
        sizes[i] = NULL;
        ptrs[i] = NULL;
    }
}

IRecordLayoutTranslator::RowTransformContext::~RowTransformContext()
{
    for(unsigned i=0; i<num; ++i)
    {
        if(sizes[i])
            delete [] sizes[i];
        if(ptrs[i])
            delete [] ptrs[i];
    }
    delete [] sizes;
    delete [] ptrs;
}

CRecordLayoutTranslator::CRecordLayoutTranslator(IDefRecordMeta const * _diskMeta, IDefRecordMeta const * _activityMeta) : diskMeta(const_cast<IDefRecordMeta *>(_diskMeta)), activityMeta(const_cast<IDefRecordMeta *>(_activityMeta)), activityKeySizes(NULL)
{
    numKeyedDisk = diskMeta->numKeyedFields();
    numKeyedActivity = activityMeta->numKeyedFields();
    MappingLevel topMappingLevel(mappings);
    numTransformers = 0;
    try
    {
        if(numKeyedDisk==0)
            throw makeFailure(IRecordLayoutTranslator::Failure::BadStructure)->append("Disk record had no keyed fields");
        if(numKeyedActivity==0)
            throw makeFailure(IRecordLayoutTranslator::Failure::BadStructure)->append("Activity record had no keyed fields");
        topMappingLevel.calculateMappings(diskMeta->queryRecord(), numKeyedDisk, activityMeta->queryRecord(), numKeyedActivity);
        calculateActivityKeySizes();
        calculateKeysTransformed();
        transformer.build(numTransformers, mappings);
    }
    catch(Failure * f)
    {
        failure.setown(f);
    }
}

void CRecordLayoutTranslator::calculateActivityKeySizes()
{
    activityKeySizes = new size32_t[numKeyedActivity];
    for(unsigned activityFieldNum=0; activityFieldNum<numKeyedActivity; ++activityFieldNum)
        activityKeySizes[activityFieldNum] = 0;
    ForEachItemIn(diskFieldNum, mappings)
    {
        FieldMapping const & mapping = mappings.item(diskFieldNum);
        if(mapping.queryType() == FieldMapping::Simple)
        {
            unsigned activityFieldNum = mapping.queryActivityFieldNum();
            if(activityFieldNum < numKeyedActivity)
                activityKeySizes[activityFieldNum] = activityMeta->queryRecord()->queryChild(activityFieldNum)->queryType()->getSize();
        }
    }
}

void CRecordLayoutTranslator::calculateKeysTransformed()
{
    keysTransformed = true;;
    if(numKeyedActivity != numKeyedDisk)
        return;
    for(unsigned diskFieldNum=0; diskFieldNum<numKeyedDisk; ++diskFieldNum)
    {
        FieldMapping const & mapping = mappings.item(diskFieldNum);
        if((mapping.queryType() != FieldMapping::Simple) || (mapping.queryActivityFieldNum() != diskFieldNum))
            return;
    }
    keysTransformed = false;
}

void CRecordLayoutTranslator::createDiskSegmentMonitors(SegmentMonitorContext const & in, IIndexReadContext & out)
{
    if(failure) return;
    if(in.ordinality() != numKeyedActivity)
    {
        failure.setown(makeFailure(Failure::UnsupportedFilter)->append("Unsupported filter (segment monitor) type (too few filters)"));
        return;
    }
    size32_t diskOffset = 0;
    for(unsigned diskFieldNum = 0; diskFieldNum < numKeyedDisk; ++diskFieldNum)
    {
        FieldMapping const & mapping = mappings.item(diskFieldNum);
        assertex(mapping.queryDiskFieldNum() == diskFieldNum);
        size32_t size = mapping.queryDiskFieldSize();
        Owned<IKeySegmentMonitor> monitor;
        switch(mapping.queryType())
        {
        case FieldMapping::Simple:
            if(mapping.queryActivityFieldNum() < numKeyedActivity)
            {
                assertex(mapping.queryActivityFieldSize() == size);
                monitor.set(in.item(mapping.queryActivityFieldNum()));
                assertex(monitor->getSize() == size);
                if(monitor->getOffset() != diskOffset)
                {
                    monitor.setown(monitor->clone());
                    if(!monitor || !monitor->setOffset(diskOffset))
                    {
                        failure.setown(makeFailure(Failure::UnsupportedFilter)->append("Unable to change offset of filter (segment monitor) for field ")->append(mapping.queryDiskFieldName()));
                        return;
                    }
                }
                break;
            }
            //fall through

        case FieldMapping::None:
            monitor.setown(createWildKeySegmentMonitor(diskOffset, size));
            break;

        case FieldMapping::ChildDataset:
        default:
            throwUnexpected();
        }
        out.append(monitor.getLink());
        diskOffset += size;
    }
}

void CRecordLayoutTranslator::checkSizes(char const * filename, size32_t activitySize, size32_t diskSize) const
{
}

IRecordLayoutTranslator::RowTransformContext * CRecordLayoutTranslator::getRowTransformContext()
{
    Owned<IRecordLayoutTranslator::RowTransformContext> ctx = new RowTransformContext(numTransformers);
    transformer.createRowTransformContext(ctx);
    return ctx.getClear();
}

size32_t CRecordLayoutTranslator::transformRow(RowTransformContext * ctx, byte const * in, size32_t inSize, IMemoryBlock & out, offset_t & fpos) const
{
    size32_t inOffset = 0;
    size32_t outOffset = 0;
    ctx->setFposIn(fpos);
    transformer.transform(ctx, in, inSize, inOffset, out, outOffset);
    transformer.getFposOut(ctx, fpos);
    return outOffset;
}

void ExpandedSegmentMonitorList::append(IKeySegmentMonitor * monitor)
{
    if(owner->failure) return;
    while(monitor->getSize() > owner->activityKeySizes[monitors.ordinality()])
    {
        Owned<IKeySegmentMonitor> split = monitor->split(owner->activityKeySizes[monitors.ordinality()]);
        if(!split)
        {
            owner->failure.setown(makeFailure(IRecordLayoutTranslator::Failure::UnsupportedFilter)->append("Unsupported filter (segment monitor) type (was larger than keyed field and unsplittable)"));
            return;
        }
        monitors.append(*split.getLink());
    }
    if(monitor->getSize() < owner->activityKeySizes[monitors.ordinality()])
    {
        owner->failure.setown(makeFailure(IRecordLayoutTranslator::Failure::UnsupportedFilter)->append("Unsupported filter (segment monitor) type (was smaller than keyed field)"));
        return;
    }
    monitors.append(*monitor);
}

void ExpandedSegmentMonitorList::setMergeBarrier(unsigned offset)
{
    // MORE - It's possible that I need to do something here??
}

IRecordLayoutTranslator * createRecordLayoutTranslator(IDefRecordMeta const * diskMeta, IDefRecordMeta const * activityMeta)
{
    Owned<IRecordLayoutTranslator> layoutTrans = new CRecordLayoutTranslator(diskMeta, activityMeta);
    if(!layoutTrans->querySuccess())
    {
        StringBuffer cause;
        layoutTrans->queryFailure().getDetail(cause);
        throw MakeStringException(0, "Unable to recover from record layout mismatch (%s)", cause.str());
    }
    return layoutTrans.getClear();
};

extern THORHELPER_API IRecordLayoutTranslator * createRecordLayoutTranslator(size32_t diskMetaSize, const void *diskMetaData, size32_t activityMetaSize, const void *activityMetaData)
{
    MemoryBuffer activityMetaSerialized;
    activityMetaSerialized.setBuffer(activityMetaSize, (void *) activityMetaData, false);
    Owned<IDefRecordMeta> activityMeta = deserializeRecordMeta(activityMetaSerialized, true);

    MemoryBuffer diskMetaSerialized;
    diskMetaSerialized.setBuffer(diskMetaSize, (void *) diskMetaData, false);
    Owned<IDefRecordMeta> diskMeta = deserializeRecordMeta(diskMetaSerialized, true);

    return createRecordLayoutTranslator(diskMeta, activityMeta);
}

#ifdef DEBUG_HELPERS_REQUIRED

IPropertyTree * convertFieldMappingsToPTree(FieldMapping::List const & mappings)
{
    Owned<IPropertyTree> tree = createPTree("Record");
    ForEachItemIn(mappingIdx, mappings)
    {
        FieldMapping const & m = mappings.item(mappingIdx);
        Owned<IPropertyTree> branch = createPTree();
        branch->setPropInt("@diskFieldNum", m.queryDiskFieldNum());
        branch->setProp("@diskFieldName", m.queryDiskFieldName());
        switch(m.queryType())
        {
        case FieldMapping::None:
            branch->setProp("@type", "None");
            break;
        case FieldMapping::Simple:
            branch->setProp("@type", "Simple");
            branch->setPropInt("@activityFieldNum", m.queryActivityFieldNum());
            branch->setProp("@activityFieldName", m.queryActivityFieldName());
            break;
        case FieldMapping::ChildDataset:
            branch->setProp("@type", "ChildDataset");
            branch->setPropInt("@activityFieldNum", m.queryActivityFieldNum());
            branch->setProp("@activityFieldName", m.queryActivityFieldName());
            branch->setPropTree("Record", convertFieldMappingsToPTree(m.queryChildMappings()));
            break;
        default:
            throwUnexpected();
        }
        tree->addPropTree("Mapping", branch.getClear());
    }
    return tree.getClear();
}

StringBuffer & CRecordLayoutTranslator::getMappingsAsString(StringBuffer & out) const
{
    Owned<IPropertyTree> tree = convertFieldMappingsToPTree(mappings);
    toXML(tree, out);
    return out;
}

#endif

CacheKey::CacheKey(size32_t _s1, void const * _d1, size32_t _s2, void const * _d2)
    : s1(_s1), d1(static_cast<byte const *>(_d1)), s2(_s2), d2(static_cast<byte const *>(_d2))
{
    hashval = hashc(d1, s1, 0);
    hashval = hashc(d2, s2, hashval);
}

CacheValue::CacheValue(size32_t s1, void const * d1, size32_t s2, void const * d2, IRecordLayoutTranslator * _trans)
    : b1(s1, d1), b2(s2, d2), key((size32_t)b1.length(), b1.get(), (size32_t)b2.length(), b2.get()), trans(_trans)
{
}

IRecordLayoutTranslator * CRecordLayoutTranslatorCache::get(size32_t diskMetaSize, void const  * diskMetaData, size32_t activityMetaSize, void const * activityMetaData, IDefRecordMeta const * activityMeta)
{
    CacheKey key(diskMetaSize, diskMetaData, activityMetaSize, activityMetaData);
    CacheValue * value = find(&key);
    if(!value)
    {
        Owned<IDefRecordMeta> activityMetaDeserialized;
        if(!activityMeta)
        {
            MemoryBuffer activityMetaSerialized;
            activityMetaSerialized.setBuffer(activityMetaSize, (void *) activityMetaData, false);
            activityMetaDeserialized.setown(deserializeRecordMeta(activityMetaSerialized, true));
            activityMeta = activityMetaDeserialized.get();
        }

        MemoryBuffer diskMetaSerialized;
        diskMetaSerialized.setBuffer(diskMetaSize, (void *) diskMetaData, false);
        Owned<IDefRecordMeta> diskMeta = deserializeRecordMeta(diskMetaSerialized, true);

        Owned<IRecordLayoutTranslator> trans = createRecordLayoutTranslator(diskMeta, activityMeta);

        value = new CacheValue(diskMetaSize, diskMetaData, activityMetaSize, activityMetaData, trans.getLink());
        addNew(value);
    }
    return value->getTranslator();
}

extern THORHELPER_API IRecordLayoutTranslatorCache * createRecordLayoutTranslatorCache()
{
    return new CRecordLayoutTranslatorCache();
}

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/HelperMacros.h>

//MORE: This does not test translation with blobs or child datasets. Also, it only creates translators --- testing they actually work would require a lot more framework...

class RecordLayoutTranslatorTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(RecordLayoutTranslatorTest);
    CPPUNIT_TEST(testCount);
    CPPUNIT_TEST(testKeyedSwap);
    CPPUNIT_TEST(testUnkey);
    CPPUNIT_TEST(testKeyFail);
    CPPUNIT_TEST(testSwapKeyFail);
    CPPUNIT_TEST(testCache);
    CPPUNIT_TEST(testDropKeyed);
    CPPUNIT_TEST(testDropUnkeyed);
    CPPUNIT_TEST(testNewFieldFail);
    CPPUNIT_TEST(testRenamedFieldFail);
    CPPUNIT_TEST(testChangeTypeFail);
    CPPUNIT_TEST_SUITE_END();
public:
    void setUp()
    {
        bool done = false;
        for(unsigned m=0; !done; ++m)
        {
            Owned<IDefRecordBuilder> builder = createDErecord(4096);
            Owned<ITypeInfo> type;
            IAtom * name;
            size32_t size;
            unsigned keyed;
            unsigned f;
            for(f = 0; getFieldData(m, f, type, name, size, keyed); ++f)
            {
                Owned<IDefRecordElement> field = createDEfield(name, type, NULL, size);
                builder->addChild(field);
            }
            Owned<IDefRecordElement> record = builder->close();
            if(f)
                meta.append(*createDefRecordMeta(record, keyed));
            else
                done = true;

        }
    }

    void testCount()
    {
        CPPUNIT_ASSERT(meta.ordinality() == 10);
    }

    void testKeyedSwap()
    {
        doTranslate(0, 1);
    }

    void testUnkey()
    {
        doTranslate(0, 2);
    }

    void testKeyFail()
    {
        doTranslateFail(0, 3, IRecordLayoutTranslator::Failure::UnkeyedDiskField);
    }

    void testSwapKeyFail()
    {
        doTranslateFail(0, 4, IRecordLayoutTranslator::Failure::UnkeyedDiskField);
    }

    void testDropKeyed()
    {
        doTranslate(0, 5);
    }

    void testDropUnkeyed()
    {
        doTranslate(0, 6);
    }

    void testNewFieldFail()
    {
        doTranslateFail(0, 7, IRecordLayoutTranslator::Failure::MissingDiskField);
    }

    void testRenamedFieldFail()
    {
        doTranslateFail(0, 8, IRecordLayoutTranslator::Failure::MissingDiskField);
    }

    void testChangeTypeFail()
    {
        doTranslateFail(0, 9, IRecordLayoutTranslator::Failure::UntranslatableField);
    }

    void testCache()
    {
        MemoryBuffer buff[3];
        for(unsigned m=0; m<3; ++m)
            serializeRecordMeta(buff[m], &meta.item(m), true);
        Owned<IRecordLayoutTranslatorCache> cache = createRecordLayoutTranslatorCache();
        CPPUNIT_ASSERT(cache.get() != 0);
        CPPUNIT_ASSERT(cache->count() == 0);
        Owned<IRecordLayoutTranslator> t1 = cache->get(buff[0].length(), buff[0].bufferBase(), buff[1].length(), buff[1].bufferBase(), NULL);
        CPPUNIT_ASSERT(cache->count() == 1);
        Owned<IRecordLayoutTranslator> t2 = cache->get(buff[0].length(), buff[0].bufferBase(), buff[1].length(), buff[1].bufferBase(), NULL);
        CPPUNIT_ASSERT(cache->count() == 1);
        Owned<IRecordLayoutTranslator> t3 = cache->get(buff[0].length(), buff[0].bufferBase(), buff[2].length(), buff[2].bufferBase(), NULL);
        CPPUNIT_ASSERT(cache->count() == 2);
        CPPUNIT_ASSERT(t1.get() == t2.get());
        CPPUNIT_ASSERT(t1.get() != t3.get());
    }

private:
    bool getFieldData(unsigned m, unsigned f, Owned<ITypeInfo> & type, IAtom * & name, size32_t & size, unsigned & keyed)
    {
        switch(m)
        {
        case 0:
            //disk
            keyed = 2;
            switch(f)
            {
            case 0:
                type.setown(makeIntType(1, false));
                name = createAtom("i");
                size = 1;
                return true;
            case 1:
                type.setown(makeIntType(4, false));
                name = createAtom("j");
                size = 4;
                return true;
            case 2:
                type.setown(makeStringType(8));
                name = createAtom("str");
                size = 8;
                return true;
            }
            return false;

        case 1:
            //swap i,j
            keyed = 2;
            switch(f)
            {
            case 0:
                type.setown(makeIntType(4, false));
                name = createAtom("j");
                size = 4;
                return true;
            case 1:
                type.setown(makeIntType(1, false));
                name = createAtom("i");
                size = 1;
                return true;
            case 2:
                type.setown(makeStringType(8));
                name = createAtom("str");
                size = 8;
                return true;
            }
            return false;

        case 2:
            //unkey j
            keyed = 1;
            switch(f)
            {
            case 0:
                type.setown(makeIntType(1, false));
                name = createAtom("i");
                size = 1;
                return true;
            case 1:
                type.setown(makeIntType(4, false));
                name = createAtom("j");
                size = 4;
                return true;
            case 2:
                type.setown(makeStringType(8));
                name = createAtom("str");
                size = 8;
                return true;
            }
            return false;

        case 3:
            //key str (fails)
            keyed = 3;
            switch(f)
            {
            case 0:
                type.setown(makeIntType(1, false));
                name = createAtom("i");
                size = 1;
                return true;
            case 1:
                type.setown(makeIntType(4, false));
                name = createAtom("j");
                size = 4;
                return true;
            case 2:
                type.setown(makeStringType(8));
                name = createAtom("str");
                size = 8;
                return true;
            }
            return false;

        case 4:
            //move str into key (fails)
            keyed = 2;
            switch(f)
            {
            case 0:
                type.setown(makeIntType(1, false));
                name = createAtom("i");
                size = 1;
                return true;
            case 1:
                type.setown(makeStringType(8));
                name = createAtom("str");
                size = 8;
                return true;
            case 2:
                type.setown(makeIntType(4, false));
                name = createAtom("j");
                size = 4;
                return true;
            }
            return false;

        case 5:
            //drop j
            keyed = 1;
            switch(f)
            {
            case 0:
                type.setown(makeIntType(1, false));
                name = createAtom("i");
                size = 1;
                return true;
            case 1:
                type.setown(makeStringType(8));
                name = createAtom("str");
                size = 8;
                return true;
            }
            return false;

        case 6:
            //drop str
            keyed = 2;
            switch(f)
            {
            case 0:
                type.setown(makeIntType(1, false));
                name = createAtom("i");
                size = 1;
                return true;
            case 1:
                type.setown(makeIntType(4, false));
                name = createAtom("j");
                size = 4;
                return true;
            }
            return false;

        case 7:
            //add new field
            keyed = 2;
            switch(f)
            {
            case 0:
                type.setown(makeIntType(1, false));
                name = createAtom("i");
                size = 1;
                return true;
            case 1:
                type.setown(makeIntType(4, false));
                name = createAtom("j");
                size = 4;
                return true;
            case 2:
                type.setown(makeStringType(8));
                name = createAtom("str");
                size = 8;
                return true;
            case 3:
                type.setown(makeStringType(8));
                name = createAtom("other");
                size = 8;
                return true;
            }
            return false;

        case 8:
            //rename field
            keyed = 2;
            switch(f)
            {
            case 0:
                type.setown(makeIntType(1, false));
                name = createAtom("i");
                size = 1;
                return true;
            case 1:
                type.setown(makeIntType(4, false));
                name = createAtom("j");
                size = 4;
                return true;
            case 2:
                type.setown(makeStringType(8));
                name = createAtom("other");
                size = 8;
                return true;
            }
            return false;

        case 9:
            //change type
            keyed = 2;
            switch(f)
            {
            case 0:
                type.setown(makeIntType(1, false));
                name = createAtom("i");
                size = 1;
                return true;
            case 1:
                type.setown(makeIntType(4, false));
                name = createAtom("j");
                size = 4;
                return true;
            case 2:
                type.setown(makeStringType(9));
                name = createAtom("str");
                size = 9;
                return true;
            }
            return false;

        }
        return false;
    }

    void doTranslate(unsigned disk, unsigned activity)
    {
        Owned<IRecordLayoutTranslator> trans = new CRecordLayoutTranslator(&meta.item(disk), &meta.item(activity));
        CPPUNIT_ASSERT(trans.get() != NULL);
        CPPUNIT_ASSERT(trans->querySuccess());
    }
    
    void doTranslateFail(unsigned disk, unsigned activity, unsigned code)
    {
        Owned<IRecordLayoutTranslator> trans = new CRecordLayoutTranslator(&meta.item(disk), &meta.item(activity));
        CPPUNIT_ASSERT(trans.get() != 0);
        CPPUNIT_ASSERT(!trans->querySuccess());
        CPPUNIT_ASSERT(trans->queryFailure().queryCode() == code);
    }

private:
    IArrayOf<IDefRecordMeta> meta;
    MemoryBuffer * buff;
    Owned<IRecordLayoutTranslatorCache> cache;
};

CPPUNIT_TEST_SUITE_REGISTRATION(RecordLayoutTranslatorTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(RecordLayoutTranslatorTest, "RecordLayoutTranslatorTest");

#endif
