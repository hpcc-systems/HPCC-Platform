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

#include "jliball.hpp"
#include "jhtree.hpp"
#include "ctfile.hpp"
#include "rtlrecord.hpp"
#include "rtlformat.hpp"
#include "rtldynfield.hpp"
#include "eclhelper_dyn.hpp"
#include "hqlexpr.hpp"
#include "hqlutil.hpp"

void fatal(const char *format, ...) __attribute__((format(printf, 1, 2)));
void fatal(const char *format, ...)
{
    va_list      args;

    va_start(args, format);
    vfprintf(stderr, format, args);
    va_end(args);
    fflush(stderr);
    releaseAtoms();
    ExitModuleObjects();
    _exit(2);
}

bool optHex = false;
bool optRaw = false;
bool optFullHeader = false;
bool optHeader = false;
StringArray files;

void usage()
{
    fprintf(stderr, "Usage: dumpkey [options] dataset [dataset...]\n"
        "Options:\n"
        "  node=[n]            - dump node n (0 = just header)\n"
        "  fpos=[n]            - dump node at offset fpos\n"
        "  recs=[n]            - output n rows\n"
        "  fields=[fieldnames] - output specified fields only\n"
        "  filter=[filter]     - filter rows\n"
        "  -H                  - hex display\n"
        "  -R                  - raw output\n"
        "  -fullheader         - output full header info for each file\n"
        "  -header             - output minimal header info for each file\n"
                    );
    fflush(stderr);
    releaseAtoms();
    ExitModuleObjects();
    _exit(2);
}

void doOption(const char *opt)
{
    if (streq(opt, "-H"))
        optHex = true;
    else if (streq(opt, "-R"))
        optRaw = true;
    else if (streq(opt, "-header"))
        optHeader = true;
    else if (streq(opt, "-fullheader"))
        optFullHeader = true;
    else
        usage();
}

class MyIndexVirtualFieldCallback : public CInterfaceOf<IVirtualFieldCallback>
{
public:
    MyIndexVirtualFieldCallback(IKeyManager *_manager) : manager(_manager)
    {
    }
    virtual const char * queryLogicalFilename(const void * row) override
    {
        UNIMPLEMENTED;
    }
    virtual unsigned __int64 getFilePosition(const void * row) override
    {
        UNIMPLEMENTED;
    }
    virtual unsigned __int64 getLocalFilePosition(const void * row) override
    {
        UNIMPLEMENTED;
    }
    virtual const byte * lookupBlob(unsigned __int64 id) override
    {
        size32_t blobSize;
        return manager->loadBlob(id, blobSize);
    }
private:
    Linked<IKeyManager> manager;
};

int main(int argc, const char **argv)
{
    InitModuleObjects();
#ifdef _WIN32
    _setmode( _fileno( stdout ), _O_BINARY );
    _setmode( _fileno( stdin ), _O_BINARY );
#endif
    Owned<IProperties> globals = createProperties("dumpkey.ini", true);
    StringArray filters;
    for (int i = 1; i < argc; i++)
    {
        if (argv[i][0] == '-')
            doOption(argv[i]);
        else if (strncmp(argv[i], "filter=", 7)==0)
            filters.append(argv[i]+7);
        else if (strchr(argv[i], '='))
            globals->loadProp(argv[i]);
        else
            files.append(argv[i]);
    }
    try
    {
        StringBuffer logname("dumpkey.");
        logname.append(GetCachedHostName()).append(".");
        StringBuffer lf;
        openLogFile(lf, logname.append("log").str());
    }
    catch (IException *E)
    {
        // Silently ignore failure to open logfile.
        E->Release();
    }
    ForEachItemIn(idx, files)
    {
        try
        {
            Owned <IKeyIndex> index;
            const char * keyName = files.item(idx);
            index.setown(createKeyIndex(keyName, 0, false, false));
            size32_t key_size = index->keySize();  // NOTE - in variable size case, this is 32767
            size32_t keyedSize = index->keyedSize();
            unsigned nodeSize = index->getNodeSize();
            if (optFullHeader)
            {
                Owned<IFile> in = createIFile(keyName);
                Owned<IFileIO> io = in->open(IFOread);
                if (!io)
                    throw MakeStringException(999, "Failed to open file %s", keyName);
                Owned<CKeyHdr> header = new CKeyHdr;
                MemoryAttr block(sizeof(KeyHdr));
                io->read(0, sizeof(KeyHdr), (void *)block.get());
                header->load(*(KeyHdr*)block.get());

                printf("Key '%s'\nkeySize=%d keyedSize = %d NumParts=%x, Top=%d\n", keyName, key_size, keyedSize, index->numParts(), index->isTopLevelKey());
                printf("File size = %" I64F "d, nodes = %" I64F "d\n", in->size(), in->size() / nodeSize - 1);
                printf("rootoffset=%" I64F "d[%" I64F "d]\n", header->getRootFPos(), header->getRootFPos()/nodeSize);
                Owned<IPropertyTree> metadata = index->getMetadata();
                if (metadata)
                {
                    StringBuffer xml;
                    toXML(metadata, xml);
                    printf("MetaData:\n%s\n", xml.str());
                }
            }
            else if (optHeader)
            {
                if (idx)
                    printf("\n");
                printf("%s:\n\n", keyName);
            }

            if (globals->hasProp("node"))
            {
                if (stricmp(globals->queryProp("node"), "all")==0)
                {
                }
                else
                {
                    int node = globals->getPropInt("node");
                    if (node != 0)
                        index->dumpNode(stdout, node * nodeSize, globals->getPropInt("recs", 0), optRaw);
                }
            }
            else if (globals->hasProp("fpos"))
            {
                index->dumpNode(stdout, globals->getPropInt("fpos"), globals->getPropInt("recs", 0), optRaw);
            }
            else
            {
                Owned<IKeyManager> manager;
                Owned<IPropertyTree> metadata = index->getMetadata();
                Owned<IOutputMetaData> diskmeta;
                Owned<IOutputMetaData> translatedmeta;
                ArrayOf<const RtlFieldInfo *> deleteFields;
                ArrayOf<const RtlFieldInfo *> fields;  // Note - the lifetime of the array needs to extend beyond the lifetime of outmeta. The fields themselves are shared with diskmeta, and do not need to be released.
                Owned<IOutputMetaData> outmeta;
                Owned<IXmlWriterExt> writer;
                Owned<const IDynamicTransform> translator;
                RowFilter rowFilter;
                unsigned __int64 count = globals->getPropInt("recs", 1);
                const RtlRecordTypeInfo *outRecType = nullptr;
                if (metadata && metadata->hasProp("_rtlType"))
                {
                    MemoryBuffer layoutBin;
                    metadata->getPropBin("_rtlType", layoutBin);
                    try
                    {
                        diskmeta.setown(createTypeInfoOutputMetaData(layoutBin, false));
                    }
                    catch (IException *E)
                    {
                        EXCLOG(E);
                        E->Release();
                    }
                }
                if (!diskmeta && metadata->hasProp("_record_ECL"))
                {
                    MultiErrorReceiver errs;
                    Owned<IHqlExpression> expr = parseQuery(metadata->queryProp("_record_ECL"), &errs);
                    if (errs.errCount() == 0)
                    {
                        MemoryBuffer layoutBin;
                        if (exportBinaryType(layoutBin, expr, true))
                            diskmeta.setown(createTypeInfoOutputMetaData(layoutBin, false));
                    }
                }
                if (diskmeta)
                {
                    writer.setown(new SimpleOutputWriter);
                    const RtlRecord &inrec = diskmeta->queryRecordAccessor(true);
                    manager.setown(createLocalKeyManager(inrec, index, nullptr, true, false));
                    size32_t minRecSize = 0;
                    if (globals->hasProp("fields"))
                    {
                        StringArray fieldNames;
                        fieldNames.appendList(globals->queryProp("fields"), ",");
                        ForEachItemIn(idx, fieldNames)
                        {
                            unsigned fieldNum = inrec.getFieldNum(fieldNames.item(idx));
                            if (fieldNum == (unsigned) -1)
                                throw MakeStringException(0, "Requested output field '%s' not found", fieldNames.item(idx));
                            const RtlFieldInfo *field = inrec.queryOriginalField(fieldNum);
                            if (field->type->getType() == type_blob)
                            {
                                // We can't just use the original source field in this case (as blobs are only supported in the input)
                                // So instead, create a field in the target with the original type.
                                field = new RtlFieldStrInfo(field->name, field->xpath, field->type->queryChildType());
                                deleteFields.append(field);
                            }
                            fields.append(field);
                            minRecSize += field->type->getMinSize();
                        }
                    }
                    else
                    {
                        // Copy all fields from the source record
                        unsigned numFields = inrec.getNumFields();
                        for (unsigned idx = 0; idx < numFields;idx++)
                        {
                            const RtlFieldInfo *field = inrec.queryOriginalField(idx);
                            if (field->type->getType() == type_blob)
                            {
                                // See above - blob field in source needs special treatment
                                field = new RtlFieldStrInfo(field->name, field->xpath, field->type->queryChildType());
                                deleteFields.append(field);
                            }
                            fields.append(field);
                            minRecSize += field->type->getMinSize();
                        }
                    }
                    fields.append(nullptr);
                    outRecType = new RtlRecordTypeInfo(type_record, minRecSize, fields.getArray(0));
                    outmeta.setown(new CDynamicOutputMetaData(*outRecType));
                    translator.setown(createRecordTranslator(outmeta->queryRecordAccessor(true), inrec));
                    if (filters.ordinality())
                    {
                        ForEachItemIn(idx, filters)
                        {
                            const IFieldFilter &thisFilter = rowFilter.addFilter(diskmeta->queryRecordAccessor(true), filters.item(idx));
                            unsigned idx = thisFilter.queryFieldIndex();
                            const RtlFieldInfo *field = inrec.queryOriginalField(idx);
                            if (field->flags & RFTMispayloadfield)
                                throw MakeStringException(0, "Cannot filter on payload field '%s'", field->name);
                        }
                    }
                    rowFilter.createSegmentMonitors(manager);
                }
                else
                {
                    // We don't have record info - fake it? We could pretend it's a single field...
                    UNIMPLEMENTED;
                    // manager.setown(createLocalKeyManager(fake, index, nullptr));
                }
                manager->finishSegmentMonitors();
                manager->reset();
                MyIndexVirtualFieldCallback callback(manager);
                while (manager->lookup(true) && count--)
                {
                    byte const * buffer = manager->queryKeyBuffer();
                    size32_t size = manager->queryRowSize();
                    unsigned __int64 seq = manager->querySequence();
                    if (optRaw)
                    {
                        fwrite(buffer, 1, size, stdout);
                    }
                    else if (optHex)
                    {
                        for (unsigned i = 0; i < size; i++)
                            printf("%02x", ((unsigned char) buffer[i]) & 0xff);
                        printf("  :%" I64F "u\n", seq);
                    }
                    else if (translator)
                    {
                        MemoryBuffer buf;
                        MemoryBufferBuilder aBuilder(buf, 0);
                        if (translator->translate(aBuilder, callback, buffer))
                        {
                            outmeta->toXML(aBuilder.getSelf(), *writer.get());
                            printf("%s\n", writer->str());
                            writer->clear();
                        }
                        else
                            count++;  // Row was postfiltered
                    }
                    else
                        printf("%.*s  :%" I64F "u\n", size, buffer, seq);
                    manager->releaseBlobs();
                }
                if (outRecType)
                    outRecType->doDelete();
                ForEachItemIn(idx, deleteFields)
                {
                    delete deleteFields.item(idx);
                }
            }
        }
        catch (IException *E)
        {
            StringBuffer msg;
            E->errorMessage(msg);
            printf("%s\n", msg.str());
            E->Release();
        }
    }
    releaseAtoms();
    ExitModuleObjects();
    return 0;
}


