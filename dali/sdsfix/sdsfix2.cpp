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


#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jptree.hpp"
#include "jfile.hpp"

#include "dasds.ipp"

#define SDSINCPREFIX "sdsinc"
enum IncCmd { None, PropDelete, AttrDelete, PropChange, PropNew, PropExisting, ChildEndMarker, PropRename };
enum IncInfo { IINull=0x00, IncData=0x01, IncDetails=0x02, IncConnect=0x04, IncDisconnect=0x08, IncDisconnectDelete=0x24 };
#define EXTERNAL_BRANCH "External"

#define CRC_VALIDATION

static bool recoverFromIncErrors=true;

struct CTreeItem : public CInterface
{
    String *tail;
    CTreeItem *parent;
    unsigned index;
    unsigned startOffset;
    unsigned endOffset;
    unsigned adjust;
    bool supressidx;
    CTreeItem(CTreeItem *_parent, String *_tail, unsigned _index, unsigned _startOffset)
    {
        parent = LINK(_parent);
        startOffset = _startOffset;
        endOffset = 0;
        adjust = 0;
        index = _index;
        supressidx = true;
        tail = _tail;
    }
    ~CTreeItem()
    {
        ::Release(parent);
        ::Release(tail);
    }
    void getXPath(StringBuffer &xpath)
    {
        if (parent)
            parent->getXPath(xpath);
        xpath.append('/').append(tail->toCharArray());
        if ((index!=0)||tail->IsShared())
            xpath.append('[').append(index+1).append(']');
    }
    unsigned size() { return endOffset?(endOffset-startOffset):0; }
    unsigned adjustedSize(bool &adjusted) { adjusted = (adjust!=0); return size()-adjust; } 
};

class CXMLSizesParser : public CInterface 
{
    Owned<IPullXMLReader> xmlReader;
    XmlReaderOptions xmlOptions;
    double pc;

    class CParse : public CInterface, implements IPTreeNotifyEvent
    {
        CIArrayOf<CTreeItem> stack;
        String * levtail;
        CIArrayOf<CTreeItem> arr;
        unsigned limit;
        __int64 totalSize;

        static int _sortF(CInterface **_left, CInterface **_right)
        {
            CTreeItem **left = (CTreeItem **)_left;
            CTreeItem **right = (CTreeItem **)_right;
            return ((*right)->size() - (*left)->size());
        }
    public:

        IMPLEMENT_IINTERFACE;

        CParse(unsigned __int64 _totalSize, double limitpc) : totalSize(_totalSize)
        {
            levtail = NULL; 
            limit = (unsigned)((double)totalSize*limitpc/100.0);
        }
        void reset()
        {
            stack.kill();
        }

// IPTreeNotifyEvent
        virtual void beginNode(const char *tag, offset_t startOffset)
        {
            String *tail = levtail;
            if (levtail&&(0 == strcmp(tag, levtail->toCharArray()))) 
                tail->Link();
            else 
                tail = new String(tag);
            levtail = NULL;     // opening new child
            CTreeItem *parent = stack.empty()?NULL:&stack.tos();
            CTreeItem *item = new CTreeItem(parent, tail, tail->getLinkCount()-1, startOffset);
            stack.append(*item);
        }
        virtual void newAttribute(const char *tag, const char *value)
        {
        }
        virtual void beginNodeContent(const char *tag)
        {
        }
        virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
        {
            CTreeItem *tos = &stack.tos();
            assertex(tos);
            tos->endOffset = endOffset;
            bool adjusted;
            unsigned sz = tos->adjustedSize(adjusted);
            if (sz>=limit)
            {
                CTreeItem *parent = tos->parent;
                while (parent) {
                    parent->adjust += sz;
                    parent = parent->parent;
                }
                tos->Link();
                arr.append(*tos);
                levtail = tos->tail;
            }
            else
                levtail = NULL;
            stack.pop();
        }

        void printFullResults()
        {
            arr.sort(_sortF);
            ForEachItemIn(m, arr)
            {
                CTreeItem &match = arr.item(m);
                StringBuffer xpath;
                match.getXPath(xpath);
                printf("xpath=%s, size=%d\n", xpath.str(), match.size());
            }
        }
        void printResultTree()
        {
            if (!totalSize)
                return;
            StringBuffer res;
            ForEachItemIn(i, arr) {
                CTreeItem &item = arr.item(i);
                bool adjusted;
                unsigned sz = item.adjustedSize(adjusted);
                if (sz>=limit) {
                    res.clear();
                    item.getXPath(res);
                    if (adjusted)
                        res.append(" (rest)");
                    res.padTo(40);
                    res.appendf(" %10d(%5.2f%%)",sz,((float)sz*100.0)/(float)totalSize);
                    printf("%s\n",res.str());
                }
            }
        }
    } *parser;

public:
    IMPLEMENT_IINTERFACE;

    CXMLSizesParser(const char *fName, XmlReaderOptions _xmlOptions=xr_none, double _pc=1.0) : xmlOptions(_xmlOptions), pc(_pc) { go(fName); }
    ~CXMLSizesParser() { ::Release(parser); }

    void go(const char *fName) 
    {
        OwnedIFile ifile = createIFile(fName);
        OwnedIFileIO ifileio = ifile->open(IFOread);
        if (!ifileio)
            throw MakeStringException(0, "Failed to open: %s", ifile->queryFilename());
        parser = new CParse(ifileio->size(), pc);
        Owned<IIOStream> stream = createIOStream(ifileio);
        xmlReader.setown(createPullXMLStreamReader(*stream, *parser, xmlOptions));
    }

    void printResultTree()
    {
        parser->printResultTree();
    }

    virtual bool next()
    {
        return xmlReader->next();
    }

    virtual void reset()
    {
        parser->reset();
        xmlReader->reset();
    }
};


void XmlSize(const char *filename, double pc)
{
    try {
        Owned<CXMLSizesParser> parser = new CXMLSizesParser((filename&&*filename)?filename:"dalisds.xml", xr_none, pc);
        while (parser->next())
            ;
        parser->printResultTree();
    }
    catch (IException *e)
    {
        pexception("xmlsize",e);
        e->Release();
    }
}

class CParseStackInfo : public CInterface
{
public:
    CParseStackInfo(const char *_tag, unsigned _index) : tag(_tag), index(_index), startOffset(0), endOffset(0), ext(false) { }
    StringAttr tag;
    unsigned index;
    unsigned startOffset, endOffset;
    bool ext;
};

static int sortN(unsigned *left, unsigned *right)
{
    return (*left - *right);
}

static int sortS(const char **left, const char **right)
{
    return stricmp(*left, *right);
}

StringBuffer &getExternalFname(IPropertyTree &tree, StringBuffer &filename)
{
    tree.getProp("@name", filename);
    filename.append('.');
    const char *format = tree.queryProp("@format");
    if (!format)
        return filename.clear();
    if (0 == stricmp("EFBinary", format))
        filename.append(EF_LegacyBinaryValue);
    else if (0 == stricmp("EFBinaryV2", format))
        filename.append(EF_BinaryValue);
    else if (0 == stricmp("EFXML", format))
        filename.append(EF_XML);
    else
        throwUnexpected();
    return filename;
}

void saveInsertBranch(const char *newStoreName, IFileIOStream &inStream, IPropertyTree &branch, CParseStackInfo &atBranch, bool skipAtBranch)
{
    PROGLOG("saving new store to: %s", newStoreName);
    OwnedIFile newFile = createIFile(newStoreName);
    OwnedIFileIO newFileIO = newFile->open(IFOcreate);
    OwnedIFileIOStream newStream = createIOStream(newFileIO);
    inStream.seek(0, IFSbegin);
    offset_t inFileSize = inStream.size();
    // copy original xml up to insertPoint offset
    unsigned r = atBranch.startOffset;
    MemoryBuffer mb;
    void *mem = mb.reserveTruncate(8192);
    while (r)
    {
        unsigned a = r>8192?8192:r;
        inStream.read(a, mem);
        newStream->write(a, mem);
        r -= a;
    }
    // save out new branch
    saveXML(*newStream, &branch, 1, 0);

    if (skipAtBranch)
    {
        inStream.seek(atBranch.endOffset, IFSbegin);
        r = (unsigned)(inFileSize-atBranch.endOffset);
    }
    else
        r = (unsigned)(inFileSize-atBranch.startOffset);
    while (r)
    {
        unsigned a = r>8192?8192:r;
        inStream.read(a, mem);
        newStream->write(a, mem);
        r -= a;
    }
    newStream.clear();
    newFileIO.clear();
    PROGLOG("New store saved");
}

void checkExternals(unsigned argc, char **args)
{
    bool fixXml = true;
    bool verbose = false;
    bool fixPhysical = false;
    bool dryRun = false;
    bool createPhysicalsDummys = true;
    bool createExternalBranch = false;
    unsigned a=0;
    while (a<argc)
    {
        const char *arg = args[a];
        if (arg && '-' == *arg || '+' == *arg)
        {
            bool onOff = ('+' == *arg);
            switch (arg[1])
            {
            case 'x':
                fixXml = onOff;
                break;
            case 'v': // verbose
                verbose = onOff;
                break;
            case 'p':
                fixPhysical = onOff;
                break;
            case 'c':
                createPhysicalsDummys = onOff;
                break;
            case 'b':
                createExternalBranch = onOff;
                break;
            case 'd':
                dryRun = onOff;
                break;
            }
        }
        a++;
    }

    if (fixXml) PROGLOG("Fix xml");
    if (fixPhysical) PROGLOG("Fix physical");
    if (createPhysicalsDummys) PROGLOG("Create physical dummy files");
    if (verbose) PROGLOG("Verbose");
    if (dryRun) PROGLOG("Dry run");
    if (createExternalBranch)
        PROGLOG("Create External branch (for legacy dali builds)");

    class CParse : public CInterface, implements IPTreeNotifyEvent
    {
        Owned<IPullXMLReader> xmlReader;
        StringAttr lastAtLevel;
        unsigned lastAtLevelCount;
        CIArrayOf<CParseStackInfo> stack;
        unsigned matchAdded;
        bool verbose;

    public:
        IMPLEMENT_IINTERFACE;

        CParse(bool _traceExternalRefs) : lastAtLevelCount(0), externalBranch(NULL), lastOffset(0), verbose(_traceExternalRefs) { }

// IPTreeNotifyEvent
        virtual void beginNode(const char *tag, offset_t startOffset)
        {
            matchAdded = 0;
            lastOffset = startOffset;
            bool e = lastAtLevel.length() && (0 == strcmp(tag, lastAtLevel));
            if (e)
                lastAtLevelCount++;
            else
                lastAtLevelCount = 0;
            lastAtLevel.clear();
            CParseStackInfo *stackInfo = new CParseStackInfo(tag, lastAtLevelCount);
            stackInfo->startOffset = startOffset;
            stack.append(*stackInfo);
        }
        virtual void newAttribute(const char *tag, const char *value)
        {
            CParseStackInfo &stackInfo = stack.tos();
            if (0 == stricmp(EXT_ATTR, tag))
            {
                unsigned i = atoi(value);
                if (NotFound == matches.bSearch(i, sortN))
                {
                    bool added;
                    matchAdded = i;
                    matches.bAdd(i, sortN, added);
                }
                stackInfo.ext = true;
            }
            else if (1 == stack.ordinality() && 0 == stricmp("@edition", tag))
            {
                StringBuffer filename(SDSINCPREFIX);
                filename.append("_E").append(value).append('_');
                filename.appendf("%04x", 1).append(".inc");
                Owned<IFile> file = createIFile(filename.str());
                if (file->exists())
                    throw MakeStringException(0, "Detected increments for this store, please coalesce before running extchk.");
            }
        }
        virtual void beginNodeContent(const char *tag)
        {
        }
        virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
        {
            lastOffset = endOffset;
            CParseStackInfo &stackInfo = stack.tos();
            stackInfo.endOffset = endOffset;

            StringBuffer xpath("/");
            ForEachItemIn(s, stack)
            {
                CParseStackInfo &sI = stack.item(s);
                xpath.append(sI.tag);
                xpath.append('[').append(sI.index+1).append(']');
                if (s < stack.ordinality()-1)
                    xpath.append('/');
            }
            if (matchAdded)
            {
                if (verbose)
                    PROGLOG("ext(%d) found at : %s", matchAdded, xpath.str());
                matchAdded = 0;
            }
            if (stack.ordinality() == 2)
                lastBranchBelowRoot.set(&stackInfo);
            if (0 == stricmp("/SDS[1]/External[1]", xpath.str()))
                externalBranch.set(&stackInfo);
            lastAtLevel.set(stackInfo.tag);
            stack.pop();
        }

        void printResults()
        {
            unsigned m;
            for (m=0; m<matches.ordinality(); m++)
            {
                PROGLOG("%d", matches.item(m));
            }
        }

    public: // data
        unsigned __int64 lastOffset;
        UnsignedArray matches;
        Linked<CParseStackInfo> externalBranch, lastBranchBelowRoot;
    } parser(verbose);

    Owned<IStoreHelper> iStoreHelper = createStoreHelper(NULL, "", NULL, SH_External);
    StringBuffer fname;
    OwnedIFile iFile = createIFile(iStoreHelper->getCurrentDeltaFilename(fname).str());
    if (iFile->exists())
    {
        PROGLOG("The store is not coalesced - coalesce before beginning this test");
        return;
    }
    iStoreHelper->getCurrentStoreFilename(fname.clear());

    OwnedIFile inFile = createIFile(fname.str());
    offset_t inFileSize = inFile->size();
    OwnedIFileIO inFileIO = inFile->open(IFOread);
    if (!inFileIO)
        throw MakeStringException(0, "Failed to open: %s", inFile->queryFilename());
    Owned<IFileIOStream> inFileStream = createIOStream(inFileIO);
    Owned<IPullXMLReader> xmlReader = createPullXMLStreamReader(*inFileStream, parser, xr_none);

    unsigned lp = 0;
    while (xmlReader->next())
    {
        unsigned p = (unsigned)((parser.lastOffset*100)/inFileSize);
        if (p != lp)
        {
            if (verbose) printf("\n");
            else printf("%c", 13);
            printf("Scanning for external references .. %3d%%  ", p);
            lp = p;
        }
    }
    if (verbose) printf("\n");
    else printf("%c", 13);
    printf("Scanning for external references .. %3d%%  \n", 100);
    xmlReader.clear();

    if (fixPhysical)
        recursiveCreateDirectory("extbak");

    StringArray referencedFiles;
    unsigned missingFiles = 0;
    unsigned deleted = 0;
    unsigned removedDefs = 0;

    if (NULL == parser.externalBranch)
    {
        PROGLOG("No 'External' branch"); // new format

        // Check referenced externals have corresponding external declaration.
        Owned<IPropertyTree> externalTree;
        if (createExternalBranch)
            externalTree.setown(createPTree("External", false));
        ForEachItemIn(r, parser.matches)
        {
            unsigned index = parser.matches.item(r);
            StringBuffer filename(EXTERNAL_NAME_PREFIX);
            filename.append(index).append('.').append(EF_BinaryValue);
            OwnedIFile ifile = createIFile(filename.str());
            if (!ifile->exists())
            {
                if (fixPhysical && createPhysicalsDummys)
                {
                    // referenced by store, but file missing
                    PROGLOG("Creating empty replacement for missing external file: %s", filename.str());
                    OwnedIFile ifile = createIFile(filename.str());
                    OwnedIFileIO io = ifile->open(IFOcreate);
                    unsigned s=0;
                    io->write(0, sizeof(s), &s);
                    missingFiles++;
                    if (NotFound == referencedFiles.find(filename.str()))
                        referencedFiles.append(filename.str());
                }
                else
                {
                    // referenced, definition present but file not
                    // if wanted to remove refernces instead of add entry below, would need to record offsets of attribute in 'matches' and patch copy
                    PROGLOG("index %d referenced but no external file", index);
                }
            }
            else if (NotFound == referencedFiles.find(filename.str()))
                referencedFiles.append(filename.str());
            if (createExternalBranch)
            {
                Owned<IPropertyTree> ext = createPTree(false);
                ext->setPropInt("@index", index);
                ext->setProp("@format", "EFBinaryV2");
                StringBuffer name(EXTERNAL_NAME_PREFIX);
                ext->setProp("@name", name.append(index).str());
                externalTree->addPropTree("File", ext.getClear());
            }
        }
        if (createExternalBranch)
        {
            if (!dryRun)
                saveInsertBranch("dalisds_fixed.xml", *inFileStream, *externalTree, *parser.lastBranchBelowRoot, false);
        }
    }
    else // old format
    {
        if (createExternalBranch)
        {
            PROGLOG("Ignoring +b [createExternalBranch] option, External branch already exists");
            createExternalBranch = false;
        }

        ICopyArrayOf<IPropertyTree> unused;
        unsigned missingRefs = 0;

        unsigned externalStart = parser.externalBranch->startOffset;
        unsigned externalEnd = parser.externalBranch->endOffset;
        inFileStream->seek(externalStart, IFSbegin);
        MemoryBuffer extMb;
        void *dst = extMb.reserveTruncate(externalEnd-externalStart);
        inFileStream->read(externalEnd-externalStart, dst);
        Owned<IFileIO> memIFileIO = createIFileIO(extMb);
        Owned<IPropertyTree> externalTree = createPTree(*memIFileIO, false);

        // Check referenced externals have corresponding external declaration.
        ForEachItemIn(r, parser.matches)
        {
            unsigned index = parser.matches.item(r);
            StringBuffer path("File[@index=\"");
            path.append(index).append("\"]");
            if (!externalTree->queryPropTree(path.str()))
            {
                PROGLOG("Missing external declaration: %d", index);
                if (fixXml)
                {
                    PROGLOG("Creating replacement declaration for missing reference: %d", index);
                    IPropertyTree *ext = createPTree(false);
                    ext->setPropInt("@index", index);
                    ext->setProp("@format", "EFBinaryV2");
                    StringBuffer name(EXTERNAL_NAME_PREFIX);
                    ext->setProp("@name", name.append(index).str());
                    externalTree->addPropTree("File", ext);
                    missingRefs++;
                }
            }
        }
        // 1) Check for External branches that are unreferenced.
        // 2) Check that branches that are referenced have external file present.
        Owned<IPropertyTreeIterator> iter = externalTree->getElements("*");
        ForEach (*iter)
        {
            IPropertyTree &file = iter->query();
            unsigned index = file.getPropInt("@index");
            if (NotFound == parser.matches.bSearch(index, sortN))
            {
                PROGLOG("index %d not referenced", index);
                unused.append(file);
            }
            else
            {
                StringBuffer filename;
                getExternalFname(file, filename);
                OwnedIFile ifile = createIFile(filename.str());
                if (!ifile->exists())
                {
                    if (fixPhysical && createPhysicalsDummys)
                    {
                        // referenced by store, but file missing
                        PROGLOG("Creating empty replacement for missing external file: %s", filename.str());
                        OwnedIFile ifile = createIFile(filename.str());
                        OwnedIFileIO io = ifile->open(IFOcreate);
                        const char *format = file.queryProp("@format");
                        if (0 == stricmp("EFBinary", format) || 0 == stricmp("EFBinaryV2", format))
                        {
                            unsigned s=0;
                            io->write(0, sizeof(s), &s);
                        }
                        missingFiles++;
                        if (NotFound == referencedFiles.find(filename.str()))
                            referencedFiles.append(filename.str());
                    }
                    else
                    {
                        // referenced, definition present but file not
                        // if wanted to remove refernces instead of add entry below, would need to record offsets of attribute in 'matches' and patch copy
                        PROGLOG("index %d referenced but no external file", index);
                    }
                }
                else if (NotFound == referencedFiles.find(filename.str()))
                    referencedFiles.append(filename.str());
            }
        }
        // Remove unrefenced external branches
        ForEachItemIn(i, unused)
        {
            IPropertyTree &tree = unused.item(i);

            StringBuffer filename;
            getExternalFname(tree, filename);
            if (fixPhysical && filename.str())
            {
                OwnedIFile ifile = createIFile(filename.str());
                if (ifile->exists())
                {
                    PROGLOG("Deleting unreferenced external file (that has External definition): %s", filename.str());
                    StringBuffer dest("extbak");
                    dest.append(PATHSEPCHAR);
                    dest.append(filename.str());
                    OwnedIFile iFileDst = createIFile(dest.str());
                    iFileDst->remove();
                    ifile->move(dest.str());
                    deleted++;
                }
            }
            if (fixXml)
            {
                PROGLOG("Removing unreferenced external definition for: %s", filename.str());
                verifyex(externalTree->removeTree(&tree));
            }
            else
                PROGLOG("Unreferenced external definition for: %s", filename.str());
        }
        if (fixXml)
            removedDefs = unused.ordinality();

        if (missingRefs || unused.ordinality()) // only for old format
        {
            if (!dryRun)
                saveInsertBranch("dalisds_fixed.xml", *inFileStream, *externalTree, *parser.externalBranch, true);
            if (fixXml)
                PROGLOG("%d external definitions created that were referenced", missingRefs);
        }
        else
            PROGLOG("No external definitions removed/altered - xml unaltered");
    }

    if (fixPhysical)
    {
        // Delete physical externals not referenced
        StringBuffer prefix(EXTERNAL_NAME_PREFIX);
        Owned<IDirectoryIterator> di = createDirectoryIterator(NULL, prefix.append("*").str());
        ForEach(*di)
        {
            StringBuffer filename;
            di->getName(filename);
            const char *tail = filename.str()+strlen(EXTERNAL_NAME_PREFIX);
            unsigned index = atoi(tail);
            if (index && NotFound == referencedFiles.find(filename.str()))
            {
                PROGLOG("Deleting unreferenced external file: %s (not referenced by store or external definition)", filename.str());
                StringBuffer dest("extbak");
                dest.append(PATHSEPCHAR);
                dest.append(filename.str());
                OwnedIFile ifile = createIFile(filename.str());
                OwnedIFile iFileDst = createIFile(dest.str());
                iFileDst->remove();
                ifile->move(dest.str());
                deleted++;
            }
        }
        PROGLOG("Referenced items created:");
        PROGLOG("%d external files created that were referenced", missingFiles);
    }

    if (removedDefs)
        PROGLOG("%d unused external definitions removed from xml", removedDefs);
    if (deleted)
        PROGLOG("%d unused external files deleted", deleted);
}

void coalesceStore()
{
    const char *daliDataPath = NULL;
    const char *remoteBackupLocation = NULL;
    Owned<IStoreHelper> iStoreHelper = createStoreHelper(NULL, daliDataPath, remoteBackupLocation, SH_External|SH_RecoverFromIncErrors);
    unsigned baseEdition = iStoreHelper->queryCurrentEdition();

    StringBuffer storeFilename(daliDataPath);
    iStoreHelper->getCurrentStoreFilename(storeFilename);
    PROGLOG("Loading store: %s", storeFilename.str());
    Owned<IPropertyTree> root = createPTreeFromXMLFile(storeFilename.str(), false);
    PROGLOG("Loaded: %s", storeFilename.str());

    if (baseEdition != iStoreHelper->queryCurrentEdition())
        PROGLOG("Store was changed by another process prior to coalesce. Exiting.");
    else
    {
        if (!iStoreHelper->loadDeltas(root))
            PROGLOG("Nothing to coalesce");
        else
            iStoreHelper->saveStore(root, &baseEdition);
    }
}


struct CTagItem : public CInterface
{
    StringAttr tag;
    OwningStringSuperHashTableOf<CTagItem> children;
    CTagItem *parent;

    const char *queryFindString() const { return tag; }
    CTagItem(CTagItem *_parent, const char *_tag) : parent(_parent), tag(_tag)
    {
    }

    void getXPath(StringBuffer &xpath)
    {
        if (parent)
            parent->getXPath(xpath);
        xpath.append('/').append(tag);
    }
};

void getTags(const char *filename)
{
    class CParse : public CInterface, implements IPTreeNotifyEvent
    {
        CTagItem *parent;

    public:
        IMPLEMENT_IINTERFACE;

        CParse(CTagItem *_parent) : parent(_parent)
        {
        }

// IPTreeNotifyEvent
        virtual void beginNode(const char *tag, offset_t startOffset)
        {
            CTagItem *child = parent->children.find(tag);
            if (!child)
            {
                child = new CTagItem(parent, tag);
                parent->children.replace(*child);
            }
            parent = child;
        }
        virtual void newAttribute(const char *tag, const char *value) { }
        virtual void beginNodeContent(const char *tag) { }
        virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
        {
            parent = parent ? parent->parent : NULL;
        }

        void printFullResults(CTagItem *parent)
        {
            if (0 == parent->children.count()) // so a leaf
            {
                StringBuffer xpath;
                parent->getXPath(xpath);
                printf("xpath=%s\n", xpath.str());
            }
            else
            {
                SuperHashIteratorOf<CTagItem> iter(parent->children);
                ForEach (iter)
                {
                    CTagItem &item = iter.query();
                    printFullResults(&item);
                }
            }
        }
    } *parser;

    Owned<CTagItem> root = new CTagItem(NULL, "__root__");
    parser = new CParse(root);

    OwnedIFile ifile = createIFile(filename);
    OwnedIFileIO ifileio = ifile->open(IFOread);
    if (!ifileio)
        throw MakeStringException(0, "Failed to open: %s", ifile->queryFilename());
    Owned<IIOStream> stream = createIOStream(ifileio);
    Owned<IPullXMLReader> xmlReader = createPullXMLStreamReader(*stream, *parser, xr_noRoot);

    while (xmlReader->next());

    parser->printFullResults(root);
}

// class to write events out as xml
class CPTNEWriter : public CInterface, implements IPTreeNotifyEvent
{
    unsigned indent;
    byte flags;
    IIOStream &out;
    unsigned attrn, attributeindent;
    StringAttr cname;
    bool inlinebody;
public:
    IMPLEMENT_IINTERFACE;

    CPTNEWriter(IIOStream &_out) : out(_out)
    {
        out.Link();
        attributeindent = attrn = 0;
        indent = 0;
        flags = 0; // XML_Format;
        inlinebody = false;
    }
    ~CPTNEWriter()
    {
        out.Release();
    }
// IPTreeNotifyEvent
    virtual void beginNode(const char *tag, offset_t startOffset)
    {
        if (flags & XML_Format) writeCharsNToStream(out, ' ', indent);
        writeCharToStream(out, '<');
        writeStringToStream(out, tag);
        attrn = 0;
        cname.set(tag);
        attributeindent = indent+2+(size32_t)strlen(tag);
        inlinebody = true;
        ++indent;
    }
    virtual void newAttribute(const char *tag, const char *value)
    {
        if (0 == attrn)
        {
            ++attrn;
            if (flags & XML_Format) inlinebody = false;
            writeCharToStream(out, ' ');
        }
        else if (0 == attrn % 3)
        {
            writeStringToStream(out, "\n");
            writeCharsNToStream(out, ' ', attributeindent);
        }
        else
            writeCharToStream(out, ' ');

        writeStringToStream(out, tag+1);
        if (flags & XML_SingleQuoteAttributeValues)
            writeStringToStream(out, "='");
        else
            writeStringToStream(out, "=\"");
        if (value)
        {
            if (flags & XML_SanitizeAttributeValues)
            {
                if (strcmp(value, "0")==0 || strcmp(value, "1")==0 || stricmp(value, "true")==0 || stricmp(value, "false")==0 || stricmp(value, "yes")==0 || stricmp(value, "no")==0)
                    encodeXML(value, out, ENCODE_NEWLINES, (unsigned)-1, true);
                else
                {
                    writeCharsNToStream(out, '*', strlen(value));
                }
            }
            else
                encodeXML(value, out, ENCODE_NEWLINES, (unsigned)-1, true);
        }
        if (flags & XML_SingleQuoteAttributeValues)
            writeCharToStream(out, '\'');
        else
            writeCharToStream(out, '"');
    }
    virtual void beginNodeContent(const char *tag)
    {
        if (flags & XML_Format)
            writeStringToStream(out, ">\n");
        else
            writeCharToStream(out, '>');
    }
    virtual void endNode(const char *tag, unsigned length, const void *_value, bool binary, offset_t endOffset)
    {
        const char *value = (const char *)_value;
        if (!inlinebody)
            writeStringToStream(out, "\n");

        if (!inlinebody)
            writeCharsNToStream(out, ' ', indent+1);
        if (flags & XML_Sanitize)
        {
            // NOTE - we don't output anything for binary.... is that ok?
            if (length)
            {
                if (strcmp(value, "0")==0 || strcmp(value, "1")==0 || stricmp(value, "true")==0 || stricmp(value, "false")==0 || stricmp(value, "yes")==0 || stricmp(value, "no")==0)
                    writeStringToStream(out, value);
                else
                    writeCharsNToStream(out, '*', length);
            }
        }
        else if (binary)
            JBASE64_Encode(value, length, out);
        else
        {
            if (flags & XML_NoEncode)
            {
                writeStringToStream(out, value);
            }
            else
            {
                const char *m = value;
                const char *p = m;
                while (isspace(*p)) 
                    p++;
                encodeXML(m, out, ENCODE_WHITESPACE, p-m, true);
                if (*p) {   // check not all spaces
                    const char *s = p+strlen(p);
                    while (isspace(*(s-1)))
                        s--;
                    assertex(s>p);
                    encodeXML(p, out, 0, s-p, true);
                    encodeXML(s, out, ENCODE_WHITESPACE, (unsigned)-1, true);
                }
            }
            
            if (!inlinebody)
                writeStringToStream(out, "\n");
        }

        if (!inlinebody)
            writeCharsNToStream(out, ' ', indent);

        if (!inlinebody)
            writeCharsNToStream(out, ' ', indent);

        writeStringToStream(out, "</");
        writeStringToStream(out, tag);
        if (flags & XML_Format)
            writeStringToStream(out, ">\n");
        else
            writeCharToStream(out, '>');
        --indent;
    }
};


class CAttrSize : public CInterface
{
public:
    CAttrSize(const char *_path, const char *_attrname, unsigned _size) : path(_path), attrname(_attrname), size(_size), stripped(0) { }
    StringAttr path, attrname;
    unsigned size, stripped;
};

enum xsroptions { xsr_attrsizes=1, xsr_output=2, xsr_fixutf8=4 };
void XmlPatch(const char *inf, char **options)
{
    class CParse : public CInterface, implements IPTreeNotifyEvent
    {
        CIArrayOf<CTreeItem> stack;
        String * levtail;
        CIArrayOf<CTreeItem> arr;
        unsigned attrCheckThreshold, options;
        CIArrayOf<CAttrSize> attrSizeArray;

        static int _sortAttrSizes(CInterface **_left, CInterface **_right)
        {
            CAttrSize **left = (CAttrSize **)_left;
            CAttrSize **right = (CAttrSize **)_right;
            return ((*right)->size - (*left)->size);
        }
    public:
        Linked<IPTreeNotifyEvent> output;

        IMPLEMENT_IINTERFACE;

        CParse(IPTreeNotifyEvent *_output, unsigned _attrCheckThreshold, unsigned _options=0) : output(_output), attrCheckThreshold(_attrCheckThreshold), options(_options)
        {
            levtail = NULL;
        }
        void reset()
        {
            stack.kill();
        }

// IPTreeNotifyEvent
        virtual void beginNode(const char *tag, offset_t startOffset)
        {
            String *tail = levtail;
            if (levtail&&(0 == strcmp(tag, levtail->toCharArray()))) 
                tail->Link();
            else 
                tail = new String(tag);
            levtail = NULL;     // opening new child
            CTreeItem *parent = stack.empty()?NULL:&stack.tos();
            CTreeItem *item = new CTreeItem(parent, tail, tail->getLinkCount()-1, startOffset);
            stack.append(*item);

            if (output)
                output->beginNode(tag, startOffset);
        }
        virtual void newAttribute(const char *tag, const char *value)
        {
            if (attrCheckThreshold)
            {
                unsigned l = strlen(value);
                if (l > attrCheckThreshold)
                {
                    StringBuffer path;
                    stack.tos().getXPath(path);
                    CAttrSize *as = new CAttrSize(path.str(), tag+1, l);
                    attrSizeArray.append(*as);

                    if (0 != (options & ((unsigned)xsr_fixutf8)))
                    {
                        const char *vp = value;
                        const char *vpe = value+l;
                        char state0c = 0;

                        StringBuffer out;
                        loop
                        {
                            byte c = *vp;
                            if (0 == state0c)
                            {
                                if (c >= 0xC2 && c <= 0xC3)
                                {
                                    ++as->stripped;
                                    state0c = c;
                                }
                                else
                                    out.append(*vp);
                            }
                            else
                            {
                                if (c >= 0x82 && c <= 0x83)
                                {
                                    ++as->stripped;
                                }
                                else
                                {
                                    --as->stripped;
                                    out.append(state0c);
                                    out.append(c);
                                }
                                state0c = 0;
                            }
                            ++vp;
                            if (vp == vpe)
                                break;
                        }
                        if (output)
                            output->newAttribute(tag, out.str());
                    }
                    else if (output)
                        output->newAttribute(tag, value);
                }
                else if (output)
                    output->newAttribute(tag, value);
            }
            else if (output)
                output->newAttribute(tag, value);
        }
        virtual void beginNodeContent(const char *tag)
        {
            if (output)
                output->beginNodeContent(tag);
        }
        virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
        {
            CTreeItem *tos = &stack.tos();
            assertex(tos);
            tos->endOffset = endOffset;
            tos->Link();
            arr.append(*tos);
            levtail = tos->tail;
            stack.pop();

            if (output)
                output->endNode(tag, length, value, binary, endOffset);
        }
        void printAttrSizes()
        {
            printf("Attributes larger >%d bytes\n", attrCheckThreshold);
            attrSizeArray.sort(_sortAttrSizes);
            if (0 != (options & ((unsigned)xsr_fixutf8)))
            {
                ForEachItemIn(a, attrSizeArray)
                {
                    CAttrSize &as = attrSizeArray.item(a);
                    printf("%10d, %10d - %s, path=%s\n", as.size, as.stripped, as.attrname.get(), as.path.get());
                }
            }
            else
            {
                ForEachItemIn(a, attrSizeArray)
                {
                    CAttrSize &as = attrSizeArray.item(a);
                    printf("%10d, %s, path=%s\n", as.size, as.attrname.get(), as.path.get());
                }
            }
        }
    };

    OwnedIFile iFileIn = createIFile(inf);
    OwnedIFileIO iFileIOIn = iFileIn->open(IFOread);
    Owned<IFileIOStream> iosIn = createIOStream(iFileIOIn);

    unsigned attrCheckThreshold = 0;
    unsigned uopt = 0;
    StringAttr outputfname("out.xml");
    while (options)
    {
        char *opt = *options;
        if (!opt) break;
        if (0 == stricmp("fixutf8", opt))
        {
            uopt |= (unsigned)xsr_fixutf8;
            uopt |= (unsigned)xsr_output;
            attrCheckThreshold = 1;
        }
        else if (0 == stricmp("attrsizes", opt))
        {
            ++options;
            if (NULL == *options)
            {
                --options;
                attrCheckThreshold = 1;
            }
            else
                attrCheckThreshold = atoi(*options);
            uopt |= (unsigned)xsr_attrsizes;
        }
        else if (0 == stricmp("output", opt))
        {
            ++options;
            if (NULL == *options)
                --options;
            else
                outputfname.set(*options);
            uopt |= (unsigned)xsr_output;
        }
        else
            DBGLOG("XmlScanRewrite: Unknown option: %s", opt);
        ++options;
    }

    if (0 == uopt)
    {
        printf("nothing to do\n");
        exit(1);
    }
    Owned<CPTNEWriter> output;
    if (uopt & ((unsigned)xsr_output))
    {
        OwnedIFile iFileOut = createIFile(outputfname.get());
        OwnedIFileIO iFileIOOut = iFileOut->open(IFOcreate);
        Owned<IFileIOStream> iosOut = createIOStream(iFileIOOut);
        output.setown(new CPTNEWriter(*iosOut));
    }
    CParse reader(output, attrCheckThreshold, uopt);
    
    Owned<IPullXMLReader> xmlReader = createPullXMLStreamReader(*iosIn, reader);
    xmlReader->load();
    xmlReader.clear();

    output.clear();

    if (0 != (uopt & ((unsigned)xsr_attrsizes)))
        reader.printAttrSizes();
}

