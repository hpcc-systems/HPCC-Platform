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
#include "hql.hpp"

#include "jlib.hpp"
#include "hqlres.hpp"
#include "hqlcpp.ipp"
#include "jmisc.hpp"
#include "jexcept.hpp"
#include "hqlcerrors.hpp"
#include "thorplugin.hpp"
#include "codesigner.hpp"

#define BIGSTRING_BASE 101
#define MANIFEST_BASE 1000

class ResourceItem : public CInterface
{
public:
    ResourceItem(const char * _type, unsigned _id, size32_t _len, const void * _ptr) 
        : data(_len, _ptr), type(_type), id(_id) {}

public:
    MemoryAttr data;
    StringAttr type;
    unsigned id;
};


ResourceManager::ResourceManager(IHqlCppInstance &_cppInstance) : cppInstance(_cppInstance)
{
    nextmfid = MANIFEST_BASE + 1;
    nextbsid = BIGSTRING_BASE;
    totalbytes = 0;
    finalized=false;
}

unsigned ResourceManager::count()
{
    return resources.ordinality();
}

unsigned ResourceManager::addString(unsigned len, const char *data)
{
    unsigned id = nextbsid++;
    resources.append(*new ResourceItem("BIGSTRING", id, len, data));
    return id;
}

void ResourceManager::addNamed(const char * type, unsigned len, const void * data, IPropertyTree *manifestEntry, unsigned id, bool addToManifest, bool compressed)
{
    if (id==(unsigned)-1)
        id = nextmfid++;
    if (addToManifest)
    {
        if (finalized)
            throwError1(HQLERR_ResourceAddAfterFinalManifest, type);
        Owned<IPropertyTree> entry=createPTree("Resource");
        entry->setProp("@type", type);
        entry->setPropInt("@id", id);
        entry->setPropBool("@compressed", compressed);
        entry->setPropBool("@header", true);
        if (manifestEntry)
            mergePTree(entry, manifestEntry);
        ensureManifestInfo()->addPropTree("Resource", entry.getClear());
    }
    MemoryBuffer mb;
    appendResource(mb, len, data, compressed);
    resources.append(*new ResourceItem(type, id, mb.length(), mb.toByteArray()));
}

bool ResourceManager::addCompress(const char * type, unsigned len, const void * data, IPropertyTree *manifestEntry, unsigned id, bool addToManifest)
{
    addNamed(type, len, data, manifestEntry, id, addToManifest, true);
    return true;
}

static void loadResource(const char *filepath, MemoryBuffer &content)
{
    Owned <IFile> f = createIFile(filepath);
    Owned <IFileIO> fio = f->open(IFOread);
    if (!fio)
        throw makeStringExceptionV(0, "Failed to open resource file %s", filepath);
    read(fio, 0, (size32_t) f->size(), content);
}

bool ResourceManager::getDuplicateResourceId(const char *srctype, const char *respath, const char *filepath, int &id)
{
    StringBuffer xpath;
    if (respath && *respath)
        xpath.appendf("Resource[@resourcePath='%s']", respath);
    else
        xpath.appendf("Resource[@originalFilename='%s']", filepath);
    Owned<IPropertyTreeIterator> iter = manifest->getElements(xpath.str());
    ForEach (*iter)
    {
        IPropertyTree &item = iter->query();
        if (item.hasProp("@id"))
        {
            const char *type = item.queryProp("@type");
            if (type && strieq(type, srctype))
            {
                id=item.getPropInt("@id");
                return true;
            }
        }
    }
    return false;
}

void updateManifestResourcePaths(IPropertyTree &resource, const char *dir)
{
    StringBuffer filepath;
    makeAbsolutePath(resource.queryProp("@filename"), dir, filepath);
    resource.setProp("@originalFilename", filepath.str());

    StringBuffer respath;
    makePathUniversal(filepath.str(), respath);
    resource.setProp("@resourcePath", respath.str());
}

void expandManifestDirectory(IPropertyTree *manifestSrc, IPropertyTree &res, StringBuffer &dir, IDirectoryIterator *it, const char*mask, bool recursive)
{
    if (!it)
        return;
    ForEach(*it)
    {
        if (it->isDir())
        {
            if (recursive)
                expandManifestDirectory(manifestSrc, res, dir, it->query().directoryFiles(mask, false, true), mask, recursive);
            continue;
        }
        StringBuffer reldir;
        Owned<IPropertyTree> newRes = createPTreeFromIPT(&res);
        reldir.append(splitRelativePath(it->query().queryFilename(), dir, reldir));
        VStringBuffer xpath("Resource[@filename='%s']", reldir.str());
        if (manifestSrc->hasProp(xpath))
            continue;
        newRes->setProp("@filename", reldir.str());
        updateManifestResourcePaths(*newRes, dir.str());
        if (manifestSrc->hasProp(xpath.setf("resource[@resourcePath='%s']", newRes->queryProp("@resourcePath"))))
            continue;
        manifestSrc->addPropTree("Resource", newRes.getClear());
    }
}

void expandManifestDirectory(IPropertyTree *manifestSrc, IPropertyTree &res, StringBuffer &dir, const char *path, const char*mask, bool recursive)
{
    Owned<IDirectoryIterator> it = createDirectoryIterator(path, mask);
    expandManifestDirectory(manifestSrc, res, dir, it, mask, recursive);
}


void ResourceManager::addManifestFile(const char *filename, ICodegenContextCallback *ctxCallback)
{
    StringBuffer fileContents;
    StringBuffer strippedFileContents;
    fileContents.loadFile(filename, false);
    bool isSigned = false;
    const char *useContents = fileContents;
    // Check for signature
    if (queryCodeSigner().hasSignature(fileContents))
    {
        try
        {
            StringBuffer signer;
            isSigned = queryCodeSigner().verifySignature(fileContents, signer);
            if (!isSigned)
                throw makeStringExceptionV(MSGAUD_user, CODESIGNER_ERR_VERIFY, "Code sign verify: signature not verified");
            useContents = queryCodeSigner().stripSignature(fileContents, strippedFileContents).str();
        }
        catch (IException *E)
        {
            StringBuffer msg;
            E->errorMessage(msg);
            auto code = E->errorCode();
            auto aud = E->errorAudience();
            E->Release();
            throw makeStringExceptionV(aud, code, "While loading manifest file %s: %s", filename, msg.str());
        }
    }
    Owned<IPropertyTree> manifestSrc = createPTreeFromXMLString(useContents);

    StringBuffer dir; 
    splitDirTail(filename, dir);

    ensureManifestInfo();
    Owned<IAttributeIterator> aiter = manifestSrc->getAttributes();
    ForEach (*aiter)
        manifest->setProp(aiter->queryName(), aiter->queryValue());
    Owned<IPropertyTreeIterator> iter = manifestSrc->getElements("*");
    ForEach(*iter)
    {
        IPropertyTree &item = iter->query();
        if (streq(item.queryName(), "Include") && item.hasProp("@filename"))
            addManifestInclude(item, dir.str(), ctxCallback);
        else if (streq(item.queryName(), "Resource") && item.hasProp("@filename"))
        {
            StringBuffer filepath;
            StringBuffer respath;
            makeAbsolutePath(item.queryProp("@filename"), dir.str(), filepath);
            makePathUniversal(filepath.str(), respath);

            item.setProp("@originalFilename", filepath.str());
            item.setProp("@resourcePath", respath.str());

            if (containsFileWildcard(filepath))
            {
                StringBuffer wildpath;
                const char *tail = splitDirTail(filepath, wildpath);
                expandManifestDirectory(manifestSrc, item, dir, wildpath, tail, item.getPropBool("@recursive"));
                manifestSrc->removeTree(&item);
            }

        }
        else
            manifest->addPropTree(item.queryName(), LINK(&item));
    }

    Owned<IPropertyTreeIterator> resources = manifestSrc->getElements("Resource[@filename]");
    ForEach(*resources)
    {
        IPropertyTree &item = resources->query();
        const char *resourceFilename = item.queryProp("@originalFilename");
        const char *md5 = item.queryProp("@md5");
        if (md5)
        {
            StringBuffer calculated;
            md5_filesum(resourceFilename, calculated);
            if (!strieq(calculated, md5))
                throw makeStringExceptionV(0, "MD5 mismatch on file %s in manifest %s", item.queryProp("@filename"), filename);
        }
        else if (isSigned)
            throw makeStringExceptionV(0, "MD5 must be supplied for file %s in signed manifest %s", item.queryProp("@filename"), filename);

        if (!item.hasProp("@type"))
            item.setProp("@type", "UNKNOWN");
        int id;
        if (getDuplicateResourceId(item.queryProp("@type"), item.queryProp("@resourcePath"), NULL, id))
        {
            item.setPropInt("@id", id);
            manifest->addPropTree("Resource", LINK(&item));
        }
        else
        {
            const char *type = item.queryProp("@type");
            if (strieq(type, "CPP") || strieq(type, "C"))
            {
                if (!ctxCallback->allowAccess("cpp", isSigned))
                    throw makeStringExceptionV(0, "Embedded code via manifest file not allowed");
                cppInstance.useSourceFile(resourceFilename, item.queryProp("@compileFlags"), false);
            }
            else
            {
                if ((strieq(type, "jar") || strieq(type, "pyzip")) && !ctxCallback->allowAccess(type, isSigned))
                    throw makeStringExceptionV(0, "Embedded %s files via manifest file not allowed", type);
                MemoryBuffer content;
                loadResource(resourceFilename, content);
                addCompress(type, content.length(), content.toByteArray(), &item); // MORE - probably should not recompress files known to be compressed, like jar
            }
        }
    }
}

void ResourceManager::addManifest(const char *filename, ICodegenContextCallback *ctxCallback)
{
    StringBuffer path;
    Owned<IPropertyTree> t = createPTree();
    t->setProp("@originalFilename", makeAbsolutePath(filename, path).str());
    ensureManifestInfo()->addPropTree("Include", t.getClear());
    addManifestFile(filename, ctxCallback);
}

void ResourceManager::addManifestInclude(IPropertyTree &include, const char *dir, ICodegenContextCallback *ctxCallback)
{
    StringBuffer includePath;
    makeAbsolutePath(include.queryProp("@filename"), dir, includePath);
    VStringBuffer xpath("Include[@originalFilename='%s']", includePath.str());
    if (manifest->hasProp(xpath.str()))
        return;
    include.setProp("@originalFilename", includePath.str());
    manifest->addPropTree("Include", LINK(&include));
    addManifestFile(includePath.str(), ctxCallback);
}

void ResourceManager::addManifestsFromArchive(IPropertyTree *archive, ICodegenContextCallback *ctxCallback)
{
    if (!archive)
        return;
    if (finalized)
        throwError1(HQLERR_ResourceAddAfterFinalManifest, "MANIFEST");
    ensureManifestInfo();
    Owned<IPropertyTreeIterator> manifests = archive->getElements("AdditionalFiles/Manifest");
    ForEach(*manifests)
    {
        StringBuffer tempDir;
        StringBuffer manifestContents;
        StringBuffer strippedManifestContents;
        manifests->query().getProp(nullptr, manifestContents);
        const char *xml = manifestContents;
        bool isSigned = false;
        // Check for signature
        if (queryCodeSigner().hasSignature(xml))
        {
            try
            {
                StringBuffer signer;
                isSigned = queryCodeSigner().verifySignature(manifestContents, signer);
                if (!isSigned)
                    throw makeStringExceptionV(0, "Code sign verify: signature not verified");
                xml = queryCodeSigner().stripSignature(manifestContents, strippedManifestContents).str();
            }
            catch (IException *E)
            {
                StringBuffer msg;
                E->errorMessage(msg);
                auto code = E->errorCode();
                auto aud = E->errorAudience();
                E->Release();
                throw makeStringExceptionV(aud, code, "While loading manifest %s: %s", manifests->query().queryProp("@originalFileName"), msg.str());
            }
        }
        Owned<IPropertyTree> manifestSrc = createPTreeFromXMLString(xml);
        Owned<IAttributeIterator> aiter = manifestSrc->getAttributes();
        ForEach (*aiter)
            manifest->setProp(aiter->queryName(), aiter->queryValue());
        StringBuffer manifestDir;
        if (manifestSrc->hasProp("@originalFilename"))
            splitDirTail(manifestSrc->queryProp("@originalFilename"), manifestDir);

        Owned<IPropertyTreeIterator> iter = manifestSrc->getElements("*");
        ForEach(*iter)
        {
            IPropertyTree &item = iter->query();
            if (streq(item.queryName(), "Resource") && item.hasProp("@filename"))
            {
                if (!item.hasProp("@type"))
                    item.setProp("@type", "UNKNOWN");
                const char *filename;
                if (item.hasProp("@originalFilename"))
                    filename = item.queryProp("@originalFilename");
                else
                    filename = item.queryProp("@filename");
                int id;
                if (getDuplicateResourceId(item.queryProp("@type"), NULL, filename, id))
                {
                    item.setPropInt("@id", (int)id);
                    manifest->addPropTree("Resource", LINK(&item));
                }
                else
                {
                    MemoryBuffer content;
                    VStringBuffer xpath("AdditionalFiles/Resource[@originalFilename=\"%s\"]", filename);
                    const char *md5=item.queryProp("@md5");
                    if (!archive->hasProp(xpath.str()))
                    {
                        if (md5)
                            xpath.clear().appendf("AdditionalFiles/Resource[@filename='%s'][@md5='%s']", filename, md5);
                        else
                            xpath.clear().appendf("AdditionalFiles/Resource[@originalFilename=\"%s\"]", filename);
                        if (!archive->hasProp(xpath.str()))
                            throw makeStringExceptionV(0, "Failed to locate resource for %s in archive", filename);
                    }
                    archive->getPropBin(xpath.str(), content);
                    if (md5)
                    {
                        StringBuffer calculated;
                        md5_data(content, calculated);
                        if (!strieq(calculated, md5))
                            throw makeStringExceptionV(0, "MD5 mismatch %s in archive", filename);
                    }
                    const char *type = item.queryProp("@type");
                    if (strieq(type, "CPP") || strieq(type, "C"))
                    {
                        if (!ctxCallback->allowAccess("cpp", isSigned))
                            throw makeStringExceptionV(0, "Embedded code via manifest file not allowed");
                        if (!tempDir.length())
                        {
                            getTempFilePath(tempDir, "eclcc", nullptr);
                            tempDir.append(PATHSEPCHAR).append("tmp.XXXXXX"); // Note - we share same temp dir for all from this manifest
                            if (!mkdtemp((char *) tempDir.str()))
                                throw makeStringExceptionV(0, "Failed to create temporary directory %s (error %d)", tempDir.str(), errno);
                            cppInstance.addTemporaryDir(tempDir.str());
                        }
                        StringBuffer tempFileName;
                        tempFileName.append(tempDir).append(PATHSEPCHAR).append(item.queryProp("@filename"));
                        if (!recursiveCreateDirectoryForFile(tempFileName))
                            throw makeStringExceptionV(0, "Failed to create temporary file %s (error %d)", tempFileName.str(), errno);
                        FILE *source = fopen(tempFileName.str(), "wt");
                        fwrite(content.toByteArray(), content.length(), 1, source);
                        fclose(source);
                        cppInstance.useSourceFile(tempFileName, item.queryProp("@compileFlags"), true);
                    }
                    else
                    {
                        if ((strieq(type, "jar") || strieq(type, "pyzip")) && !ctxCallback->allowAccess(type, isSigned))
                            throw makeStringExceptionV(0, "Embedded %s files via manifest file not allowed", type);
                        addCompress(type, content.length(), content.toByteArray(), &item); // MORE - probably should not recompress files known to be compressed, like jar
                    }
                }
            }
            else
                manifest->addPropTree(item.queryName(), LINK(&item));
        }
    }
}

void ResourceManager::addWebServiceInfo(IPropertyTree *wsinfo)
{
    //convert legacy web service info to the new resource format
    if (wsinfo)
    {
        if (wsinfo->hasProp("SOAP"))
            ensureManifestInfo()->addProp("WS-PARAMS", wsinfo->queryProp("SOAP"));
        if (wsinfo->hasProp("HELP"))
        {
            const char *content = wsinfo->queryProp("HELP");
            addCompress("HELP", strlen(content)+1, content);
        }
        if (wsinfo->hasProp("INFO"))
        {
            const char *content = wsinfo->queryProp("INFO");
            addCompress("INFO", strlen(content)+1, content);
        }
        if (wsinfo->hasProp("OTX"))
        {
            const char *content = wsinfo->queryProp("OTX");
            addCompress("HYPER-LINK", strlen(content)+1, content);
        }
        if (wsinfo->hasProp("HTML"))
        {
            const char *content = wsinfo->queryProp("HTML");
            Owned<IPropertyTree> manifestEntry = createPTree("Resource");
            manifestEntry->setProp("@name", "Custom Form");
            addCompress("XSLT", strlen(content)+1, content, manifestEntry);
            IPropertyTree *view = ensurePTree(ensureManifestInfo(), "Views/XSLT/FORM");
            view->setProp("@resource", "Custom Form");
        }
        if (wsinfo->hasProp("HTMLD"))
        {
            const char *content = wsinfo->queryProp("HTMLD");
            Owned<IPropertyTree> manifestEntry = createPTree("Resource");
            manifestEntry->setProp("@name", "Custom HTML");
            addCompress("HTML", strlen(content)+1, content, manifestEntry);
            IPropertyTree *view = ensurePTree(ensureManifestInfo(), "Views/HTML/FORM");
            view->setProp("@resource", "Custom HTML");
        }
        if (wsinfo->hasProp("RESULT"))
        {
            const char *content = wsinfo->queryProp("RESULT");
            Owned<IPropertyTree> manifestEntry = createPTree("Resource");
            manifestEntry->setProp("@name", "Results");
            addCompress("XSLT", strlen(content)+1, content, manifestEntry);
            IPropertyTree *view = ensurePTree(ensureManifestInfo(), "Views/XSLT/RESULTS");
            view->setProp("@resource", "Results");
        }
        if (wsinfo->hasProp("ERROR"))
        {
            const char *content = wsinfo->queryProp("ERROR");
            Owned<IPropertyTree> manifestEntry = createPTree("Resource");
            manifestEntry->setProp("@name", "Error");
            addCompress("XSLT", strlen(content)+1, content, manifestEntry);
            IPropertyTree *view = ensurePTree(ensureManifestInfo(), "Views/XSLT/ERROR");
            view->setProp("@resource", "Error");
        }
    }
}

void ResourceManager::finalize()
{
    if (!finalized)
    {
        if (manifest)
        {
            StringBuffer content;
            toXML(manifest, content);
            addCompress("MANIFEST", content.length()+1, content.str(), NULL, MANIFEST_BASE, false);
        }
        finalized=true;
    }
}

void ResourceManager::putbytes(int h, const void *b, unsigned len)
{
    unsigned written = _write(h, b, len);
    assertex(written == len);
    totalbytes += len;
}

void ResourceManager::flushAsText(const char *filename)
{
    finalize();

    StringBuffer name;
    int len = strlen(filename);
    name.append(filename,0,len-4).append(".txt");

    FILE* f = fopen(name.str(), "wb");
    if (f==NULL)
    {
        IERRLOG("Create resource text file %s failed", name.str());
        return; // error is ignorable.
    }

    ForEachItemIn(idx, resources)
    {
        ResourceItem&s = (ResourceItem&)resources.item(idx);
        fwrite(s.data.get(),1,s.data.length(),f);
    }

    fclose(f);
}

bool ResourceManager::flush(StringBuffer &filename, const char *basename, bool flushText, bool target64bit)
{
    finalize();

    // Use "resources" for strings that are a bit large to generate in the c++ (some compilers had limits at 64k) 
    // or that we want to access without having to run the dll/so
    // In linux there is no .res concept but we can achieve the same effect by generating an object file with a specially-named section 
    // bintils tools can be used to extract the data externally (internally we just have a named symbol for it)
    // Alternatively we can generate an assembler file to create the equivalent object file, if binutils is not available
    bool isObjectFile = true;
#ifdef _WIN32
    filename.append(basename).append(".res");
    int h = _open(filename, _O_WRONLY|_O_CREAT|_O_TRUNC|_O_BINARY|_O_SEQUENTIAL, _S_IREAD | _S_IWRITE | _S_IEXEC);
    
    //assertex(h != HFILE_ERROR);
    if (h == HFILE_ERROR) // error can not be ignored!
        throwError1(HQLERR_ResourceCreateFailed, filename.str());

    totalbytes = 0;
    putbytes(h, "\x00\x00\x00\x00\x20\x00\x00\x00\xff\xff\x00\x00\xff\xff\x00\x00"
                "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", 0x20);

    MemoryBuffer temp;
    ForEachItemIn(idx, resources)
    {
        ResourceItem&s = static_cast<ResourceItem&>(resources.item(idx));
        __int32 len = s.data.length();
        unsigned lenType = strlen(s.type);
        unsigned sizeType = (lenType+1)*2;
        unsigned sizeTypeName = (sizeType + 4);
        unsigned packedSizeTypeName = ((sizeTypeName + 2) & ~3);
        __int32 lenHeader = 4 + 4 + packedSizeTypeName + 4 + 2 + 2 + 4 + 4;
        unsigned short id = s.id;
        temp.clear();
        temp.append(sizeof(len), &len);
        temp.append(sizeof(lenHeader), &lenHeader);
        for (unsigned i=0; i < lenType; i++)
            temp.append((byte)s.type[i]).append((byte)0);
        temp.append((byte)0).append((byte)0);
        temp.append((byte)0xff).append((byte)0xff);
        temp.append(sizeof(id), &id);
        if (temp.length() & 2)
            temp.append((byte)0).append((byte)0);
        temp.append(4, "\x00\x00\x00\x00"); // version
        temp.append(12, "\x30\x10\x09\x04\x00\x00\x00\x00\x00\x00\x00\x00");    // 0x1030 memory 0x0409 language
        assertex(lenHeader == temp.length());

        putbytes(h, temp.bufferBase(), lenHeader);
        putbytes(h, s.data.get(), len);
        if (totalbytes & 3)
            putbytes(h, "\x00\x00\x00",4-(totalbytes & 3));
    }
    _close(h);
#else
    isObjectFile = false;
    filename.append(basename).append(".res.s");
    FILE *f = fopen(filename, "wt");
    if (!f)
        throwError1(HQLERR_ResourceCreateFailed, filename.str());

    //MORE: This should really use targetCompiler instead
#if defined(__APPLE__)
    const bool generateClang = true;
#else
    const bool generateClang = false;
#endif
    ForEachItemIn(idx, resources)
    {
        ResourceItem &s = (ResourceItem &) resources.item(idx);
        const char *type = s.type.str();
        unsigned id = s.id;
        VStringBuffer binfile("%s_%s_%u.bin", filename.str(), type, id);
        VStringBuffer label("%s_%u_txt_start", type, id);
        if (generateClang)
        {
#ifdef __APPLE__
            if (id <= 1200)  // There is a limit of 255 sections before linker complains - and some are used elsewhere
#endif
                fprintf(f, " .section __TEXT,%s_%u\n", type, id);
            fprintf(f, " .global _%s\n", label.str());  // For some reason apple needs a leading underbar and linux does not
            fprintf(f, "_%s:\n", label.str());
        }
        else
        {
#if defined(__linux__) && defined(__GNUC__) && defined(__arm__)
            fprintf(f, " .section .note.GNU-stack,\"\",%%progbits\n");   // Prevent the stack from being marked as executable
#else
            fprintf(f, " .section .note.GNU-stack,\"\",@progbits\n");   // Prevent the stack from being marked as executable
#endif
            fprintf(f, " .section %s_%u,\"a\"\n", type, id);
            fprintf(f, " .global %s\n", label.str());
            fprintf(f, " .type %s,STT_OBJECT\n", label.str());
            fprintf(f, "%s:\n", label.str());
        }
        fprintf(f, " .incbin \"%s\"\n", binfile.str());
        FILE *bin = fopen(binfile, "wb");
        if (!bin)
        {
            fclose(f);
            throwError1(HQLERR_ResourceCreateFailed, binfile.str());
        }
        fwrite(s.data.get(), 1, s.data.length(), bin);
        fclose(bin);
    }
    fclose(f);
#endif
    if (flushText)
        flushAsText(filename);
    return isObjectFile;
}


bool ResourceManager::queryWriteText(StringBuffer & resTextName, const char * filename)
{
    int len = strlen(filename);
    resTextName.append(filename,0,len-4).append(".txt");
    return true;
}


#if 0
int test()
{
    ResourceManager r;
    r.add("Hello there!2");
    r.add("Hello again");
    r.flush("c:\\t2.res");
    return 6;
}

static int dummy = test();
#endif
