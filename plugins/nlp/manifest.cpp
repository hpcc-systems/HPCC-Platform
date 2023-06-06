#include "manifest.hpp"

#include "workunit.hpp"
#include "thorplugin.hpp"
#include "jfile.hpp"

#include <mutex>
#include <iostream>
#include <fstream>

std::shared_ptr<IManifest> instance;
std::once_flag initFlag;

class Manifest : public IManifest
{
protected:
    Linked<IConstWorkUnit> cw;
    Owned<ILoadedDllEntry> dll;
    Owned<IPropertyTree> manifest;
    StringBuffer manifestDir;

    static IConstWorkUnit *getWorkunit(ICodeContext *ctx)
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        StringAttr wuid;
        wuid.setown(ctx->getWuid());
        return factory->openWorkUnit(wuid);
    }

    Manifest(ICodeContext *ctx) : cw(getWorkunit(ctx))
    {
        DBGLOG("MANIFEST:  Constructor");
        Owned<IConstWUQuery> q = cw->getQuery();
        if (q)
        {
            SCMStringBuffer dllname;
            q->getQueryDllName(dllname);
            if (dllname.length())
            {
                try
                {
                    dll.setown(queryDllServer().loadDllResources(dllname.str(), DllLocationAnywhere));
                    StringBuffer xml;
                    manifest.setown(getEmbeddedManifestXML(dll, xml) ? createPTreeFromXMLString(xml.str()) : createPTree());
                    manifestDir.set(manifest->queryProp("@manifestDir"));
                }
                catch (IException *e)
                {
                    VStringBuffer msg("Failed to load %s", dllname.str());
                    EXCLOG(e, msg.str());
                    e->Release();
                }
                catch (...)
                {
                    DBGLOG("Failed to load %s", dllname.str());
                }
            }
        }

        if (!manifest)
        {
            DBGLOG("Failed to initialize nlp manifest");
            manifest.setown(createPTree());
        }
    }

public:
    Manifest(const Manifest &) = delete;
    Manifest &operator=(const Manifest &) = delete;
    ~Manifest()
    {
        DBGLOG("MANIFEST:  Destructor");
    }

    static std::shared_ptr<IManifest> getInstance(ICodeContext *ctx)
    {
        std::call_once(initFlag, [ctx]()
                       { instance = std::shared_ptr<Manifest>(new Manifest(ctx)); });
        return instance;
    }

    const char *stripLeadingCharacters(const char *input) const
    {
        while (*input == '.' || *input == '/' || *input == '\\')
            ++input;
        return input;
    }

    StringBuffer &makeResourcePath(const char *path, const char *basedir, StringBuffer &respath) const
    {
        StringBuffer abspath;
        makeAbsolutePath(path, basedir, abspath);
        return makePathUniversal(abspath, respath);
    }

    bool getResourceData(IPropertyTree *res, size32_t &len, const void *&data) const
    {
        if (res->hasProp("@id") && (res->hasProp("@header") || res->hasProp("@compressed")))
        {
            int id = res->getPropInt("@id");
            return (dll->getResource(len, data, res->queryProp("@type"), (unsigned)id) && len > 0);
        }
        return false;
    }

    bool getResourceData(IPropertyTree *res, MemoryBuffer &content) const
    {
        size32_t len = 0;
        const void *data = NULL;
        if (getResourceData(res, len, data))
        {
            if (res->getPropBool("@compressed"))
                decompressResource(len, data, content);
            else
                content.append(len, (const char *)data);
            return true;
        }
        return false;
    }

    IPropertyTree *queryResourcePT(const char *_path) const
    {
        const char *path = stripLeadingCharacters(_path);
        StringBuffer xpath;
        if (!manifestDir.length())
            xpath.setf("Resource[@filename='%s'][1]", path);
        else
        {
            StringBuffer respath;
            makeResourcePath(path, manifestDir.str(), respath);
            xpath.setf("Resource[@resourcePath='%s'][1]", respath.str());
        }

        return manifest->queryPropTree(xpath.str());
    }

    bool ends_with(const std::string &str, const std::string &suffix) const
    {
        if (suffix.size() > str.size())
            return false;
        return std::equal(suffix.rbegin(), suffix.rend(), str.rbegin());
    }

    //  --- IManifest ---
    virtual const char *extractResources(StringBuffer &sb) const
    {
        // dll->queryManifestFiles extracts the manifest files to a tmp folder the paths as a StringArray
        const StringArray &resFiles = dll->queryManifestFiles("UNKNOWN", "nlp");
        if (resFiles.length() > 0)
        {
            std::string fullPath = resFiles.item(0);
            const char *const sig = ".tmp/nlp";
            std::size_t tmpPos = fullPath.find(sig);
            if (tmpPos != std::string::npos)
            {
                sb.append(fullPath.substr(0, tmpPos + strlen(sig)).c_str());
            }
        }
        return sb.str();
    }

    virtual bool getResourceData(const char *partialPath, MemoryBuffer &mb) const
    {
        Linked<IPropertyTree> res = queryResourcePT(partialPath);
        if (!res)
            return false;
        return getResourceData(res, mb);
    }
};

std::shared_ptr<IManifest> createIManifest(ICodeContext *ctx)
{
    return Manifest::getInstance(ctx);
}
