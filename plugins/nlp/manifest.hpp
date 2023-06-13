#include "jlib.hpp"
#include "eclhelper.hpp"

#include <memory>

class IManifest
{
public:
    virtual const char *extractResources(StringBuffer &buff) const = 0; // Returns the location of the files.
    virtual bool getResourceData(const char *partialPath, MemoryBuffer &mb) const = 0;
};

std::shared_ptr<IManifest> createIManifest(ICodeContext *ctx);