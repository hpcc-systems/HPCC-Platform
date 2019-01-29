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
#ifndef _HQLRES_INCL
#define _HQLRES_INCL
#include "jarray.hpp"

class ResourceManager
{
    unsigned nextmfid;
    unsigned nextbsid;
    unsigned totalbytes;
    CIArray resources;
    Owned<IPropertyTree> manifest;
    bool finalized;
public:
    ResourceManager();
    unsigned addString(unsigned len, const char *data);
    void addNamed(const char * type, unsigned len, const void *data, IPropertyTree *entry=NULL, unsigned id=(unsigned)-1, bool addToManifest=true, bool compressed=false);
    bool addCompress(const char * type, unsigned len, const void *data, IPropertyTree *entry=NULL, unsigned id=(unsigned)-1, bool addToManifest=true);
    void addManifest(const char *filename);
    void addManifestsFromArchive(IPropertyTree *archive);
    void addWebServiceInfo(IPropertyTree *wsinfo);
    IPropertyTree *ensureManifestInfo(){if (!manifest) manifest.setown(createPTree("Manifest")); return manifest;}
    bool getDuplicateResourceId(const char *srctype, const char *respath, const char *filename, int &id);
    void finalize();

    unsigned count();
    bool flush(StringBuffer &filename, const char *basename, bool flushText, bool target64bit);
    void flushAsText(const char *filename);
    bool queryWriteText(StringBuffer & resTextName, const char * filename);
private:
    void putbytes(int h, const void *b, unsigned len);
    void addManifestFile(const char *filename);
    void addManifestInclude(IPropertyTree &include, const char *dir);
};

#endif

