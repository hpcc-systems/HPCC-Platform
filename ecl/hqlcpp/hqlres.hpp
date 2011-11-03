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
    void addManifest(IPropertyTree *manifestSrc, const char *dir);
    void addManifestFromArchive(IPropertyTree *archive);
    void addResourceIncludes(IPropertyTree *includes);
    void addWebServiceInfo(IPropertyTree *wsinfo);
    IPropertyTree *ensureManifestInfo(){if (!manifest) manifest.setown(createPTree("Manifest")); return manifest;}
    bool getDuplicateResourceId(const char *srctype, const char *filename, int &id);
    void finalize();

    unsigned count();
    void flush(const char *filename, bool flushText, bool target64bit);
    void flushAsText(const char *filename);
    bool queryWriteText(StringBuffer & resTextName, const char * filename);
private:
    void putbytes(int h, const void *b, unsigned len);
};

#endif

