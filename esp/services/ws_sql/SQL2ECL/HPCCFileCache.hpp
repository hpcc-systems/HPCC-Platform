/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

#ifndef HPCCFILECACHE_HPP_
#define HPCCFILECACHE_HPP_

#include "ws_sql.hpp"
#include "ws_sql_esp.ipp"
#include "dadfs.hpp"
#include "dasess.hpp"
#include "HPCCFile.hpp"
#include "SQLTable.hpp"
#include "fileview.hpp"
#include "fvrelate.hpp"

typedef HPCCFile * HPCCFilePtr;
typedef MapStringTo<HPCCFilePtr> HpccFiles;

class HPCCFileCache : public CInterface, public IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    static HPCCFileCache * createFileCache(const char * username, const char * passwd);
    bool cacheAllHpccFiles(const char * filterby);
    bool fetchHpccFilesByTableName(IArrayOf<SQLTable> * sqltables);
    static bool updateHpccFileDescription(const char * filename, const char * user, const char * pass, const char * description);
    HPCCFile * fetchHpccFileByName(IUserDescriptor *user, const char * filename, bool namevalidated, bool acceptrawfiles=false);
    static HPCCFile * fetchHpccFileByName(const char * filename, const char * user, const char * pass, bool namevalidated, bool acceptrawfiles);
    const char * cacheHpccFileByName(const char * filename, bool namevalidated = false);
    bool isHpccFileCached(const char * filename);
    HPCCFilePtr getHpccFileByName(const char * filename);
    bool populateTablesResponse(IEspGetDBMetaDataResponse & tablesrespstruct, const char * filterby, bool nestedcolumns);

    HPCCFileCache(const char * username, const char * passwd)
    {
        userdesc.setown(createUserDescriptor());
        userdesc->set(username, passwd);
    }

    virtual ~HPCCFileCache()
    {
        HashIterator sIter(cache);
        for(sIter.first();sIter.isValid();sIter.next())
        {
            HPCCFilePtr a = *cache.mapToValue(&sIter.query());
            delete a;
        }
#ifdef _DEBUG
        fprintf(stderr, "Leaving filecache");
#endif
    }

private:

    Owned<IUserDescriptor> userdesc;
    HpccFiles cache;
};

#endif /* HPCCFILECACHE_HPP_ */
