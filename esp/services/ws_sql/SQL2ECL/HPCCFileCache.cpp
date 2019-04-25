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

#include "HPCCFileCache.hpp"


HPCCFileCache * HPCCFileCache::createFileCache(const char * username, const char * passwd)
{
    ESPLOG(LogMax, "WsSQL: Creating new HPCC FILE CACHE");
    return new HPCCFileCache(username,passwd);
}

bool HPCCFileCache::populateTablesResponse(IEspGetDBMetaDataResponse & tablesrespstruct, const char * filterby)
{
    bool success = false;

    cacheAllHpccFiles(filterby);
    IArrayOf<IEspHPCCTable> tables;

    HashIterator iterHash(cache);
    ForEach(iterHash)
    {
       const char* key = (const char*)iterHash.query().getKey();
       HPCCFilePtr file = dynamic_cast<HPCCFile *>(*cache.getValue(key));
       if ( file )
       {
           Owned<IEspHPCCTable> pTable = createHPCCTable();
           pTable->setName(file->getFullname());
           pTable->setFormat(file->getFormat());
           const char * ecl = file->getEcl();

           if (!ecl || !*ecl)
               continue;
           else
           {
               pTable->setDescription(file->getDescription());
               pTable->setIsKeyed(file->isFileKeyed());
               pTable->setIsSuper(file->isFileSuper());
               pTable->setOwner(file->getOwner());

               IArrayOf<IEspHPCCColumn> pColumns;

               IArrayOf<HPCCColumnMetaData> * cols = file->getColumns();
               for (int i = 0; i < cols->length(); i++)
               {
                   Owned<IEspHPCCColumn> pCol = createHPCCColumn();
                   HPCCColumnMetaData currcol = cols->item(i);
                   pCol->setName(currcol.getColumnName());
                   pCol->setType(currcol.getColumnType());
                   pColumns.append(*pCol.getLink());
               }
               pTable->setColumns(pColumns);
               tables.append(*pTable.getLink());
           }
        }
     }

     tablesrespstruct.setTables(tables);
     return success;
 }

bool HPCCFileCache::fetchHpccFilesByTableName(IArrayOf<SQLTable> * sqltables)
{
    bool allFound = true;

    ForEachItemIn(tableindex, *sqltables)
    {
       SQLTable table = sqltables->item(tableindex);
       const char * cachedKey = cacheHpccFileByName(table.getName());
       allFound &= (cachedKey && *cachedKey);
    }

    return allFound;
}

bool HPCCFileCache::cacheAllHpccFiles(const char * filterby)
{
    bool success = false;
    StringBuffer filter;
    if(filterby && *filterby)
        filter.append(filterby);
    else
        filter.append("*");

    Owned<IDFAttributesIterator> fi = queryDistributedFileDirectory().getDFAttributesIterator(filter, userdesc.get(), true, true, NULL);
    if(!fi)
        throw MakeStringException(-1,"Cannot get information from file system.");

    success = true;
    ForEach(*fi)
    {
       IPropertyTree &attr=fi->query();

#if defined(_DEBUG)
    StringBuffer toxml;
    toXML(&attr, toxml);
    fprintf(stderr, "%s", toxml.str());
#endif

       StringBuffer name(attr.queryProp("@name"));

       if (name.length()>0 && HPCCFile::validateFileName(name.str()))
       //if (name.length()>0)
       {
           const char * cachedKey = cacheHpccFileByName(name.str(), true);
           success &= (cachedKey && *cachedKey);
       }
    }

    return success;
}
bool HPCCFileCache::updateHpccFileDescription(const char * filename, const char * user, const char * pass, const char * description)
{
    Owned<IUserDescriptor> userdesc;
    try
    {
        userdesc.setown(createUserDescriptor());
        userdesc->set(user, pass);

        //queryDistributedFileDirectory returns singleton
        IDistributedFileDirectory & dfd = queryDistributedFileDirectory();
        Owned<IDistributedFile> df = dfd.lookup(filename, userdesc);

        if(!df)
            return false;

        DistributedFilePropertyLock lock(df);
        lock.queryAttributes().setProp("@description",description);
    }
    catch (...)
    {
        return false;
    }

    return true;
}


HPCCFile * HPCCFileCache::fetchHpccFileByName(IUserDescriptor *user, const char * filename, bool namevalidated, bool acceptrawfiles)
{
    StringBuffer username;
    user->getUserName(username);
    StringBuffer password;
    user->getPassword(password);

    return HPCCFileCache::fetchHpccFileByName(filename, username.str(), password.str(), namevalidated, acceptrawfiles);
}

HPCCFile * HPCCFileCache::fetchHpccFileByName(const char * filename, const char * user, const char * pass, bool namevalidated, bool acceptrawfiles)
{
    Owned<HPCCFile> file;
    Owned<IUserDescriptor> userdesc;
    try
    {
        userdesc.setown(createUserDescriptor());
        userdesc->set(user, pass);

        //queryDistributedFileDirectory returns singleton
        IDistributedFileDirectory & dfd = queryDistributedFileDirectory();
        Owned<IDistributedFile> df = dfd.lookup(filename, userdesc);

        if(!df)
            throw MakeStringException(-1,"Cannot find file %s.",filename);

        const char* lname=df->queryLogicalName();
        if (lname && *lname)
        {
            const char* fname=strrchr(lname,':');
            if (!namevalidated && !HPCCFile::validateFileName(lname))
                throw MakeStringException(-1,"Invalid SQL file name detected %s.", fname);
            file.setown(HPCCFile::createHPCCFile());
            file->setFullname(lname);
            file->setName(fname ? fname+1 : lname);
        }
        else
            throw MakeStringException(-1,"Cannot find file %s.",filename);

        file->setDescription(df->queryAttributes().queryProp("@description"));

        //Do we care about the clusters??
        //StringArray clusters;
        //if (cluster && *cluster)
        //{
        //df->getClusterNames(clusters);
        //if(!FindInStringArray(clusters, cluster))
            //throw MakeStringException(ECLWATCH_FILE_NOT_EXIST,"Cannot find file %s.",fname);
        //}

    #if defined(_DEBUG)
        StringBuffer atttree;
        toXML(&df->queryAttributes(), atttree, 0);
        fprintf(stderr, "%s", atttree.str());
    #endif

        IPropertyTree & properties = df->queryAttributes();

        if(properties.hasProp("ECL"))
            file->setEcl(properties.queryProp("ECL"));
        else if (!acceptrawfiles)
            throw MakeStringException(-1,"File %s does not contain required ECL record layout.",filename);

        file->setOwner(properties.queryProp("@owner"));
        IDistributedSuperFile *sf = df->querySuperFile();
        if(sf)
        {
            file->setIsSuperfile(true);
        }

        //unfortunately @format sometimes holds the file format, sometimes @kind does
        const char * kind = properties.queryProp("@kind");
        if (kind)
        {
            file->setFormat(kind);
            if ((stricmp(kind, "key") == 0))
            {
                file->setIsKeyedFile(true);

                ISecManager *secmgr = NULL;
                ISecUser *secuser = NULL;
                StringBuffer username;
                StringBuffer passwd;
                userdesc->getUserName(username);
                userdesc->getPassword(passwd);

                Owned<IResultSetFactory> resultSetFactory = getSecResultSetFactory(secmgr, secuser, username.str(), passwd.str());
                Owned<INewResultSet> result;
                try
                {
                    result.setown(resultSetFactory->createNewFileResultSet(filename, NULL));

                    if (result)
                    {
                        Owned<IResultSetCursor> cursor = result->createCursor();
                        const IResultSetMetaData & meta = cursor->queryResultSet()->getMetaData();
                        int columnCount = meta.getColumnCount();
                        int keyedColumnCount = meta.getNumKeyedColumns();

                        for (int i = 0; i < keyedColumnCount; i++)
                        {
                            SCMStringBuffer columnLabel;
                            if (meta.hasSetTranslation(i))
                            {
                                meta.getNaturalColumnLabel(columnLabel, i);
                            }

                            if (columnLabel.length() < 1)
                            {
                                meta.getColumnLabel(columnLabel, i);
                            }

                            file->setKeyedColumn(columnLabel.str());
                        }
                    }
                }
                catch (IException * se)
                {
                    StringBuffer s;
                    se->errorMessage(s);
                    IERRLOG("Error fetching keyed file %s info: %s", filename, s.str());
                    se->Release();
                    if (file)
                        file.clear();
                }
            }
        }
        else
        {
            //@format - what format the file is (if not fixed width)
            const char* format = properties.queryProp("@format");
            file->setFormat(format);
        }

        if (file && ( file->containsNestedColumns() || strncmp(file->getFormat(), "XML", 3)==0))
            throw MakeStringException(-1,"Nested data files not supported: %s.",filename);
    }

    catch (IException * se)
    {
        StringBuffer s;
        se->errorMessage(s);
        IERRLOG("Error fetching file %s info: %s", filename, s.str());
        se->Release();
        if (file)
            file.clear();
    }

    catch (...)
    {
        if (file)
            file.clear();
    }

    if (file)
        return file.getLink();
    else
        return NULL;
}

const char * HPCCFileCache::cacheHpccFileByName(const char * filename, bool namevalidated)
{
    if(cache.getValue(filename))
        return filename;

    Owned<HPCCFile> file;

    try
    {
        file.setown(HPCCFileCache::fetchHpccFileByName(userdesc, filename, namevalidated, false));
    }
    catch (...)
    {
        if (file)
            file.clear();
    }

    if (file)
    {
        cache.setValue(file->getFullname(), file.getLink());
        return file->getFullname();
    }

    return "";
}

HPCCFilePtr HPCCFileCache::getHpccFileByName(const char * filename)
{
    HPCCFilePtr * hpccfile = cache.getValue(filename);

    if (hpccfile)
        return *hpccfile;

    return NULL;
}

bool HPCCFileCache::isHpccFileCached(const char * filename)
{
    return cache.getValue(filename) != NULL;
}
