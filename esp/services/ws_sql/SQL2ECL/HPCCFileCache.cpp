#include "HPCCFileCache.hpp"


HPCCFileCache * HPCCFileCache::createFileCache(const char * username, const char * passwd)
{
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
               pTable->setIsKeyed(file->isFileKeyed());
               pTable->setIsSuper(file->isFileSuper());

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

bool HPCCFileCache::fetchHpccFilesByTableName(IArrayOf<SQLTable> * sqltables, HpccFiles * hpccfilecache)
{
    bool allFound = true;

    ForEachItemIn(tableindex, *sqltables)
    {
       SQLTable table = sqltables->item(tableindex);
       allFound &= cacheHpccFileByName(table.getName());
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

    Owned<IDFAttributesIterator> fi = queryDistributedFileDirectory().getDFAttributesIterator(filter, userdesc.get(), true, false, NULL);
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

       if (name.length()>0)
           cacheHpccFileByName(name.str());
    }

    return success;
}

bool HPCCFileCache::cacheHpccFileByName(const char * filename)
{
    if(cache.getValue(filename))
        return true;

    IDistributedFileDirectory & dfd = queryDistributedFileDirectory();
    Owned<IDistributedFile> df = dfd.lookup(filename, userdesc);

    if(!df)
        throw MakeStringException(-1,"Cannot find file %s.",filename);

    Owned<HPCCFile> file =  HPCCFile::createHPCCFile();
    const char* lname=df->queryLogicalName(), *fname=strrchr(lname,':');
    file->setFullname(lname);
    file->setName(fname ? fname+1 : lname);

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

    //TODO extract index info if present
    //StringBuffer strDesc = properties.queryProp("@description");
    //if (strDesc.length() > 0 )
    //{
    //    fprintf(stderr, ">>>%s", strDesc.str());
    //}

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
            //if (m_clusterName.length() > 0)
            //{
            //    result.setown(resultSetFactory->createNewFileResultSet(logicalNameStr.str(), m_clusterName.str()));
            //}
            //else
            //{
                result.setown(resultSetFactory->createNewFileResultSet(filename, NULL));
            //}

            Owned<IResultSetCursor> cursor = result->createCursor();
            const IResultSetMetaData & meta = cursor->queryResultSet()->getMetaData();
            int columnCount = meta.getColumnCount();
            int keyedColumnCount = meta.getNumKeyedColumns();

//            for (int i = keyedColumnCount-1; i >= 0 ; i--)
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
    else
    {
        //@format - what format the file is (if not fixed width)
        const char* format = properties.queryProp("@format");
        file->setFormat(format);
    }

    cache.setValue(file->getFullname(), file.getLink());

    return true;
}

HPCCFilePtr HPCCFileCache::getHpccFileByName(const char * filename)
{
    return *(cache.getValue(filename));
}

bool HPCCFileCache::isHpccFileCached(const char * filename)
{
    return cache.getValue(filename) != NULL;
}
