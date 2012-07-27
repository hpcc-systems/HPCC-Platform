package com.hpccsystems.jdbcdriver;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

public class DFUFile
{
    private String              Prefix;
    private String              ClusterName;
    private String              Directory;
    private String              Description;
    private int                 Parts;
    private String              FullyQualifiedName;
    private String              FileName;
    private String              Owner;
    private long                TotalSize;
    private long                RecordCount;
    private String              Modified;
    private long                LongSize;
    private long                LongRecordCount;
    private boolean             isSuperFile;
    private boolean             isZipFile;
    private boolean             isDirectory;
    private int                 Replicate;
    private int                 IntSize;
    private int                 IntRecordCount;
    private boolean             FromRoxieCluster;
    private boolean             BrowseData;
    private boolean             IsKeyFile;
    private Properties          Fields;
    private String              Format;
    private String              CsvSeparate;
    private String              CsvTerminate;
    private String              CsvQuote;
    private String              Ecl;
    private Properties          KeyedColumns;
    private Properties          NonKeyedColumns;
    private List<String>        relatedIndexes;
    private List<String>        subFiles;
    private String              IdxFilePosField;
    private boolean             HasPayLoad;

    private final static String RELATEDINDEXKEYWORD = "XDBC:RelIndexes";
    private final static String IDXFILEPOSFIELDTAG  = "XDBC:PosField";

    public DFUFile(String prefix, String clusterName, String filename)
    {
        super();
        Prefix = prefix;
        ClusterName = clusterName;
        FileName = filename;
        FullyQualifiedName = filename;

        Directory = null;
        Description = null;
        Parts = -1;
        Owner = null;
        TotalSize = -1;
        RecordCount = -1;
        Modified = null;
        LongSize = -1;
        LongRecordCount = -1;
        isSuperFile = false;
        isZipFile = false;
        isDirectory = false;
        Replicate = -1;
        IntSize = -1;
        IntRecordCount = -1;
        FromRoxieCluster = false;
        BrowseData = false;
        IsKeyFile = false;
        Format = "FLAT";
        CsvSeparate = null;
        CsvTerminate = null;
        CsvQuote = null;
        Ecl = null;
        Fields = new Properties();
        KeyedColumns = new Properties();
        NonKeyedColumns = new Properties();
        relatedIndexes = null;
        IdxFilePosField = null;
        HasPayLoad = false;
        subFiles = null;
    }

    public DFUFile(String prefix, String clusterName, String directory, String description, int parts, String filename,
            String fullyqualifiedname, String owner, long totalSize, long recordCount, String modified, long longSize,
            long longRecordCount, boolean isSuperFile, boolean isZipFile, boolean isDirectory, int replicate,
            int intSize, int intRecordCount, boolean fromRoxieCluster, boolean browseData, boolean isKeyFile,
            String format, String csvseparate, String csvterminate, String csvquote, String ecl)
    {
        Prefix = prefix;
        ClusterName = clusterName;
        Directory = directory;
        Description = description;
        Parts = parts;
        FileName = filename;
        FullyQualifiedName = fullyqualifiedname;
        Owner = owner;
        TotalSize = totalSize;
        RecordCount = recordCount;
        Modified = modified;
        LongSize = longSize;
        LongRecordCount = longRecordCount;
        this.isSuperFile = isSuperFile;
        this.isZipFile = isZipFile;
        this.isDirectory = isDirectory;
        Replicate = replicate;
        IntSize = intSize;
        IntRecordCount = intRecordCount;
        FromRoxieCluster = fromRoxieCluster;
        BrowseData = browseData;
        IsKeyFile = isKeyFile;
        Fields = new Properties();
        Format = format;
        CsvSeparate = csvseparate;
        CsvTerminate = csvterminate;
        CsvQuote = csvquote;
        KeyedColumns = new Properties();
        NonKeyedColumns = new Properties();

        if (ecl != null && ecl.length() > 0)
        {
            Ecl = ecl;
            setFileRecDef(ecl);
        }
        relatedIndexes = null;
        IdxFilePosField = null;
        HasPayLoad = false;
        subFiles = null;
    }

    public DFUFile()
    {
        Prefix = null;
        ClusterName = null;
        Directory = null;
        Description = null;
        Parts = -1;
        FullyQualifiedName = null;
        FileName = null;
        Owner = null;
        TotalSize = -1;
        RecordCount = -1;
        Modified = null;
        LongSize = -1;
        LongRecordCount = -1;
        this.isSuperFile = false;
        this.isZipFile = false;
        this.isDirectory = false;
        Replicate = -1;
        IntSize = -1;
        IntRecordCount = -1;
        FromRoxieCluster = false;
        BrowseData = false;
        IsKeyFile = false;
        Fields = new Properties();
        Format = "FLAT";
        CsvSeparate = null;
        CsvTerminate = null;
        CsvQuote = null;
        Ecl = null;
        KeyedColumns = new Properties();
        NonKeyedColumns = new Properties();
        relatedIndexes = null;
        IdxFilePosField = null;
        HasPayLoad = false;
        subFiles = null;
    }

    public String getFileName()
    {
        return FileName;
    }

    public void setFileName(String fileName)
    {
        FileName = fileName;
    }

    public String getPrefix()
    {
        return Prefix;
    }

    public void setPrefix(String prefix)
    {
        Prefix = prefix;
    }

    public String getClusterName()
    {
        return ClusterName;
    }

    public void setClusterName(String clusterName)
    {
        ClusterName = clusterName;
    }

    public String getDirectory()
    {
        return Directory;
    }

    public void setDirectory(String directory)
    {
        Directory = directory;
    }

    public String getDescription()
    {
        return Description;
    }

    public void setDescription(String description)
    {
        Description = description;

        if (description.contains(RELATEDINDEXKEYWORD))
            setRelatedIndexes(description.substring(description.indexOf(RELATEDINDEXKEYWORD)));

        if (description.contains(IDXFILEPOSFIELDTAG))
            setIdxFilePosField(description.substring(description.indexOf(IDXFILEPOSFIELDTAG)));
    }

    private void setIdxFilePosField(String str)
    {
        IdxFilePosField = str.substring(IDXFILEPOSFIELDTAG.length() + 1 + 1, str.indexOf(']'));
    }

    public String getIdxFilePosField()
    {
        return IdxFilePosField != null ? IdxFilePosField : getLastNonKeyedNumericField();
    }

    public boolean hasValidIdxFilePosField()
    {
        String tmp = getIdxFilePosField();
        return tmp != null && tmp.length() > 0 ? true : false;
    }

    private String getLastNonKeyedNumericField()
    {
        // TODO get numeric field
        return (String) NonKeyedColumns.get(NonKeyedColumns.size());
    }

    private void setRelatedIndexes(String str)
    {
        String indexes = str.substring(RELATEDINDEXKEYWORD.length() + 1 + 1, str.indexOf(']'));
        StringTokenizer indexeToks = new StringTokenizer(indexes, ";");

        while (indexeToks.hasMoreTokens())
        {
            addRelatedIndex(indexeToks.nextToken().trim());
        }
    }

    public int getParts()
    {
        return Parts;
    }

    public void setParts(int parts)
    {
        Parts = parts;
    }

    public String getFullyQualifiedName()
    {
        return FullyQualifiedName;
    }

    public void setFullyQualifiedName(String name)
    {
        FullyQualifiedName = name;
    }

    public String getOwner()
    {
        return Owner;
    }

    public void setOwner(String owner)
    {
        Owner = owner;
    }

    public long getTotalSize()
    {
        return TotalSize;
    }

    public void setTotalSize(long totalSize)
    {
        TotalSize = totalSize;
    }

    public long getRecordCount()
    {
        return RecordCount;
    }

    public void setRecordCount(long recordCount)
    {
        RecordCount = recordCount;
    }

    public String getModified()
    {
        return Modified;
    }

    public void setModified(String modified)
    {
        Modified = modified;
    }

    public long getLongSize()
    {
        return LongSize;
    }

    public void setLongSize(long longSize)
    {
        LongSize = longSize;
    }

    public long getLongRecordCount()
    {
        return LongRecordCount;
    }

    public void setLongRecordCount(long longRecordCount)
    {
        LongRecordCount = longRecordCount;
    }

    public boolean isSuperFile()
    {
        return isSuperFile;
    }

    public void setSuperFile(boolean isSuperFile)
    {
        this.isSuperFile = isSuperFile;
    }

    public boolean isZipFile()
    {
        return isZipFile;
    }

    public void setZipFile(boolean isZipFile)
    {
        this.isZipFile = isZipFile;
    }

    public boolean isDirectory()
    {
        return isDirectory;
    }

    public void setDirectory(boolean isDirectory)
    {
        this.isDirectory = isDirectory;
    }

    public int getReplicate()
    {
        return Replicate;
    }

    public void setReplicate(int replicate)
    {
        Replicate = replicate;
    }

    public int getIntSize()
    {
        return IntSize;
    }

    public void setIntSize(int intSize)
    {
        IntSize = intSize;
    }

    public int getIntRecordCount()
    {
        return IntRecordCount;
    }

    public void setIntRecordCount(int intRecordCount)
    {
        IntRecordCount = intRecordCount;
    }

    public boolean isFromRoxieCluster()
    {
        return FromRoxieCluster;
    }

    public void setFromRoxieCluster(boolean fromRoxieCluster)
    {
        FromRoxieCluster = fromRoxieCluster;
    }

    public boolean isBrowseData()
    {
        return BrowseData;
    }

    public void setBrowseData(boolean browseData)
    {
        BrowseData = browseData;
    }

    public boolean isKeyFile()
    {
        return IsKeyFile;
    }

    public void setIsKeyFile(boolean isKeyFile)
    {
        IsKeyFile = isKeyFile;
    }

    public void setFileFields(String eclString)
    {
        Ecl = "";
        if (eclString != null && eclString.length() > 0)
        {
            try
            {
                StringTokenizer commatokens = null;
                // ECL RECORD can be defined as { type name,...,type name}; or
                // RECORD type name;...;type name;END;
                // TODO we should handle nested file types
                if (eclString.toUpperCase().contains("RECORD"))
                {
                    String tmp = eclString.substring(eclString.indexOf("RECORD") + 6, eclString.indexOf("END"));
                    commatokens = new StringTokenizer(tmp, ";");
                }
                else if (eclString.contains("{"))
                {
                    String tmp = eclString.substring(eclString.indexOf('{') + 1, eclString.indexOf('}'));
                    commatokens = new StringTokenizer(tmp, ",");
                }

                if (commatokens != null)
                {
                    int index = 0;
                    while (commatokens.hasMoreTokens())
                    {
                        String commatoken = commatokens.nextToken().trim();
                        String spacesplit[] = commatoken.split("\\s+");

                        String name = spacesplit[spacesplit.length - 1];
                        if (name.length() > 0)
                        {
                            StringBuffer type = new StringBuffer();
                            for (int i = 0; i < spacesplit.length - 1; i++)
                            {
                                type.append(spacesplit[i]);
                                if (i + 1 < spacesplit.length - 1)
                                    type.append(" ");
                            }

                            HPCCColumnMetaData columnmeta = new HPCCColumnMetaData(name, index,
                                    HPCCDatabaseMetaData.convertECLtype2SQLtype(type.toString().toUpperCase()));
                            columnmeta.setEclType(type.toString());
                            columnmeta.setTableName(this.FullyQualifiedName);

                            Ecl += type + " " + name + "; ";
                            Fields.put(name.toUpperCase(), columnmeta);

                            index++;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                System.out.println("Invalid ECL Record definition found in " + this.getFullyQualifiedName()
                        + " details.");
                return;
            }
        }
    }

    public Enumeration<Object> getAllFields()
    {
        return Fields.elements();
    }

    public Properties getAllFieldsProps()
    {
        return Fields;
    }

    public boolean containsField(String fieldName)
    {
        return Fields.containsKey(fieldName.toUpperCase());
    }

    public HPCCColumnMetaData getFieldMetaData(String fieldName)
    {
        return (HPCCColumnMetaData) Fields.get(fieldName.toUpperCase());
    }

    public String[] getAllTableFieldsStringArray()
    {
        String[] fields = new String[Fields.size()];
        Enumeration<Object> it = Fields.elements();
        while (it.hasMoreElements())
        {
            HPCCColumnMetaData col = ((HPCCColumnMetaData) it.nextElement());
            fields[col.getIndex()] = col.getColumnName();
        }
        return fields;
    }

    public String getFormat()
    {
        return Format;
    }

    public String getCsvSeparate()
    {
        return CsvSeparate;
    }

    public String getCsvTerminate()
    {
        return CsvTerminate;
    }

    public void setFormat(String format)
    {
        if (format != null && format.length() > 0)
        {
            if (format.equalsIgnoreCase("THOR") || format.equalsIgnoreCase("CSV") || format.equalsIgnoreCase("XML"))
            {
                Format = format;
                return;
            }
        }
        // TODO Default = FLAT... is this safe to assume???
        Format = "FLAT";
    }

    public void setCsvSeparate(String csvSeparate)
    {
        CsvSeparate = csvSeparate;
    }

    public void setCsvTerminate(String csvTerminate)
    {
        CsvTerminate = csvTerminate;
    }

    public void setCsvQuote(String csvQuote)
    {
        CsvQuote = csvQuote;
    }

    public String getCsvQuote()
    {
        return CsvQuote;
    }

    public boolean hasFileRecDef()
    {
        return Ecl == null || Ecl.length() <= 0 ? false : true;
    }

    public String getFileRecDef(String structname)
    {
        return Ecl == null ? null : structname + " := RECORD " + Ecl + "END; ";
    }

    public void setFileRecDef(String ecl)
    {
        if (ecl != null && ecl.length() > 0)
            setFileFields(ecl);
    }

    public HPCCColumnMetaData getCompatibleField(String keyName, String keyType)
    {
        // EclColumnMetaData field = (EclColumnMetaData)
        // Fields.get(keyName.toUpperCase());
        // want to match up name and type, for now just check name.
        return (HPCCColumnMetaData) Fields.get(keyName.toUpperCase());
    }

    public void setKeyedColumns(Properties KeyFields)
    {
        KeyedColumns = KeyFields;
    }

    public void setNonKeyedColumns(Properties NonKeyFields)
    {
        NonKeyedColumns = NonKeyFields;
        if (NonKeyFields != null && NonKeyFields.size() > 0)
            HasPayLoad = true;
    }

    public void addKeyedColumnInOrder(String KeyLabel)
    {
        KeyedColumns.put(KeyedColumns.size() + 1, KeyLabel);
    }

    public void addKeyedColumn(int KeyIndex, String KeyLabel)
    {
        if (!KeyedColumns.containsKey(KeyIndex))
            KeyedColumns.put(KeyIndex, KeyLabel);
    }

    public void addNonKeyedColumnInOrder(String KeyLabel)
    {
        if (KeyLabel.startsWith("__internal_fpos__"))
        {
            // IndexPositionField = KeyLabel;
            return;
        }
        NonKeyedColumns.put(NonKeyedColumns.size() + 1, KeyLabel);
    }

    public void addNonKeyedColumn(int NonKeyIndex, String NonKeyLabel)
    {
        if (!NonKeyedColumns.containsKey(NonKeyIndex))
        {
            NonKeyedColumns.put(NonKeyIndex, NonKeyLabel);
            HasPayLoad = true;
        }
    }

    public boolean hasPayLoad()
    {
        return HasPayLoad;
    }

    public Properties getKeyedColumns()
    {
        return KeyedColumns;
    }

    public Properties getNonKeyedColumns()
    {
        return NonKeyedColumns;
    }

    public int getNonKeyColumnIndex(String ColumnName)
    {
        int cols = NonKeyedColumns.size();
        for (int i = 1; i <= cols; i++)
        {
            if (NonKeyedColumns.get(i).equals(ColumnName))
                return i;
        }

        return -1;
    }

    public int getKeyColumnIndex(String ColumnName)
    {
        int cols = KeyedColumns.size();
        for (int i = 1; i <= cols; i++)
        {
            if (KeyedColumns.get(i).equals(ColumnName))
                return i;
        }
        return -1;
    }

    public void addRelatedIndex(String IndexName)
    {
        if (relatedIndexes == null)
            relatedIndexes = new ArrayList<String>();

        relatedIndexes.add(IndexName);
    }

    public void setRelatedIndexes(List<String> indexes)
    {
        relatedIndexes = indexes;
    }

    public boolean isRelatedIndex(String indexname)
    {
        if (relatedIndexes != null)
            return relatedIndexes.contains(indexname);
        return false;
    }

    public Iterator<String> getRelatedIndexes()
    {
        return relatedIndexes == null ? null : relatedIndexes.iterator();
    }

    public String[] getRelatedIndexesAsArray()
    {
        String[] indexes = new String[relatedIndexes.size()];
        for (int i = 0; i < indexes.length; i++)
        {
            indexes[i] = relatedIndexes.get(i);
        }
        return indexes;
    }

    public List<String> getRelatedIndexesList()
    {
        return relatedIndexes;
    }

    public int getRelatedIndexesCount()
    {
        return relatedIndexes == null ? 0 : relatedIndexes.size();
    }

    public Object getFileRecDefwithIndexpos(HPCCColumnMetaData fieldMetaData, String structname)
    {
        if (fieldMetaData != null)
            return structname + " := RECORD " + Ecl + fieldMetaData.getEclType() + " " + fieldMetaData.getColumnName()
                    + " {virtual(fileposition)}; END; ";

        return structname + " := RECORD " + Ecl + " END; "; // might need to throw exception instead
    }

    public String getKeyedFieldsAsDelmitedString(char c, String prefixtoappend)
    {
        StringBuilder fields = new StringBuilder();
        int colscount = KeyedColumns.size();

        for (int i = 1; i <= colscount; i++)
        {
            if (prefixtoappend != null)
                fields.append(prefixtoappend).append('.');
            fields.append(KeyedColumns.get(i));
            if (i < colscount)
                fields.append(c).append(" ");
        }
        return fields.toString();
    }

    public String getNonKeyedFieldsAsDelmitedString(char c, String prefixtoappend)
    {
        StringBuilder fields = new StringBuilder();
        int colscount = NonKeyedColumns.size();

        for (int i = 1; i <= colscount; i++)
        {
            if (prefixtoappend != null)
                fields.append(prefixtoappend).append('.');
            fields.append(NonKeyedColumns.get(i));
            if (i < colscount)
                fields.append(c).append(" ");
        }
        return fields.toString();
    }

    public String getAllFieldsAsDelimitedString(char c)
    {
        String[] cols = getAllTableFieldsStringArray();
        StringBuilder fields = new StringBuilder();
        if (cols.length <= 0)
        {
            for (int index = 0; index < cols.length; index++)
            {
                fields.append(cols[index]);

                if (index < cols.length - 1)
                    fields.append(c).append(" ");
            }
        }

        return fields.toString();
    }

    public String getIndexNameByIndex(int i)
    {
        return relatedIndexes.get(i);
    }

    public boolean hasRelatedIndexes()
    {
        return relatedIndexes == null || relatedIndexes.size() <= 0 ? false : true;
    }

    public boolean containsField(HPCCColumnMetaData fieldMetaData, boolean verifyEclType)
    {
        String fieldName = fieldMetaData.getColumnName().toUpperCase();
        if (Fields.containsKey(fieldName))
            if (!verifyEclType
                    || ((HPCCColumnMetaData) Fields.get(fieldName)).getEclType().equals(fieldMetaData.getEclType()))
                return true;
        return false;
    }

    public boolean containsAllFieldsNames(String[] columnNames)
    {
        if (columnNames != null)
        {
            for (int i = 0; i < columnNames.length; i++)
            {
                if (!this.containsField(columnNames[i]))
                    return false;
            }
            return true;
        }

        return false;
    }

    public int getNonKeyedColumnsCount()
    {
        return NonKeyedColumns == null ? 0 : NonKeyedColumns.size();
    }

    public boolean containsField(HPCCColumnMetaData column)
    {
        return this.containsField(column.getColumnName());
    }

    public int getSubfilesCount()
    {
        return (subFiles == null) ? 0 : subFiles.size();
    }

    public boolean containsSubfiles()
    {
        return (getSubfilesCount() > 0) ? true : false;
    }

    public List<String> getSubfiles()
    {
        return subFiles;
    }

    public String getSubfile(int index)
    {
        String subfilename = "";

        try
        {
            if (subFiles != null)
                subfilename = subFiles.get(index);
        }
        catch (Exception e)
        {
        }

        return subfilename;
    }

    public boolean addSubfile(String subfilename)
    {
        boolean isSuccess = false;

        if (subfilename.length() > 0)
        {
            if (subFiles == null)
                subFiles = new ArrayList<String>();
            try
            {
                isSuccess = subFiles.add(subfilename);
            }
            catch (Exception e)
            {
                isSuccess = false;
            }
        }

        return isSuccess;
    }
}
