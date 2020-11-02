#ifndef WSDFUSERVICEBASE_HPP__
#define WSDFUSERVICEBASE_HPP__

#include "jlib.hpp"
#include "jptree.hpp"
#include "jarray.hpp"
#include "primitivetypes.hpp"

using namespace std;
using namespace cppplugin;

class EsdlContext : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> username;
    Owned<Integer> clientMajorVersion;
    Owned<Integer> clientMinorVersion;
};


enum class DFUArrayActions
{
    UNSET = 0,
    Delete,
    AddToSuperfile,
    ChangeProtection,
    ChangeRestriction
};


class EnumHandlerDFUArrayActions
{
public:
    static DFUArrayActions fromString(const char* str)
    {
        if (strcmp(str, "Delete") == 0)
            return DFUArrayActions::Delete;
        else if (strcmp(str, "Add To Superfile") == 0)
            return DFUArrayActions::AddToSuperfile;
        else if (strcmp(str, "Change Protection") == 0)
            return DFUArrayActions::ChangeProtection;
        else
            return DFUArrayActions::ChangeRestriction;

    }

    static const char* toString(DFUArrayActions val)
    {
        if (val == DFUArrayActions::Delete)
            return "Delete";
        else if (val == DFUArrayActions::AddToSuperfile)
            return "Add To Superfile";
        else if (val == DFUArrayActions::ChangeProtection)
            return "Change Protection";
        else
            return "Change Restriction";

    }
};

enum class DFUChangeProtection
{
    UNSET = 0,
    NoChange,
    Protect,
    Unprotect,
    UnprotectAll
};


class EnumHandlerDFUChangeProtection
{
public:
    static DFUChangeProtection fromString(const char* str)
    {
        if (strcmp(str, "0") == 0)
            return DFUChangeProtection::NoChange;
        else if (strcmp(str, "1") == 0)
            return DFUChangeProtection::Protect;
        else if (strcmp(str, "2") == 0)
            return DFUChangeProtection::Unprotect;
        else
            return DFUChangeProtection::UnprotectAll;

    }

    static const char* toString(DFUChangeProtection val)
    {
        if (val == DFUChangeProtection::NoChange)
            return "0";
        else if (val == DFUChangeProtection::Protect)
            return "1";
        else if (val == DFUChangeProtection::Unprotect)
            return "2";
        else
            return "3";

    }
};

enum class DFUChangeRestriction
{
    UNSET = 0,
    NoChange,
    Restrict,
    NotRestricted
};


class EnumHandlerDFUChangeRestriction
{
public:
    static DFUChangeRestriction fromString(const char* str)
    {
        if (strcmp(str, "0") == 0)
            return DFUChangeRestriction::NoChange;
        else if (strcmp(str, "1") == 0)
            return DFUChangeRestriction::Restrict;
        else
            return DFUChangeRestriction::NotRestricted;

    }

    static const char* toString(DFUChangeRestriction val)
    {
        if (val == DFUChangeRestriction::NoChange)
            return "0";
        else if (val == DFUChangeRestriction::Restrict)
            return "1";
        else
            return "2";

    }
};

enum class DFUDefFileFormat
{
    UNSET = 0,
    xml,
    def
};


class EnumHandlerDFUDefFileFormat
{
public:
    static DFUDefFileFormat fromString(const char* str)
    {
        if (strcmp(str, "xml") == 0)
            return DFUDefFileFormat::xml;
        else
            return DFUDefFileFormat::def;

    }

    static const char* toString(DFUDefFileFormat val)
    {
        if (val == DFUDefFileFormat::xml)
            return "xml";
        else
            return "def";

    }
};

enum class FileAccessRole
{
    UNSET = 0,
    Token,
    Engine,
    External
};


class EnumHandlerFileAccessRole
{
public:
    static FileAccessRole fromString(const char* str)
    {
        if (strcmp(str, "Token") == 0)
            return FileAccessRole::Token;
        else if (strcmp(str, "Engine") == 0)
            return FileAccessRole::Engine;
        else
            return FileAccessRole::External;

    }

    static const char* toString(FileAccessRole val)
    {
        if (val == FileAccessRole::Token)
            return "Token";
        else if (val == FileAccessRole::Engine)
            return "Engine";
        else
            return "External";

    }
};

enum class SecAccessType
{
    UNSET = 0,
    None,
    Access,
    Read,
    Write,
    Full
};


class EnumHandlerSecAccessType
{
public:
    static SecAccessType fromString(const char* str)
    {
        if (strcmp(str, "None") == 0)
            return SecAccessType::None;
        else if (strcmp(str, "Access") == 0)
            return SecAccessType::Access;
        else if (strcmp(str, "Read") == 0)
            return SecAccessType::Read;
        else if (strcmp(str, "Write") == 0)
            return SecAccessType::Write;
        else
            return SecAccessType::Full;

    }

    static const char* toString(SecAccessType val)
    {
        if (val == SecAccessType::None)
            return "None";
        else if (val == SecAccessType::Access)
            return "Access";
        else if (val == SecAccessType::Read)
            return "Read";
        else if (val == SecAccessType::Write)
            return "Write";
        else
            return "Full";

    }
};

enum class DFUFileType
{
    UNSET = 0,
    Flat,
    Index,
    Xml,
    Csv,
    Json,
    IndexLocal,
    IndexPartitioned,
    Unset
};


class EnumHandlerDFUFileType
{
public:
    static DFUFileType fromString(const char* str)
    {
        if (strcmp(str, "Flat") == 0)
            return DFUFileType::Flat;
        else if (strcmp(str, "Index") == 0)
            return DFUFileType::Index;
        else if (strcmp(str, "Xml") == 0)
            return DFUFileType::Xml;
        else if (strcmp(str, "Csv") == 0)
            return DFUFileType::Csv;
        else if (strcmp(str, "Json") == 0)
            return DFUFileType::Json;
        else if (strcmp(str, "IndexLocal") == 0)
            return DFUFileType::IndexLocal;
        else if (strcmp(str, "IndexPartitioned") == 0)
            return DFUFileType::IndexPartitioned;
        else
            return DFUFileType::Unset;

    }

    static const char* toString(DFUFileType val)
    {
        if (val == DFUFileType::Flat)
            return "Flat";
        else if (val == DFUFileType::Index)
            return "Index";
        else if (val == DFUFileType::Xml)
            return "Xml";
        else if (val == DFUFileType::Csv)
            return "Csv";
        else if (val == DFUFileType::Json)
            return "Json";
        else if (val == DFUFileType::IndexLocal)
            return "IndexLocal";
        else if (val == DFUFileType::IndexPartitioned)
            return "IndexPartitioned";
        else
            return "Unset";

    }
};

class DFUActionInfo : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_FileName;
    Owned<PString> m_NodeGroup;
    Owned<PString> m_ActionResult;
    Owned<Boolean> m_Failed;
};

class DFUDataColumn : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<Integer> m_ColumnID;
    Owned<PString> m_ColumnLabel;
    Owned<PString> m_ColumnType;
    Owned<PString> m_ColumnValue;
    Owned<Integer> m_ColumnSize;
    Owned<Integer> m_MaxSize;
    Owned<PString> m_ColumnEclType;
    Owned<Integer> m_ColumnRawSize;
    Owned<Boolean> m_IsNaturalColumn;
    Owned<Boolean> m_IsKeyedColumn;
    IArrayOf<DFUDataColumn> m_DataColumns;
};

class DFUFileAccessRequestBase : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Name;
    Owned<PString> m_Cluster;
    Owned<PString> m_JobId;
    Owned<Integer> m_ExpirySeconds = new Integer(60);
    FileAccessRole m_AccessRole = FileAccessRole::UNSET;
    SecAccessType m_AccessType = SecAccessType::UNSET;
    Owned<Boolean> m_ReturnJsonTypeInfo = new Boolean(0);
    Owned<Boolean> m_ReturnBinTypeInfo = new Boolean(0);
};

class DFUPartLocation : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<Integer> m_LocationIndex;
    Owned<PString> m_Host;
};

class DFUFileCopy : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<Integer> m_CopyIndex;
    Owned<Integer> m_LocationIndex;
    Owned<PString> m_Path;
};

class DFUFilePart : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<Integer> m_PartIndex;
    IArrayOf<DFUFileCopy> m_Copies;
    Owned<Boolean> m_TopLevelKey = new Boolean(0);
};

class DFUFileAccessInfo : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_MetaInfoBlob;
    Owned<PString> m_ExpiryTime;
    Owned<Integer> m_NumParts;
    IArrayOf<DFUPartLocation> m_FileLocations;
    IArrayOf<DFUFilePart> m_FileParts;
    Owned<PString> m_RecordTypeInfoBin;
    Owned<PString> m_RecordTypeInfoJson;
    Owned<Integer> m_fileAccessPort;
    Owned<Boolean> m_fileAccessSSL;
};

class DFULogicalFile : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Prefix;
    Owned<PString> m_ClusterName;
    Owned<PString> m_NodeGroup;
    Owned<PString> m_Directory;
    Owned<PString> m_Description;
    Owned<PString> m_Parts;
    Owned<PString> m_Name;
    Owned<PString> m_Owner;
    Owned<PString> m_Totalsize;
    Owned<PString> m_RecordCount;
    Owned<PString> m_Modified;
    Owned<PString> m_LongSize;
    Owned<PString> m_LongRecordCount;
    Owned<Boolean> m_isSuperfile;
    Owned<Boolean> m_isZipfile;
    Owned<Boolean> m_isDirectory = new Boolean(0);
    Owned<Boolean> m_Replicate = new Boolean(0);
    Owned<Integer64> m_IntSize;
    Owned<Integer64> m_IntRecordCount;
    Owned<Boolean> m_FromRoxieCluster;
    Owned<Boolean> m_BrowseData;
    Owned<Boolean> m_IsKeyFile;
    Owned<Boolean> m_IsCompressed;
    Owned<PString> m_ContentType;
    Owned<Integer64> m_CompressedFileSize;
    Owned<PString> m_SuperOwners;
    Owned<Boolean> m_Persistent = new Boolean(0);
    Owned<Boolean> m_IsProtected = new Boolean(0);
    Owned<PString> m_KeyType;
    Owned<Integer> m_NumOfSubfiles;
    Owned<PString> m_Accessed;
};

class DFUFileStat : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_MinSkew;
    Owned<PString> m_MaxSkew;
    Owned<Integer64> m_MinSkewInt64;
    Owned<Integer64> m_MaxSkewInt64;
    Owned<Integer64> m_MinSkewPart;
    Owned<Integer64> m_MaxSkewPart;
};

class DFUPart : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<Integer> m_Id;
    Owned<Integer> m_Copy;
    Owned<PString> m_ActualSize;
    Owned<PString> m_Ip;
    Owned<PString> m_Partsize;
    Owned<Integer64> m_PartSizeInt64;
};

class DFUFilePartsOnCluster : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Cluster;
    Owned<PString> m_BaseDir;
    Owned<PString> m_ReplicateDir;
    Owned<Boolean> m_Replicate;
    Owned<Boolean> m_CanReplicate;
    IArrayOf<DFUPart> m_DFUFileParts;
};

class DFUFileProtect : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Owner;
    Owned<Integer> m_Count;
    Owned<PString> m_Modified;
};

class DFUFilePartition : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<Integer64> m_FieldMask;
    IArrayOf<PString> m_FieldNames;
};

class DFUFileBloom : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<Integer64> m_FieldMask;
    IArrayOf<PString> m_FieldNames;
    Owned<Integer64> m_Limit;
    Owned<PString> m_Probability;
};

class DFUFileDetail : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Name;
    Owned<PString> m_Filename;
    Owned<PString> m_Prefix;
    Owned<PString> m_NodeGroup;
    Owned<Integer> m_NumParts;
    Owned<PString> m_Description;
    Owned<PString> m_Dir;
    Owned<PString> m_PathMask;
    Owned<PString> m_Filesize;
    Owned<Integer64> m_FileSizeInt64;
    Owned<PString> m_ActualSize;
    Owned<PString> m_RecordSize;
    Owned<PString> m_RecordCount;
    Owned<Integer64> m_RecordSizeInt64;
    Owned<Integer64> m_RecordCountInt64;
    Owned<PString> m_Wuid;
    Owned<PString> m_Owner;
    Owned<PString> m_Cluster;
    Owned<PString> m_JobName;
    Owned<PString> m_Persistent;
    Owned<PString> m_Format;
    Owned<PString> m_MaxRecordSize;
    Owned<PString> m_CsvSeparate;
    Owned<PString> m_CsvQuote;
    Owned<PString> m_CsvTerminate;
    Owned<PString> m_CsvEscape;
    Owned<PString> m_Modified;
    Owned<PString> m_Ecl;
    Owned<Boolean> m_ZipFile = new Boolean(0);
    Owned<DFUFileStat> m_Stat;
    IArrayOf<DFUPart> m_DFUFileParts;
    IArrayOf<DFUFilePartsOnCluster> m_DFUFilePartsOnClusters;
    Owned<Boolean> m_isSuperfile = new Boolean(0);
    Owned<Boolean> m_ShowFileContent = new Boolean(1);
    IArrayOf<PString> m_subfiles;
    IArrayOf<DFULogicalFile> m_Superfiles;
    IArrayOf<DFUFileProtect> m_ProtectList;
    Owned<Boolean> m_FromRoxieCluster;
    IArrayOf<PString> m_Graphs;
    Owned<PString> m_UserPermission;
    Owned<PString> m_ContentType;
    Owned<Integer64> m_CompressedFileSize;
    Owned<PString> m_PercentCompressed;
    Owned<Boolean> m_IsCompressed = new Boolean(0);
    Owned<Boolean> m_IsRestricted = new Boolean(0);
    Owned<Boolean> m_BrowseData = new Boolean(1);
    Owned<PString> m_jsonInfo;
    Owned<PString> m_binInfo;
    Owned<PString> m_PackageID;
    Owned<DFUFilePartition> m_Partition;
    IArrayOf<DFUFileBloom> m_Blooms;
    Owned<Integer> m_ExpireDays;
    Owned<PString> m_KeyType;
};

class DFUSpaceItem : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Name;
    Owned<PString> m_NumOfFiles;
    Owned<PString> m_NumOfFilesUnknown;
    Owned<PString> m_TotalSize;
    Owned<PString> m_LargestFile;
    Owned<PString> m_LargestSize;
    Owned<PString> m_SmallestFile;
    Owned<PString> m_SmallestSize;
    Owned<Integer64> m_NumOfFilesInt64;
    Owned<Integer64> m_NumOfFilesUnknownInt64;
    Owned<Integer64> m_TotalSizeInt64;
    Owned<Integer64> m_LargestSizeInt64;
    Owned<Integer64> m_SmallestSizeInt64;
};

class History : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Name;
    Owned<PString> m_Operation;
    Owned<PString> m_Timestamp;
    Owned<PString> m_IP;
    Owned<PString> m_Path;
    Owned<PString> m_Owner;
    Owned<PString> m_Workunit;
};

class AddRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_dstname;
    Owned<PString> m_xmlmap;
};

class AddRemoteRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_dstname;
    Owned<PString> m_srcname;
    Owned<PString> m_srcdali;
    Owned<PString> m_srcusername;
    Owned<PString> m_srcpassword;
};

class AddtoSuperfileRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Superfile;
    Owned<PString> m_Subfiles;
    IArrayOf<PString> m_names;
    Owned<Boolean> m_ExistingFile = new Boolean(0);
    Owned<PString> m_BackToPage;
};

class DFUArrayActionRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    DFUArrayActions m_Type = DFUArrayActions::UNSET;
    Owned<Boolean> m_NoDelete;
    Owned<PString> m_BackToPage;
    IArrayOf<PString> m_LogicalFiles;
    Owned<Boolean> m_removeFromSuperfiles = new Boolean(0);
    Owned<Boolean> m_removeRecursively = new Boolean(0);
    DFUChangeProtection m_Protect = EnumHandlerDFUChangeProtection::fromString("0");
    DFUChangeRestriction m_Restrict = EnumHandlerDFUChangeRestriction::fromString("0");
};

class DFUBrowseDataRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_LogicalName;
    Owned<PString> m_FilterBy;
    Owned<PString> m_ShowColumns;
    Owned<Boolean> m_SchemaOnly = new Boolean(0);
    Owned<Integer64> m_StartForGoback = new Integer64(0);
    Owned<Integer> m_CountForGoback;
    Owned<Integer> m_ChooseFile = new Integer(0);
    Owned<PString> m_Cluster;
    Owned<PString> m_ClusterType;
    Owned<PString> m_ParentName;
    Owned<Integer64> m_Start = new Integer64(0);
    Owned<Integer> m_Count;
    Owned<Boolean> m_DisableUppercaseTranslation;
};

class DFUDefFileRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Name;
    DFUDefFileFormat m_Format = DFUDefFileFormat::UNSET;
};

class DFUFileAccessRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<DFUFileAccessRequestBase> m_RequestBase;
};

class DFUFileAccessV2Request : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Name;
    Owned<PString> m_Cluster;
    Owned<PString> m_RequestId;
    Owned<Integer> m_ExpirySeconds = new Integer(60);
    Owned<Boolean> m_ReturnTextResponse = new Boolean(0);
    Owned<Integer64> m_SessionId = new Integer64(0);
    Owned<Integer> m_LockTimeoutMs = new Integer(300000);
};

class DFUFileCreateRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_ECLRecordDefinition;
    IArrayOf<PString> m_PartLocations;
    Owned<DFUFileAccessRequestBase> m_RequestBase;
};

class DFUFileCreateV2Request : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Name;
    Owned<PString> m_Cluster;
    DFUFileType m_Type = DFUFileType::UNSET;
    Owned<PString> m_ECLRecordDefinition;
    Owned<PString> m_RequestId;
    Owned<Integer> m_ExpirySeconds = new Integer(60);
    Owned<Boolean> m_ReturnTextResponse = new Boolean(0);
    Owned<Boolean> m_Compressed = new Boolean(0);
    Owned<Integer64> m_SessionId = new Integer64(0);
    Owned<Integer> m_LockTimeoutMs = new Integer(300000);
};

class DFUFilePublishRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_FileId;
    Owned<Boolean> m_Overwrite;
    Owned<PString> m_FileDescriptorBlob;
    Owned<Integer64> m_SessionId = new Integer64(0);
    Owned<Integer> m_LockTimeoutMs = new Integer(300000);
    Owned<PString> m_ECLRecordDefinition;
    Owned<Integer64> m_RecordCount;
    Owned<Integer64> m_FileSize;
};

class DFUFileViewRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Scope;
    Owned<Boolean> m_IncludeSuperOwner;
};

class DFUGetDataColumnsRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_OpenLogicalName;
    Owned<PString> m_LogicalName;
    Owned<PString> m_FilterBy;
    Owned<PString> m_ShowColumns;
    Owned<Integer> m_ChooseFile = new Integer(0);
    Owned<PString> m_Cluster;
    Owned<PString> m_ClusterType;
    Owned<Integer64> m_StartIndex = new Integer64(0);
    Owned<Integer64> m_EndIndex = new Integer64(0);
};

class DFUGetFileMetaDataRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_LogicalFileName;
    Owned<PString> m_ClusterName;
    Owned<Boolean> m_IncludeXmlSchema = new Boolean(0);
    Owned<Boolean> m_AddHeaderInXmlSchema = new Boolean(0);
    Owned<Boolean> m_IncludeXmlXPathSchema = new Boolean(0);
    Owned<Boolean> m_AddHeaderInXmlXPathSchema = new Boolean(0);
};

class DFUInfoRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Name;
    Owned<PString> m_Cluster;
    Owned<Boolean> m_UpdateDescription = new Boolean(0);
    Owned<PString> m_QuerySet;
    Owned<PString> m_Query;
    Owned<PString> m_FileName;
    Owned<PString> m_FileDesc;
    Owned<Boolean> m_IncludeJsonTypeInfo = new Boolean(0);
    Owned<Boolean> m_IncludeBinTypeInfo = new Boolean(0);
    DFUChangeProtection m_Protect = EnumHandlerDFUChangeProtection::fromString("0");
    DFUChangeRestriction m_Restrict = EnumHandlerDFUChangeRestriction::fromString("0");
};

class DFUQueryRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Prefix;
    Owned<PString> m_ClusterName;
    Owned<PString> m_NodeGroup;
    Owned<PString> m_ContentType;
    Owned<PString> m_LogicalName;
    Owned<PString> m_Owner;
    Owned<PString> m_StartDate;
    Owned<PString> m_EndDate;
    Owned<PString> m_FileType;
    Owned<Integer64> m_FileSizeFrom = new Integer64(-1);
    Owned<Integer64> m_FileSizeTo = new Integer64(-1);
    Owned<Integer> m_FirstN = new Integer(-1);
    Owned<PString> m_FirstNType;
    Owned<Integer> m_PageSize;
    Owned<Integer> m_PageStartFrom;
    Owned<PString> m_Sortby;
    Owned<Boolean> m_Descending = new Boolean(0);
    Owned<Boolean> m_OneLevelDirFileReturn = new Boolean(0);
    Owned<Integer64> m_CacheHint;
    Owned<Integer> m_MaxNumberOfFiles;
    Owned<Boolean> m_IncludeSuperOwner;
    Owned<PString> m_StartAccessedTime;
    Owned<PString> m_EndAccessedTime;
};

class DFURecordTypeInfoRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Name;
    Owned<Boolean> m_IncludeJsonTypeInfo = new Boolean(1);
    Owned<Boolean> m_IncludeBinTypeInfo = new Boolean(1);
};

class DFUSearchRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_ShowExample;
};

class DFUSearchDataRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Cluster;
    Owned<PString> m_ClusterType;
    Owned<PString> m_OpenLogicalName;
    Owned<PString> m_FilterBy;
    Owned<PString> m_ShowColumns;
    Owned<Integer> m_ChooseFile = new Integer(0);
    Owned<Integer64> m_StartIndex = new Integer64(0);
    Owned<Integer64> m_EndIndex = new Integer64(0);
    Owned<PString> m_LogicalName;
    Owned<PString> m_ParentName;
    Owned<Integer64> m_StartForGoback = new Integer64(0);
    Owned<Integer> m_CountForGoback;
    Owned<Integer64> m_Start = new Integer64(0);
    Owned<Integer> m_Count;
    Owned<PString> m_File;
    Owned<PString> m_Key;
    Owned<Boolean> m_SchemaOnly = new Boolean(1);
    Owned<Boolean> m_RoxieSelections = new Boolean(1);
    Owned<Boolean> m_DisableUppercaseTranslation;
    Owned<PString> m_SelectedKey;
};

class DFUSpaceRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_CountBy;
    Owned<PString> m_ScopeUnder;
    Owned<PString> m_OwnerUnder;
    Owned<PString> m_Interval;
    Owned<PString> m_StartDate;
    Owned<PString> m_EndDate;
};

class EclRecordTypeInfoRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Ecl;
    Owned<Boolean> m_IncludeJsonTypeInfo = new Boolean(1);
    Owned<Boolean> m_IncludeBinTypeInfo = new Boolean(1);
};

class EraseHistoryRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Name;
};

class ListHistoryRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Name;
};

class WsDfuPingRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

};

class SavexmlRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_name;
};

class SuperfileActionRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_action;
    Owned<PString> m_superfile;
    IArrayOf<PString> m_subfiles;
    Owned<PString> m_before;
    Owned<Boolean> m_delete;
    Owned<Boolean> m_removeSuperfile;
};

class SuperfileListRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_superfile;
};

class AddResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

};

class AddRemoteResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

};

class AddtoSuperfileResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Subfiles;
    Owned<PString> m_BackToPage;
    IArrayOf<PString> m_SubfileNames;
};

class DFUArrayActionResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_BackToPage;
    Owned<PString> m_RedirectTo;
    Owned<PString> m_DFUArrayActionResult;
    IArrayOf<DFUActionInfo> m_ActionResults;
};

class DFUBrowseDataResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Name;
    Owned<PString> m_LogicalName;
    Owned<PString> m_FilterBy;
    Owned<PString> m_FilterForGoBack;
    IArrayOf<DFUDataColumn> m_ColumnsHidden;
    Owned<Integer> m_ColumnCount;
    Owned<Integer64> m_StartForGoback = new Integer64(0);
    Owned<Integer> m_CountForGoback;
    Owned<Integer> m_ChooseFile = new Integer(0);
    Owned<Boolean> m_SchemaOnly = new Boolean(0);
    Owned<PString> m_Cluster;
    Owned<PString> m_ClusterType;
    Owned<PString> m_ParentName;
    Owned<Integer64> m_Start;
    Owned<Integer64> m_Count;
    Owned<Integer64> m_PageSize;
    Owned<Integer64> m_Total;
    Owned<PString> m_Result;
    Owned<PString> m_MsgToDisplay;
    Owned<Boolean> m_DisableUppercaseTranslation;
};

class DFUDefFileResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_defFile;
};

class DFUFileAccessResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<DFUFileAccessInfo> m_AccessInfo;
    DFUFileType m_Type = DFUFileType::UNSET;
};

class DFUFileCreateResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_FileId;
    Owned<PString> m_Warning;
    Owned<DFUFileAccessInfo> m_AccessInfo;
};

class DFUFilePublishResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

};

class DFUFileViewResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Scope;
    Owned<Integer> m_NumFiles = new Integer(0);
    IArrayOf<DFULogicalFile> m_DFULogicalFiles;
};

class DFUGetDataColumnsResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_LogicalName;
    Owned<Integer64> m_StartIndex;
    Owned<Integer64> m_EndIndex;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns1;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns2;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns3;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns4;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns5;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns6;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns7;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns8;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns9;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns10;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns11;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns12;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns13;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns14;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns15;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns16;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns17;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns18;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns19;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns20;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns1;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns2;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns3;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns4;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns5;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns6;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns7;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns8;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns9;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns10;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns11;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns12;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns13;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns14;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns15;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns16;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns17;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns18;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns19;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns20;
    Owned<Integer64> m_RowCount;
    Owned<PString> m_ShowColumns;
    Owned<Integer> m_ChooseFile = new Integer(0);
    Owned<PString> m_Cluster;
    Owned<PString> m_ClusterType;
};

class DFUGetFileMetaDataResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<Integer> m_TotalColumnCount;
    Owned<Integer> m_KeyedColumnCount;
    IArrayOf<DFUDataColumn> m_DataColumns;
    Owned<PString> m_XmlSchema;
    Owned<PString> m_XmlXPathSchema;
    Owned<Integer64> m_TotalResultRows;
};

class DFUInfoResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<DFUFileDetail> m_FileDetail;
};

class DFUQueryResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    IArrayOf<DFULogicalFile> m_DFULogicalFiles;
    Owned<PString> m_Prefix;
    Owned<PString> m_ClusterName;
    Owned<PString> m_NodeGroup;
    Owned<PString> m_LogicalName;
    Owned<PString> m_Description;
    Owned<PString> m_Owner;
    Owned<PString> m_StartDate;
    Owned<PString> m_EndDate;
    Owned<PString> m_FileType;
    Owned<Integer64> m_FileSizeFrom = new Integer64(-1);
    Owned<Integer64> m_FileSizeTo = new Integer64(-1);
    Owned<Integer> m_FirstN = new Integer(-1);
    Owned<PString> m_FirstNType;
    Owned<Integer> m_PageSize = new Integer(20);
    Owned<Integer64> m_PageStartFrom = new Integer64(1);
    Owned<Integer64> m_LastPageFrom = new Integer64(-1);
    Owned<Integer64> m_PageEndAt;
    Owned<Integer64> m_PrevPageFrom = new Integer64(-1);
    Owned<Integer64> m_NextPageFrom = new Integer64(-1);
    Owned<Integer64> m_NumFiles;
    Owned<PString> m_Sortby;
    Owned<Boolean> m_Descending = new Boolean(0);
    Owned<PString> m_BasicQuery;
    Owned<PString> m_ParametersForPaging;
    Owned<PString> m_Filters;
    Owned<Integer64> m_CacheHint;
    Owned<Boolean> m_IsSubsetOfFiles;
    Owned<PString> m_Warning;
};

class DFURecordTypeInfoResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_jsonInfo;
    Owned<PString> m_binInfo;
};

class DFUSearchResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_ShowExample;
    IArrayOf<PString> m_ClusterNames;
    IArrayOf<PString> m_FileTypes;
};

class DFUSearchDataResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_OpenLogicalName;
    Owned<PString> m_LogicalName;
    Owned<PString> m_ParentName;
    Owned<Integer64> m_StartIndex;
    Owned<Integer64> m_EndIndex;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns1;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns2;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns3;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns4;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns5;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns6;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns7;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns8;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns9;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns10;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns11;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns12;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns13;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns14;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns15;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns16;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns17;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns18;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns19;
    IArrayOf<DFUDataColumn> m_DFUDataKeyedColumns20;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns1;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns2;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns3;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns4;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns5;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns6;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns7;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns8;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns9;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns10;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns11;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns12;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns13;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns14;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns15;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns16;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns17;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns18;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns19;
    IArrayOf<DFUDataColumn> m_DFUDataNonKeyedColumns20;
    Owned<Integer64> m_RowCount;
    Owned<PString> m_ShowColumns;
    Owned<Integer> m_ChooseFile = new Integer(0);
    Owned<PString> m_Name;
    Owned<PString> m_FilterBy;
    Owned<PString> m_FilterForGoBack;
    IArrayOf<DFUDataColumn> m_ColumnsHidden;
    Owned<Integer> m_ColumnCount;
    Owned<Integer64> m_StartForGoback = new Integer64(0);
    Owned<Integer> m_CountForGoback;
    Owned<Integer64> m_Start;
    Owned<Integer64> m_Count;
    Owned<Integer64> m_PageSize;
    Owned<Integer64> m_Total;
    Owned<PString> m_Result;
    Owned<PString> m_MsgToDisplay;
    Owned<PString> m_Cluster;
    Owned<PString> m_ClusterType;
    Owned<PString> m_File;
    Owned<PString> m_Key;
    Owned<Boolean> m_SchemaOnly;
    Owned<Boolean> m_RoxieSelections;
    Owned<Boolean> m_DisableUppercaseTranslation;
    Owned<Boolean> m_AutoUppercaseTranslation;
    Owned<PString> m_SelectedKey;
};

class DFUSpaceResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_CountBy;
    Owned<PString> m_ScopeUnder;
    Owned<PString> m_OwnerUnder;
    Owned<PString> m_Interval;
    Owned<PString> m_StartDate;
    Owned<PString> m_EndDate;
    IArrayOf<DFUSpaceItem> m_DFUSpaceItems;
};

class EclRecordTypeInfoResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_jsonInfo;
    Owned<PString> m_binInfo;
};

class EraseHistoryResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_xmlmap;
    IArrayOf<History> m_History;
};

class ListHistoryResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_xmlmap;
    IArrayOf<History> m_History;
};

class WsDfuPingResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

};

class SavexmlResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_xmlmap;
};

class SuperfileActionResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_superfile;
    Owned<Integer> m_retcode;
};

class SuperfileListResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_superfile;
    IArrayOf<PString> m_subfiles;
};

class WsDfuServiceBase : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;
    virtual AddResponse* Add(EsdlContext* context, AddRequest* request){return nullptr;}
    virtual AddRemoteResponse* AddRemote(EsdlContext* context, AddRemoteRequest* request){return nullptr;}
    virtual AddtoSuperfileResponse* AddtoSuperfile(EsdlContext* context, AddtoSuperfileRequest* request){return nullptr;}
    virtual DFUArrayActionResponse* DFUArrayAction(EsdlContext* context, DFUArrayActionRequest* request){return nullptr;}
    virtual DFUBrowseDataResponse* DFUBrowseData(EsdlContext* context, DFUBrowseDataRequest* request){return nullptr;}
    virtual DFUDefFileResponse* DFUDefFile(EsdlContext* context, DFUDefFileRequest* request){return nullptr;}
    virtual DFUFileAccessResponse* DFUFileAccess(EsdlContext* context, DFUFileAccessRequest* request){return nullptr;}
    virtual DFUFileAccessResponse* DFUFileAccessV2(EsdlContext* context, DFUFileAccessV2Request* request){return nullptr;}
    virtual DFUFileCreateResponse* DFUFileCreate(EsdlContext* context, DFUFileCreateRequest* request){return nullptr;}
    virtual DFUFileCreateResponse* DFUFileCreateV2(EsdlContext* context, DFUFileCreateV2Request* request){return nullptr;}
    virtual DFUFilePublishResponse* DFUFilePublish(EsdlContext* context, DFUFilePublishRequest* request){return nullptr;}
    virtual DFUFileViewResponse* DFUFileView(EsdlContext* context, DFUFileViewRequest* request){return nullptr;}
    virtual DFUGetDataColumnsResponse* DFUGetDataColumns(EsdlContext* context, DFUGetDataColumnsRequest* request){return nullptr;}
    virtual DFUGetFileMetaDataResponse* DFUGetFileMetaData(EsdlContext* context, DFUGetFileMetaDataRequest* request){return nullptr;}
    virtual DFUInfoResponse* DFUInfo(EsdlContext* context, DFUInfoRequest* request){return nullptr;}
    virtual DFUQueryResponse* DFUQuery(EsdlContext* context, DFUQueryRequest* request){return nullptr;}
    virtual DFURecordTypeInfoResponse* DFURecordTypeInfo(EsdlContext* context, DFURecordTypeInfoRequest* request){return nullptr;}
    virtual DFUSearchResponse* DFUSearch(EsdlContext* context, DFUSearchRequest* request){return nullptr;}
    virtual DFUSearchDataResponse* DFUSearchData(EsdlContext* context, DFUSearchDataRequest* request){return nullptr;}
    virtual DFUSpaceResponse* DFUSpace(EsdlContext* context, DFUSpaceRequest* request){return nullptr;}
    virtual EclRecordTypeInfoResponse* EclRecordTypeInfo(EsdlContext* context, EclRecordTypeInfoRequest* request){return nullptr;}
    virtual EraseHistoryResponse* EraseHistory(EsdlContext* context, EraseHistoryRequest* request){return nullptr;}
    virtual ListHistoryResponse* ListHistory(EsdlContext* context, ListHistoryRequest* request){return nullptr;}
    virtual WsDfuPingResponse* Ping(EsdlContext* context, WsDfuPingRequest* request){return nullptr;}
    virtual SavexmlResponse* Savexml(EsdlContext* context, SavexmlRequest* request){return nullptr;}
    virtual SuperfileActionResponse* SuperfileAction(EsdlContext* context, SuperfileActionRequest* request){return nullptr;}
    virtual SuperfileListResponse* SuperfileList(EsdlContext* context, SuperfileListRequest* request){return nullptr;}
    virtual int Add(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int AddRemote(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int AddtoSuperfile(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUArrayAction(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUBrowseData(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUDefFile(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUFileAccess(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUFileAccessV2(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUFileCreate(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUFileCreateV2(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUFilePublish(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUFileView(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUGetDataColumns(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUGetFileMetaData(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUInfo(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUQuery(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFURecordTypeInfo(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUSearch(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUSearchData(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int DFUSpace(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int EclRecordTypeInfo(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int EraseHistory(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int ListHistory(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int Ping(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int Savexml(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int SuperfileAction(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int SuperfileList(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
};

// Implemented in generated code
extern "C" int onWsDfuAdd(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuAddRemote(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuAddtoSuperfile(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUArrayAction(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUBrowseData(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUDefFile(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUFileAccess(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUFileAccessV2(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUFileCreate(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUFileCreateV2(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUFilePublish(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUFileView(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUGetDataColumns(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUGetFileMetaData(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUInfo(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUQuery(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFURecordTypeInfo(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUSearch(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUSearchData(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuDFUSpace(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuEclRecordTypeInfo(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuEraseHistory(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuListHistory(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuPing(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuSavexml(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuSuperfileAction(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsDfuSuperfileList(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);

// User need to implement this function
extern "C" WsDfuServiceBase* createWsDfuServiceObj();


#endif