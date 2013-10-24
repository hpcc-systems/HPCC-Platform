/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.  All rights reserved.
############################################################################## */

EXPORT File := MODULE

IMPORT lib_fileservices;

/*------------------------------------- Various types and constants -----------------------------------------------*/
/**
 * A record containing information about filename.  Includes name, size and when last modified.
 * ??rename?
 */

EXPORT FsFilenameRecord := lib_fileservices.FsFilenameRecord;

/**
 * A record containing a logical filename.
 */

EXPORT FsLogicalFileName := lib_fileservices.FsLogicalFileName;

/**
 * A record containing a logical filename.
 */

EXPORT FsLogicalFileNameRecord := lib_fileservices.FsLogicalFileNameRecord;

/**
 * A record containing information about a logical file.
 */

EXPORT FsLogicalFileInfoRecord := lib_fileservices.FsLogicalFileInfoRecord;

/**
 * A record containing information about a super file.
 */

EXPORT FsLogicalSuperSubRecord := lib_fileservices.FsLogicalSuperSubRecord;

/**
 * A record containing information about the relationship between two files.
 */

EXPORT FsFileRelationshipRecord := lib_fileservices.FsFileRelationshipRecord;

/** ??
 * Constant that indicates IBM RECFM V format file.  Can be passed to SprayFixed for the record size.
 */

EXPORT INTEGER4 RECFMV_RECSIZE := lib_fileservices.RECFMV_RECSIZE;

/** ??
 * Constant that indicates IBM RECFM VB format file.  Can be passed to SprayFixed for the record size.
 */

EXPORT INTEGER4 RECFMVB_RECSIZE := lib_fileservices.RECFMVB_RECSIZE;

/**
 * Constant that indicates a variable little endian 4 byte length prefixed file.  Can be passed to SprayFixed for the record size.
 */

EXPORT INTEGER4 PREFIX_VARIABLE_RECSIZE := lib_fileservices.PREFIX_VARIABLE_RECSIZE;

/**
 * Constant that indicates a variable big endian 4 byte length prefixed file.  Can be passed to SprayFixed for the record size.
 */

EXPORT INTEGER4 PREFIX_VARIABLE_BIGENDIAN_RECSIZE := lib_fileservices.PREFIX_VARIABLE_BIGENDIAN_RECSIZE;

/*------------------------------------- Spray functions -----------------------------------------------------------*/

/**
 * Returns whether the file exists.
 * 
 * @param lfn           The logical name of the file.
 * @param physical      Whether to also check for the physical existance on disk.  Defaults to false.
 * @return              whether the file exists
 */

EXPORT boolean FileExists(varstring lfn, boolean physical=false) :=
    lib_fileservices.FileServices.FileExists(lfn, physical);

/**
 * Removes the logical file from the system, and deletes from the disk.
 * 
 * @param lfn           The logical name of the file.
 * @param allow_missing Whether to suppress an error if the filename does not exist. Defaults to false.
 */

EXPORT DeleteLogicalFile(varstring lfn, boolean allow_missing=false) :=
    lib_fileservices.FileServices.DeleteLogicalFile(lfn, allow_missing);

/**
 * Changes whether access to a file is read only or not.
 * 
 * @param lfn           The logical name of the file.
 * @param ro            Whether updates to the file are disallowed.  Defaults to true.
 */
EXPORT SetReadOnly(varstring lfn, boolean ro) :=
    lib_fileservices.FileServices.SetReadOnly(lfn, ro);

/**
 * Changes the name of a logical file.
 * 
 * @param oldname       The current name of the file to be renamed.
 * @param newname       The new logical name of the file.
 */

EXPORT RenameLogicalFile(varstring oldname, varstring newname) :=
    lib_fileservices.FileServices.RenameLogicalFile(oldname, newname);

/**
 * Returns a logical filename that can be used to refer to a logical file in a local or remote dali.
 * 
 * @param name          The logical name of the file.
 * @param foreigndali   The IP address of the foreign dali used to resolve the file.  If blank then the file is resolved
 *                      locally.  Defaults to blank.
 * @param abspath       Should a tilde (~) be prepended to the resulting logical file name.  Defaults to false.
 */

EXPORT varstring ForeignLogicalFileName(varstring name, varstring foreigndali='', boolean abspath=false) :=
    lib_fileservices.FileServices.ForeignLogicalFileName(name, foreigndali, abspath);

/**
 * Returns an encoded logical filename that can be used to refer to a external file.  Examples include directly
 * reading from a landing zone.  Upper case characters and other details are escaped.
 * 
 * @param location      The IP address of the remote machine. '.' can be used for the local machine.
 * @param path          The path/name of the file on the remote machine.
 * @param abspath       Should a tilde (~) be prepended to the resulting logical file name.  Defaults to true.
 * @return              The encoded logical filename.
 */

EXPORT varstring ExternalLogicalFileName(varstring location, varstring path, boolean abspath=true) :=
    lib_fileservices.FileServices.ExternalLogicalFileName(location, path, abspath);

/**
 * Returns a string containing the description information associated with the specified filename. This description
 * is set either through ECL watch or by using the FileServices.SetFileDescription function.
 * 
 * @param lfn           The logical name of the file.
 */

EXPORT varstring GetFileDescription(varstring lfn) :=
    lib_fileservices.FileServices.GetFileDescription(lfn);

/**
 * Sets the description associated with the specified filename. 
 * 
 * @param lfn           The logical name of the file.
 * @param val           The description to be associated with the file.
 */

EXPORT SetFileDescription(varstring lfn, varstring val) :=
    lib_fileservices.FileServices.SetFileDescription(lfn, val);

/**
 * Returns a dataset containing a list of files from the specified machineIP and directory. 
 * 
 * @param machineIP     The IP address of the remote machine.
 * @param directory     The path to the directory to read. This must be in the appropriate format for the operating
 *                      system running on the remote machine.
 * @param mask          The filemask specifying which files to include in the result. Defaults to '*' (all files).
 * @param recurse       Whether to include files from subdirectories under the directory. Defaults to FALSE.
 */
EXPORT dataset(FsFilenameRecord) RemoteDirectory(varstring machineIP, varstring dir, varstring mask='*', boolean recurse=FALSE) :=
    lib_fileservices.FileServices.RemoteDirectory(machineIP, dir, mask, recurse);

/**
 * Removes a dataset of information about the logical files known to the the system.
 * 
 * @param namepattern   The mask of the files to list. Defaults to '*' (all files).
 * @param includenormal Whether to include �normal� files. Defaults to TRUE.
 * @param includesuper  Whether to include SuperFiles. Defaults to FALSE.
 * @param unknownszero  Whether to set file sizes that are unknown to zero(0) instead of minus-one (-1). Defaults to FALSE.
 * @param foreigndali   The IP address of the foreign dali used to resolve the file.  If blank then the file is resolved
 *                      locally.  Defaults to blank.
 */
EXPORT dataset(FsLogicalFileInfoRecord) LogicalFileList(varstring namepattern='*', boolean includenormal=true, boolean includesuper=false, boolean unknownszero=false, varstring foreigndali='') :=
    lib_fileservices.FileServices.LogicalFileList(namepattern, includenormal, includesuper, unknownszero, foreigndali);

/**
 * Compares two files, and returns a result indicating how well they match.
 * 
 * @param file1         The logical name of the first file.
 * @param file2         The logical name of the second file.
 * @param logical_only  Whether to only compare logical information in the system datastore (Dali), and ignore physical
                        information on disk. [Default true]
 * @param use_crcs      Whether to compare physical CRCs of all the parts on disk. This may be slow on large files. 
                        Defaults to FALSE.
 * @return              0 if file1 and file2 match exactly
 *                      1 if file1 and file2 contents match, but file1 is newer than file2
 *                      -1 if file1 and file2 contents match, but file2 is newer than file1
 *                      2 if file1 and file2 contents do not match and file1 is newer than file2
 *                      -2 if file1 and file2 contents do not match and file2 is newer than file1
 */

EXPORT INTEGER4 CompareFiles(varstring lfn1, varstring lfn2, boolean logical_only=true, boolean use_crcs=false) :=
    lib_fileservices.FileServices.CompareFiles(lfn1, lfn2, logical_only, use_crcs);

/**
 * Checks the system datastore (Dali) information for the file against the physical parts on disk.
 * 
 * @param lfn           The name of the file to check.
 * @param use_crcs      Whether to compare physical CRCs of all the parts on disk. This may be slow on large files. 
 * @return              'OK' - The file parts match the datastore informaion
 *                      'Could not find file: <filename>' - The logical filename was not found
 *                      'Could not find part file: <partname>' - The partname was not found
 *                      'Modified time differs for: <partname>' - The partname has a different timestamp
 *                      'File size differs for: <partname>' - The partname has a file size
 *                      'File CRC differs for: <partname>' - The partname has a different CRC
 */

EXPORT varstring VerifyFile(varstring lfn, boolean usecrcs) :=
    lib_fileservices.FileServices.VerifyFile(lfn, usecrcs);

/**
 * Defines the relationship between two files. These may be DATASETs or INDEXes. Each record in the primary file
 * should be uniquely defined by the primaryfields (ideally), preferably efficiently.  This information is used
 * by the roxie browser to link files together.
 * 
 * @param primary       The logical filename of the primary file.
 * @param secondary     The logical filename of the secondary file.</para>
 * @param primaryfields The name of the primary key field for the primary file. The value "__fileposition__"
 *                      indicates the secondary is an INDEX that must use FETCH to access non-keyed fields.
 * @param secondaryfields The name of the foreign key field relating to the primary file.
 * @param relationship  The type of relationship between the primary and secondary files.
 *                      Containing either �link� or �view�.  Default is "link".
 * @param cardinality   The cardinality of the relationship.  The format is <primary>:<secondary>. Valid values are
 *                      "1" or "M".
 * @param payload       Indicates whether the primary or secondary are payload INDEXes.
 * @param description   The desciption of the relationship.
 */

EXPORT AddFileRelationship(varstring primary, varstring secondary, varstring primaryflds,  varstring secondaryflds, varstring kind='link', varstring cardinality, boolean payload, varstring description='') :=
    lib_fileservices.FileServices.AddFileRelationship(primary, secondary, primaryflds,  secondaryflds, kind, cardinality, payload, description);

/**
 * Returns a dataset of relationships.   The return records are structured in the FsFileRelationshipRecord format.
 * 
 * @param primary       The logical filename of the primary file.
 * @param secondary     The logical filename of the secondary file.</para>
 * @param primaryfields The name of the primary key field for the primary file.
 * @param secondaryfields The name of the foreign key field relating to the primary file.
 * @param relationship  The type of relationship between the primary and secondary files.
 *                      Containing either �link� or �view�.  Default is "link".
 */

EXPORT dataset(FsFileRelationshipRecord) FileRelationshipList(varstring primary, varstring secondary, varstring primflds='', varstring secondaryflds='',  varstring kind='link') :=
    lib_fileservices.FileServices.FileRelationshipList(primary, secondary, primflds, secondaryflds, kind);

/**
 * Removes a relationship between two files. 
 * 
 * @param primary       The logical filename of the primary file.
 * @param secondary     The logical filename of the secondary file.</para>
 * @param primaryfields The name of the primary key field for the primary file.
 * @param secondaryfields The name of the foreign key field relating to the primary file.
 * @param relationship  The type of relationship between the primary and secondary files.
 *                      Containing either �link� or �view�.  Default is "link".
 */

EXPORT RemoveFileRelationship(varstring primary,  varstring secondary, varstring primaryflds='', varstring secondaryflds='',  varstring kind='link') :=
    lib_fileservices.FileServices.RemoveFileRelationship(primary,  secondary, primaryflds, secondaryflds,  kind);

/**
 * Returns the field mappings for the file, in the same format specified for the SetColumnMapping function.
 * 
 * @param lfn           The logical filename of the primary file.
 */

EXPORT varstring GetColumnMapping(varstring lfn) :=
    lib_fileservices.FileServices.GetColumnMapping(lfn);

/**
 * Defines how the data in the fields of the file mist be transformed between the actual data storage format and the
 * input format used to query that data.  This is used by the user interface of the roxie browser.
 * 
 * @param lfn           The logical filename of the primary file.
 * @param mapping       A string containing a comma separated list of field mappings.
 */

EXPORT SetColumnMapping(varstring lfn, varstring mapping) :=
    lib_fileservices.FileServices.SetColumnMapping(lfn, mapping);

/**
 * Returns a string that can be used in a DATASET declaration to read data from an RFS (Remote File Server) instance
 * (e.g. rfsmysql) on another node.
 *
 * @param server        A string containing the ip:port address for the remote file server.
 * @param query         The text of the query to send to the server
 */

EXPORT varstring EncodeRfsQuery(varstring server, varstring query) :=
    lib_fileservices.FileServices.RfsQuery(server, query);

/**
 * Sends the query to the rfs server.
 *
 * @param server        A string containing the ip:port address for the remote file server.
 * @param query         The text of the query to send to the server
 */

EXPORT RfsAction(varstring server, varstring query) :=
    lib_fileservices.FileServices.RfsAction(server, query);

/**
 * Moves the single physical file between two locations on the same remote machine. The
 * dafileserv utility program must be running on the location machine.
 *
 * @param location      The IP address of the remote machine.
 * @param frompath      The path/name of the file to move.
 * @param topath        The path/name of the target file.
 */

EXPORT MoveExternalFile(varstring location, varstring frompath, varstring topath) :=
    lib_fileservices.FileServices.MoveExternalFile(location, frompath, topath);

/**
 * Removes a single physical file from a remote machine. The
 * dafileserv utility program must be running on the location machine.
 *
 * @param location      The IP address of the remote machine.
 * @param path          The path/name of the file to remove.
 */

EXPORT DeleteExternalFile(varstring location, varstring path) :=
    lib_fileservices.FileServices.DeleteExternalFile(location, path);

/**
 * Creates the path on the location (if it does not already exist). The
 * dafileserv utility program must be running on the location machine.
 *
 * @param location      The IP address of the remote machine.
 * @param path          The path/name of the file to remove.
 */

EXPORT CreateExternalDirectory(varstring location, varstring path) :=
    lib_fileservices.FileServices.CreateExternalDirectory(location, path);

EXPORT varstring GetLogicalFileAttribute(varstring lfn, varstring attrname) :=
    lib_fileservices.FileServices.GetLogicalFileAttribute(lfn, attrname);

EXPORT ProtectLogicalFile(varstring lfn, boolean value=true) :=
    lib_fileservices.FileServices.ProtectLogicalFile(lfn, value);

EXPORT DfuPlusExec(varstring cmdline) :=
    lib_fileservices.FileServices.DfuPlusExec(cmdline);

/*------------------------------------- Spray functions -----------------------------------------------------------*/

EXPORT SprayFixed(varstring sourceIP, varstring sourcePath, integer4 recordSize, varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean compress=false, boolean failifnosourcefile=false) :=
    lib_fileservices.FileServices.SprayFixed(sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowoverwrite, replicate, compress, failIfNoSourceFile);

// SprayVariable is now called SprayDelimited (but the old name is available for backward compatibility)
EXPORT SprayVariable(varstring sourceIP, varstring sourcePath, integer4 sourceMaxRecordSize=8192, varstring sourceCsvSeparate='\\,', varstring sourceCsvTerminate='\\n,\\r\\n', varstring sourceCsvQuote='\'', varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean compress=false, varstring sourceCsvEscape='', boolean failifnosourcefile=false) :=
    lib_fileservices.FileServices.SprayVariable(sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowoverwrite, replicate, compress, sourceCsvEscape, failIfNoSourceFile);

EXPORT SprayDelimited(varstring sourceIP, varstring sourcePath, integer4 sourceMaxRecordSize=8192, varstring sourceCsvSeparate='\\,', varstring sourceCsvTerminate='\\n,\\r\\n', varstring sourceCsvQuote='\'', varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean compress=false, varstring sourceCsvEscape='', boolean failifnosourcefile=false) :=
    lib_fileservices.FileServices.SprayVariable(sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowoverwrite, replicate, compress, sourceCsvEscape, failIfNoSourceFile);

EXPORT SprayXml(varstring sourceIP, varstring sourcePath, integer4 sourceMaxRecordSize=8192, varstring sourceRowTag, varstring sourceEncoding='utf8', varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean compress=false, boolean failifnosourcefile=false) :=
    lib_fileservices.FileServices.SprayXml(sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowoverwrite, replicate, compress, failIfNoSourceFile);

EXPORT Despray(varstring logicalName, varstring destinationIP, varstring destinationPath, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false) :=
    lib_fileservices.FileServices.Despray(logicalName, destinationIP, destinationPath, timeOut, espServerIpPort, maxConnections, allowoverwrite);

EXPORT Copy(varstring sourceLogicalName, varstring destinationGroup, varstring destinationLogicalName, varstring sourceDali='', integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean asSuperfile=false, boolean compress=false, boolean forcePush=false, integer4 transferBufferSize=0) :=
    lib_fileservices.FileServices.Copy(sourceLogicalName, destinationGroup, destinationLogicalName, sourceDali, timeOut, espServerIpPort, maxConnections, allowoverwrite, replicate, asSuperfile, compress, forcePush, transferBufferSize);

EXPORT Replicate(varstring logicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.Replicate(logicalName, timeOut, espServerIpPort);

EXPORT varstring fSprayFixed(varstring sourceIP, varstring sourcePath, integer4 recordSize, varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean compress=false, boolean failifnosourcefile=false) :=
    lib_fileservices.FileServices.fSprayFixed(sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowoverwrite, replicate, compress, failIfNoSourceFile);

// fSprayVariable is now called fSprayDelimited (but the old name is available for backward compatibility)
EXPORT varstring fSprayVariable(varstring sourceIP, varstring sourcePath, integer4 sourceMaxRecordSize=8192, varstring sourceCsvSeparate='\\,', varstring sourceCsvTerminate='\\n,\\r\\n', varstring sourceCsvQuote='\'', varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean compress=false, varstring sourceCsvEscape='', boolean failifnosourcefile=false) :=
    lib_fileservices.FileServices.fSprayVariable(sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowoverwrite, replicate, compress, sourceCsvEscape, failIfNoSourceFile);

EXPORT varstring fSprayDelimited(varstring sourceIP, varstring sourcePath, integer4 sourceMaxRecordSize=8192, varstring sourceCsvSeparate='\\,', varstring sourceCsvTerminate='\\n,\\r\\n', varstring sourceCsvQuote='\'', varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean compress=false, varstring sourceCsvEscape='', boolean failifnosourcefile=false) :=
    lib_fileservices.FileServices.fSprayVariable(sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowoverwrite, replicate, compress, sourceCsvEscape, failIfNoSourceFile);

EXPORT varstring fSprayXml(varstring sourceIP, varstring sourcePath, integer4 sourceMaxRecordSize=8192, varstring sourceRowTag, varstring sourceEncoding='utf8', varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean compress=false, boolean failifnosourcefile=false) :=
    lib_fileservices.FileServices.fSprayXml(sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowoverwrite, replicate, compress, failIfNoSourceFile);

EXPORT varstring fDespray(varstring logicalName, varstring destinationIP, varstring destinationPath, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false) :=
    lib_fileservices.FileServices.fDespray(logicalName, destinationIP, destinationPath, timeOut, espServerIpPort, maxConnections, allowoverwrite);

EXPORT varstring fCopy(varstring sourceLogicalName, varstring destinationGroup, varstring destinationLogicalName, varstring sourceDali='', integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean asSuperfile=false, boolean compress=false, boolean forcePush=false, integer4 transferBufferSize=0) :=
    lib_fileservices.FileServices.fCopy(sourceLogicalName, destinationGroup, destinationLogicalName, sourceDali, timeOut, espServerIpPort, maxConnections, allowoverwrite, replicate, asSuperfile, compress, forcePush, transferBufferSize);

EXPORT varstring fMonitorLogicalFileName(varstring event_name, varstring name, integer4 shotcount=1, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.fMonitorLogicalFileName(event_name, name, shotcount, espServerIpPort);

EXPORT varstring fMonitorFile(varstring event_name, varstring ip, varstring filename, boolean subdirs=false, integer4 shotcount=1, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.fMonitorFile(event_name, ip, filename, subdirs, shotcount, espServerIpPort);

EXPORT varstring fReplicate(varstring logicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.fReplicate(logicalName, timeOut, espServerIpPort);

EXPORT varstring WaitDfuWorkunit(varstring wuid, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.WaitDfuWorkunit(wuid, timeOut, espServerIpPort);

EXPORT AbortDfuWorkunit(varstring wuid, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.AbortDfuWorkunit(wuid, espServerIpPort);

EXPORT MonitorLogicalFileName(varstring event_name, varstring name, integer4 shotcount=1, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.MonitorLogicalFileName(event_name, name, shotcount, espServerIpPort);

EXPORT MonitorFile(varstring event_name, varstring ip, varstring filename, boolean subdirs=false, integer4 shotcount=1, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.MonitorFile(event_name, ip, filename, subdirs, shotcount, espServerIpPort);

EXPORT RemotePull(varstring remoteEspFsURL, varstring sourceLogicalName, varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean asSuperfile=false, boolean forcePush=false, integer4 transferBufferSize=0, boolean wrap=false, boolean compress=false) :=
    lib_fileservices.FileServices.RemotePull(remoteEspFsURL, sourceLogicalName, destinationGroup, destinationLogicalName, timeOut, maxConnections, allowoverwrite, replicate, asSuperfile, forcePush, transferBufferSize, wrap, compress);

EXPORT varstring fRemotePull(varstring remoteEspFsURL, varstring sourceLogicalName, varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean asSuperfile=false, boolean forcePush=false, integer4 transferBufferSize=0, boolean wrap=false, boolean compress=false) :=
    lib_fileservices.FileServices.fRemotePull(remoteEspFsURL, sourceLogicalName, destinationGroup, destinationLogicalName, timeOut, maxConnections, allowoverwrite, replicate, asSuperfile, forcePush, transferBufferSize, wrap, compress);

/*------------------------------------- Superfile functions -------------------------------------------------------*/

/**
  * Creates an empty superfile. This function is not included in a superfile transaction.
  *
  * @param lsuperfn     The logical name of the superfile.
  * @param sequentialparts Whether the sub-files must be sequentially ordered. Default to FALSE.
  * @param allow_exist  Indicating whether to post an error if the superfile already exists. If TRUE, no error is 
  *                     posted. Defaults to FALSE.
  */

EXPORT CreateSuperFile(varstring lsuperfn, boolean sequentialparts=false, boolean allow_exist=false) :=
    lib_fileservices.FileServices.CreateSuperFile(lsuperfn, sequentialparts, allow_exist);

/**
  * Checks if the specified filename is present in the Distributed File Utility (DFU) and is a SuperFile.
  *
  * @param lsuperfn     The logical name of the superfile.
  * @see FileExists
  */

EXPORT boolean SuperFileExists(varstring lsuperfn) :=
    lib_fileservices.FileServices.SuperFileExists(lsuperfn);

/**
  * Deletes the super file from 
  *
  * @param lsuperfn     The logical name of the superfile.
  * @see FileExists
  */

EXPORT DeleteSuperFile(varstring lsuperfn, boolean deletesub=false) :=
    lib_fileservices.FileServices.DeleteSuperFile(lsuperfn, deletesub);

EXPORT unsigned4 GetSuperFileSubCount(varstring lsuperfn) :=
    lib_fileservices.FileServices.GetSuperFileSubCount(lsuperfn);

EXPORT varstring GetSuperFileSubName(varstring lsuperfn, unsigned4 filenum, boolean abspath=false) :=
    lib_fileservices.FileServices.GetSuperFileSubName(lsuperfn, filenum, abspath);

EXPORT unsigned4 FindSuperFileSubName(varstring lsuperfn, varstring lfn) :=
    lib_fileservices.FileServices.FindSuperFileSubName(lsuperfn, lfn);

EXPORT StartSuperFileTransaction() :=
    lib_fileservices.FileServices.StartSuperFileTransaction();

EXPORT AddSuperFile(varstring lsuperfn, varstring lfn, unsigned4 atpos=0, boolean addcontents=false, boolean strict=false) :=
    lib_fileservices.FileServices.AddSuperFile(lsuperfn, lfn, atpos, addcontents, strict);

EXPORT RemoveSuperFile(varstring lsuperfn, varstring lfn, boolean del=false, boolean remcontents=false) :=
    lib_fileservices.FileServices.RemoveSuperFile(lsuperfn, lfn, del, remcontents);

EXPORT ClearSuperFile(varstring lsuperfn, boolean del=false) :=
    lib_fileservices.FileServices.ClearSuperFile(lsuperfn, del);

EXPORT DeleteOwnedSubFiles(varstring lsuperfn) :=
    lib_fileservices.FileServices.DeleteOwnedSubFiles(lsuperfn);

EXPORT SwapSuperFile(varstring lsuperfn1, varstring lsuperfn2) :=
    lib_fileservices.FileServices.SwapSuperFile(lsuperfn1, lsuperfn2);

EXPORT ReplaceSuperFile(varstring lsuperfn, varstring lfn, varstring bylfn) :=
    lib_fileservices.FileServices.ReplaceSuperFile(lsuperfn, lfn, bylfn);

EXPORT FinishSuperFileTransaction(boolean rollback=false) :=
    lib_fileservices.FileServices.FinishSuperFileTransaction(rollback);

EXPORT dataset(FsLogicalFileNameRecord) SuperFileContents(varstring lsuperfn, boolean recurse=false) :=
    lib_fileservices.FileServices.SuperFileContents(lsuperfn, recurse);

EXPORT dataset(FsLogicalFileNameRecord) LogicalFileSuperOwners(varstring lfn) :=
    lib_fileservices.FileServices.LogicalFileSuperOwners(lfn);

EXPORT dataset(FsLogicalSuperSubRecord) LogicalFileSuperSubList() :=
    lib_fileservices.FileServices.LogicalFileSuperSubList();

EXPORT PromoteSuperFileList(set of varstring lsuperfns, varstring addhead='', boolean deltail=false, boolean createonlyonesuperfile=false, boolean reverse=false) :=
    lib_fileservices.FileServices.PromoteSuperFileList(lsuperfns, addhead, deltail, createonlyonesuperfile, reverse);

EXPORT varstring fPromoteSuperFileList(set of varstring lsuperfns, varstring addhead='', boolean deltail=false, boolean createonlyonesuperfile=false, boolean reverse=false) :=
    lib_fileservices.FileServices.fPromoteSuperFileList(lsuperfns, addhead, deltail, createonlyonesuperfile, reverse);

END;
