/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

EXPORT File := MODULE

IMPORT lib_fileservices;

/*------------------------------------- Various types and constants -----------------------------------------------*/
/**
 * A record containing information about filename.  Includes name, size and when last modified.
 * export FsFilenameRecord := RECORD
 *     string name;
 *     integer8 size;
 *     string19 modified;
 * END;
 */

EXPORT FsFilenameRecord := lib_fileservices.FsFilenameRecord;

/**
 * An alias for a logical filename that is stored in a row.
 */

EXPORT FsLogicalFileName := lib_fileservices.FsLogicalFileName;

/**
 * A record containing a logical filename. It contains the following fields:
 *
 * @field name          The logical name of the file;
 */

EXPORT FsLogicalFileNameRecord := lib_fileservices.FsLogicalFileNameRecord;

/**
 * A record containing information about a logical file.
 *
 * @inherits            Contains all the fields in FsLogicalFileNameRecord)
 * @field superfile     Is this a superfile?
 * @field size          Number of bytes in the file (before compression)
 * @field rowcount      Number of rows in the file.
 * @modified            Timestamp when the file was last modified;
 * @owner               The username of the owner who ran the job to create this file.
 * @cluster             The cluster that this file was created on.
 */

EXPORT FsLogicalFileInfoRecord := lib_fileservices.FsLogicalFileInfoRecord;

/**
 * A record containing information about a superfile and its contents.
 *
 * @field supername     The name of the superfile
 * @field subname       The name of the sub-file
 */

EXPORT FsLogicalSuperSubRecord := lib_fileservices.FsLogicalSuperSubRecord;

/**
 * A record containing information about the relationship between two files.
 *
 * @field primaryfile   The logical filename of the primary file
 * @field secondaryfile The logical filename of the secondary file.
 * @field primaryflds   The name of the primary key field for the primary file. The value "__fileposition__"
 *                      indicates the secondary is an INDEX that must use FETCH to access non-keyed fields.
 * @field secondaryflds The name of the foreign key field relating to the primary file.
 * @field kind          The type of relationship between the primary and secondary files.
 *                      Containing either 'link' or 'view'.
 * @field cardinality   The cardinality of the relationship.  The format is <primary>:<secondary>. Valid values are
 *                      "1" or "M".
 * @field payload       Indicates whether the primary or secondary are payload INDEXes.
 * @field description   The description of the relationship.
 */

EXPORT FsFileRelationshipRecord := lib_fileservices.FsFileRelationshipRecord;

/**
 * Constant that indicates IBM RECFM V format file.  Can be passed to SprayFixed for the record size.
 */

EXPORT RECFMV_RECSIZE := lib_fileservices.RECFMV_RECSIZE;

/**
 * Constant that indicates IBM RECFM VB format file.  Can be passed to SprayFixed for the record size.
 */

EXPORT RECFMVB_RECSIZE := lib_fileservices.RECFMVB_RECSIZE;

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
 * @param physical      Whether to also check for the physical existence on disk.  Defaults to FALSE.
 * @return              Whether the file exists.
 */

EXPORT boolean FileExists(varstring lfn, boolean physical=FALSE) :=
    lib_fileservices.FileServices.FileExists(lfn, physical);

/**
 * Removes the logical file from the system, and deletes from the disk.
 *
 * @param lfn           The logical name of the file.
 * @param allowMissing  Whether to suppress an error if the filename does not exist. Defaults to FALSE.
 */

EXPORT DeleteLogicalFile(varstring lfn, boolean allowMissing=FALSE) :=
    lib_fileservices.FileServices.DeleteLogicalFile(lfn, allowMissing);

/**
 * Changes whether access to a file is read only or not.
 *
 * @param lfn           The logical name of the file.
 * @param ro            Whether updates to the file are disallowed.  Defaults to TRUE.
 */
EXPORT SetReadOnly(varstring lfn, boolean ro=TRUE) :=
    lib_fileservices.FileServices.SetReadOnly(lfn, ro);

/**
 * Changes the name of a logical file.
 *
 * @param oldname       The current name of the file to be renamed.
 * @param newname       The new logical name of the file.
 * @param allowOverwrite Is it valid to overwrite an existing file of the same name?  Defaults to FALSE
 */

EXPORT RenameLogicalFile(varstring oldname, varstring newname, boolean allowOverwrite=FALSE) :=
    lib_fileservices.FileServices.RenameLogicalFile(oldname, newname, allowOverwrite);

/**
 * Returns a logical filename that can be used to refer to a logical file in a local or remote dali.
 *
 * @param name          The logical name of the file.
 * @param foreigndali   The IP address of the foreign dali used to resolve the file.  If blank then the file is resolved
 *                      locally.  Defaults to blank.
 * @param abspath       Should a tilde (~) be prepended to the resulting logical file name.  Defaults to FALSE.
 */

EXPORT varstring ForeignLogicalFileName(varstring name, varstring foreigndali='', boolean abspath=FALSE) :=
    lib_fileservices.FileServices.ForeignLogicalFileName(name, foreigndali, abspath);

/**
 * Returns an encoded logical filename that can be used to refer to a external file.  Examples include directly
 * reading from a landing zone.  Upper case characters and other details are escaped.
 *
 * @param location      The IP address of the remote machine. '.' can be used for the local machine.
 * @param path          The path/name of the file on the remote machine.
 * @param abspath       Should a tilde (~) be prepended to the resulting logical file name.  Defaults to TRUE.
 * @return              The encoded logical filename.
 */

EXPORT varstring ExternalLogicalFileName(varstring location, varstring path, boolean abspath=TRUE) :=
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
 * Returns a dataset of information about the logical files known to the system.
 *
 * @param namepattern   The mask of the files to list. Defaults to '*' (all files).
 * @param includenormal Whether to include 'normal' files. Defaults to TRUE.
 * @param includesuper  Whether to include SuperFiles. Defaults to FALSE.
 * @param unknownszero  Whether to set file sizes that are unknown to zero(0) instead of minus-one (-1). Defaults to FALSE.
 * @param foreigndali   The IP address of the foreign dali used to resolve the file.  If blank then the file is resolved
 *                      locally.  Defaults to blank.
 */
EXPORT dataset(FsLogicalFileInfoRecord) LogicalFileList(varstring namepattern='*', boolean includenormal=TRUE, boolean includesuper=FALSE, boolean unknownszero=FALSE, varstring foreigndali='') :=
    lib_fileservices.FileServices.LogicalFileList(namepattern, includenormal, includesuper, unknownszero, foreigndali);

/**
 * Compares two files, and returns a result indicating how well they match.
 *
 * @param file1         The logical name of the first file.
 * @param file2         The logical name of the second file.
 * @param logical_only  Whether to only compare logical information in the system datastore (Dali), and ignore physical
                        information on disk. [Default TRUE]
 * @param use_crcs      Whether to compare physical CRCs of all the parts on disk. This may be slow on large files.
                        Defaults to FALSE.
 * @return              0 if file1 and file2 match exactly
 *                      1 if file1 and file2 contents match, but file1 is newer than file2
 *                      -1 if file1 and file2 contents match, but file2 is newer than file1
 *                      2 if file1 and file2 contents do not match and file1 is newer than file2
 *                      -2 if file1 and file2 contents do not match and file2 is newer than file1
 */

EXPORT INTEGER4 CompareFiles(varstring lfn1, varstring lfn2, boolean logical_only=TRUE, boolean use_crcs=FALSE) :=
    lib_fileservices.FileServices.CompareFiles(lfn1, lfn2, logical_only, use_crcs);

/**
 * Checks the system datastore (Dali) information for the file against the physical parts on disk.
 *
 * @param lfn           The name of the file to check.
 * @param use_crcs      Whether to compare physical CRCs of all the parts on disk. This may be slow on large files.
 * @return              'OK' - The file parts match the datastore information
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
 * @param secondary     The logical filename of the secondary file.
 * @param primaryfields The name of the primary key field for the primary file. The value "__fileposition__"
 *                      indicates the secondary is an INDEX that must use FETCH to access non-keyed fields.
 * @param secondaryfields The name of the foreign key field relating to the primary file.
 * @param relationship  The type of relationship between the primary and secondary files.
 *                      Containing either 'link' or 'view'.  Default is "link".
 * @param cardinality   The cardinality of the relationship.  The format is <primary>:<secondary>. Valid values are
 *                      "1" or "M".
 * @param payload       Indicates whether the primary or secondary are payload INDEXes.
 * @param description   The description of the relationship.
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
 *                      Containing either 'link' or 'view'.  Default is "link".
 */

EXPORT dataset(FsFileRelationshipRecord) FileRelationshipList(varstring primary, varstring secondary, varstring primflds='', varstring secondaryflds='',  varstring kind='link') :=
    lib_fileservices.FileServices.FileRelationshipList(primary, secondary, primflds, secondaryflds, kind);

/**
 * Removes a relationship between two files.
 *
 * @param primary       The logical filename of the primary file.
 * @param secondary     The logical filename of the secondary file.
 * @param primaryfields The name of the primary key field for the primary file.
 * @param secondaryfields The name of the foreign key field relating to the primary file.
 * @param relationship  The type of relationship between the primary and secondary files.
 *                      Containing either 'link' or 'view'.  Default is "link".
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

/**
 * Returns the value of the given attribute for the specified logicalfilename.
 *
 * @param lfn           The name of the logical file.
 * @param attrname      The name of the file attribute to return.
 */
EXPORT varstring GetLogicalFileAttribute(varstring lfn, varstring attrname) :=
    lib_fileservices.FileServices.GetLogicalFileAttribute(lfn, attrname);

/**
 * Toggles protection on and off for the specified logicalfilename.
 *
 * @param lfn           The name of the logical file.
 * @param value         TRUE to enable protection, FALSE to disable.
 */
EXPORT ProtectLogicalFile(varstring lfn, boolean value=TRUE) :=
    lib_fileservices.FileServices.ProtectLogicalFile(lfn, value);

/**
 * The DfuPlusExec action executes the specified command line just as the DfuPLus.exe program would do. This
 * allows you to have all the functionality of the DfuPLus.exe program available within your ECL code.
 *
 * param cmdline        The DFUPlus.exe command line to execute. The valid arguments are documented in the Client
 *                      Tools manual, in the section describing the DfuPlus.exe program.
 */
EXPORT DfuPlusExec(varstring cmdline) :=
    lib_fileservices.FileServices.DfuPlusExec(cmdline);

/*------------------------------------- Spray functions -----------------------------------------------------------*/

/**
 * Sprays a file of fixed length records from a single machine and distributes it across the nodes of the
 * destination group.
 *
 * @param sourceIP      The IP address of the file.
 * @param sourcePath    The path and name of the file.
 * @param recordsize    The size (in bytes) of the records in the file.
 * @param destinationGroup The name of the group to distribute the file across.
 * @param destinationLogicalName The logical name of the file to create.
 * @param timeOut       The time in ms to wait for the operation to complete.  A value of 0 causes the call to return immediately.
 *                      Defaults to no timeout (-1).
 * @param espServerIpPort The url of the ESP file copying service. Defaults to the value of ws_fs_server in the environment.
 * @param maxConnections The maximum number of target nodes to write to concurrently.  Defaults to 1.
 * @param allowOverwrite Is it valid to overwrite an existing file of the same name?  Defaults to FALSE
 * @param replicate     Whether to replicate the new file. Defaults to FALSE.
 * @param compress      Whether to compress the new file. Defaults to FALSE.
 * @param failIfNoSourceFile If TRUE it causes a missing source file to trigger a failure.  Defaults to FALSE.
 * @param expireDays    Number of days to auto-remove file. Default is -1, not expire.
 * @return              The DFU workunit id for the job.
 */
EXPORT varstring fSprayFixed(varstring sourceIP, varstring sourcePath, integer4 recordSize, varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowOverwrite=FALSE, boolean replicate=FALSE, boolean compress=FALSE, boolean failIfNoSourceFile=FALSE, integer4 expireDays=-1) :=
    lib_fileservices.FileServices.fSprayFixed(sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowOverwrite, replicate, compress, failIfNoSourceFile, expireDays);

/**
 * Same as fSprayFixed, but does not return the DFU Workunit ID.
 *
 * @see fSprayFixed
 */

EXPORT SprayFixed(varstring sourceIP, varstring sourcePath, integer4 recordSize, varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowOverwrite=FALSE, boolean replicate=FALSE, boolean compress=FALSE, boolean failIfNoSourceFile=FALSE, integer4 expireDays=-1) :=
    lib_fileservices.FileServices.SprayFixed(sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowOverwrite, replicate, compress, failIfNoSourceFile, expireDays);

// fSprayVariable is now called fSprayDelimited (but the old name is available for backward compatibility)
EXPORT varstring fSprayVariable(varstring sourceIP, varstring sourcePath, integer4 sourceMaxRecordSize=8192, varstring sourceCsvSeparate='\\,', varstring sourceCsvTerminate='\\n,\\r\\n', varstring sourceCsvQuote='\"', varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowOverwrite=FALSE, boolean replicate=FALSE, boolean compress=FALSE, varstring sourceCsvEscape='', boolean failIfNoSourceFile=FALSE, boolean recordStructurePresent=FALSE, boolean quotedTerminator=TRUE, varstring encoding='ascii', integer4 expireDays=-1) :=
    lib_fileservices.FileServices.fSprayVariable(sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowOverwrite, replicate, compress, sourceCsvEscape, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, expireDays);

// SprayVariable is now called SprayDelimited (but the old name is available for backward compatibility)
EXPORT SprayVariable(varstring sourceIP, varstring sourcePath, integer4 sourceMaxRecordSize=8192, varstring sourceCsvSeparate='\\,', varstring sourceCsvTerminate='\\n,\\r\\n', varstring sourceCsvQuote='\"', varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowOverwrite=FALSE, boolean replicate=FALSE, boolean compress=FALSE, varstring sourceCsvEscape='', boolean failIfNoSourceFile=FALSE, boolean recordStructurePresent=FALSE, boolean quotedTerminator=TRUE, varstring encoding='ascii', integer4 expireDays=-1) :=
    lib_fileservices.FileServices.SprayVariable(sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowOverwrite, replicate, compress, sourceCsvEscape, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, expireDays);

/**
 * Sprays a file of fixed delimited records from a single machine and distributes it across the nodes of the
 * destination group.
 *
 * @param sourceIP      The IP address of the file.
 * @param sourcePath    The path and name of the file.
 * @param sourceCsvSeparate The character sequence which separates fields in the file.
 * @param sourceCsvTerminate The character sequence which separates records in the file.
 * @param sourceCsvQuote A string which can be used to delimit fields in the file.
 * @param sourceMaxRecordSize    The maximum size (in bytes) of the records in the file.
 * @param destinationGroup The name of the group to distribute the file across.
 * @param destinationLogicalName The logical name of the file to create.
 * @param timeOut       The time in ms to wait for the operation to complete.  A value of 0 causes the call to return immediately.
 *                      Defaults to no timeout (-1).
 * @param espServerIpPort The url of the ESP file copying service. Defaults to the value of ws_fs_server in the environment.
 * @param maxConnections The maximum number of target nodes to write to concurrently.  Defaults to 1.
 * @param allowOverwrite Is it valid to overwrite an existing file of the same name?  Defaults to FALSE
 * @param replicate     Whether to replicate the new file. Defaults to FALSE.
 * @param compress      Whether to compress the new file. Defaults to FALSE.
 * @param sourceCsvEscape A character that is used to escape quote characters.  Defaults to none.
 * @param failIfNoSourceFile If TRUE it causes a missing source file to trigger a failure.  Defaults to FALSE.
 * @param recordStructurePresent If TRUE derives the record structure from the header of the file.
 * @param quotedTerminator Can the terminator character be included in a quoted field.  Defaults to TRUE.
 *                      If FALSE it allows quicker partitioning of the file (avoiding a complete file scan).
 * @param encoding      A null-terminated string containing the encoding. 
 *                      Can be set to one of the following: 
 *                      ascii, utf8, utf8n, utf16, utf16le, utf16be, utf32, utf32le,utf32be. If omitted, the default is ascii.
 * @param expireDays    Number of days to auto-remove file. Default is -1, not expire.
 * @return              The DFU workunit id for the job.
 */
EXPORT varstring fSprayDelimited(varstring sourceIP, varstring sourcePath, integer4 sourceMaxRecordSize=8192, varstring sourceCsvSeparate='\\,', varstring sourceCsvTerminate='\\n,\\r\\n', varstring sourceCsvQuote='\"', varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowOverwrite=FALSE, boolean replicate=FALSE, boolean compress=FALSE, varstring sourceCsvEscape='', boolean failIfNoSourceFile=FALSE, boolean recordStructurePresent=FALSE, boolean quotedTerminator=TRUE, varstring encoding='ascii', integer4 expireDays=-1) :=
    lib_fileservices.FileServices.fSprayVariable(sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowOverwrite, replicate, compress, sourceCsvEscape, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, expireDays);

/**
 * Same as fSprayDelimited, but does not return the DFU Workunit ID.
 *
 * @see fSprayDelimited
 */

EXPORT SprayDelimited(varstring sourceIP, varstring sourcePath, integer4 sourceMaxRecordSize=8192, varstring sourceCsvSeparate='\\,', varstring sourceCsvTerminate='\\n,\\r\\n', varstring sourceCsvQuote='\"', varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowOverwrite=FALSE, boolean replicate=FALSE, boolean compress=FALSE, varstring sourceCsvEscape='', boolean failIfNoSourceFile=FALSE, boolean recordStructurePresent=FALSE, boolean quotedTerminator=TRUE, const varstring encoding='ascii', integer4 expireDays=-1) :=
    lib_fileservices.FileServices.SprayVariable(sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowOverwrite, replicate, compress, sourceCsvEscape, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, expireDays);

/**
 * Sprays an xml file from a single machine and distributes it across the nodes of the destination group.
 *
 * @param sourceIP      The IP address of the file.
 * @param sourcePath    The path and name of the file.
 * @param sourceMaxRecordSize    The maximum size (in bytes) of the records in the file.
 * @param sourceRowTag  The xml tag that is used to delimit records in the source file.  (This tag cannot recursivly nest.)
 * @param sourceEncoding The unicode encoding of the file.  (utf8,utf8n,utf16be,utf16le,utf32be,utf32le)
 * @param destinationGroup The name of the group to distribute the file across.
 * @param destinationLogicalName The logical name of the file to create.
 * @param timeOut       The time in ms to wait for the operation to complete.  A value of 0 causes the call to return immediately.
 *                      Defaults to no timeout (-1).
 * @param espServerIpPort The url of the ESP file copying service. Defaults to the value of ws_fs_server in the environment.
 * @param maxConnections The maximum number of target nodes to write to concurrently.  Defaults to 1.
 * @param allowOverwrite Is it valid to overwrite an existing file of the same name?  Defaults to FALSE
 * @param replicate     Whether to replicate the new file. Defaults to FALSE.
 * @param compress      Whether to compress the new file. Defaults to FALSE.
 * @param failIfNoSourceFile If TRUE it causes a missing source file to trigger a failure.  Defaults to FALSE.
 * @param expireDays    Number of days to auto-remove file. Default is -1, not expire.
 * @return              The DFU workunit id for the job.
 */

EXPORT varstring fSprayXml(varstring sourceIP, varstring sourcePath, integer4 sourceMaxRecordSize=8192, varstring sourceRowTag, varstring sourceEncoding='utf8', varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowOverwrite=FALSE, boolean replicate=FALSE, boolean compress=FALSE, boolean failIfNoSourceFile=FALSE, integer4 expireDays=-1) :=
    lib_fileservices.FileServices.fSprayXml(sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowOverwrite, replicate, compress, failIfNoSourceFile, expireDays);

/**
 * Same as fSprayXml, but does not return the DFU Workunit ID.
 *
 * @see fSprayXml
 */

EXPORT SprayXml(varstring sourceIP, varstring sourcePath, integer4 sourceMaxRecordSize=8192, varstring sourceRowTag, varstring sourceEncoding='utf8', varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowOverwrite=FALSE, boolean replicate=FALSE, boolean compress=FALSE, boolean failIfNoSourceFile=FALSE, integer4 expireDays=-1) :=
    lib_fileservices.FileServices.SprayXml(sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, allowOverwrite, replicate, compress, failIfNoSourceFile, expireDays);

/**
 * Copies a distributed file from multiple machines, and desprays it to a single file on a single machine.
 *
 * @param logicalName   The name of the file to despray.
 * @param destinationIP The IP of the target machine.
 * @param destinationPath The path of the file to create on the destination machine.
 * @param timeOut       The time in ms to wait for the operation to complete.  A value of 0 causes the call to return immediately.
 *                      Defaults to no timeout (-1).
 * @param espServerIpPort The url of the ESP file copying service. Defaults to the value of ws_fs_server in the environment.
 * @param maxConnections The maximum number of target nodes to write to concurrently.  Defaults to 1.
 * @param allowOverwrite Is it valid to overwrite an existing file of the same name?  Defaults to FALSE
 * @return              The DFU workunit id for the job.
 */

EXPORT varstring fDespray(varstring logicalName, varstring destinationIP, varstring destinationPath, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowOverwrite=FALSE) :=
    lib_fileservices.FileServices.fDespray(logicalName, destinationIP, destinationPath, timeOut, espServerIpPort, maxConnections, allowOverwrite);

/**
 * Same as fDespray, but does not return the DFU Workunit ID.
 *
 * @see fDespray
 */

EXPORT Despray(varstring logicalName, varstring destinationIP, varstring destinationPath, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowOverwrite=FALSE) :=
    lib_fileservices.FileServices.Despray(logicalName, destinationIP, destinationPath, timeOut, espServerIpPort, maxConnections, allowOverwrite);

/**
 * Copies a distributed file to another distributed file.
 *
 * @param sourceLogicalName The name of the file to despray.
 * @param destinationGroup The name of the group to distribute the file across.
 * @param destinationLogicalName The logical name of the file to create.
 * @param sourceDali    The dali that contains the source file (blank implies same dali).  Defaults to same dali.
 * @param timeOut       The time in ms to wait for the operation to complete.  A value of 0 causes the call to return immediately.
 *                      Defaults to no timeout (-1).
 * @param espServerIpPort The url of the ESP file copying service. Defaults to the value of ws_fs_server in the environment.
 * @param maxConnections The maximum number of target nodes to write to concurrently.  Defaults to 1.
 * @param allowOverwrite Is it valid to overwrite an existing file of the same name?  Defaults to FALSE
 * @param replicate     Should the copied file also be replicated on the destination?  Defaults to FALSE
 * @param asSuperfile   Should the file be copied as a superfile?  If TRUE and source is a superfile, then the
 *                      operation creates a superfile on the target, creating sub-files as needed and only overwriting
 *                      existing sub-files whose content has changed. If FALSE, a single file is created.  Defaults to FALSE.
 * @param compress      Whether to compress the new file. Defaults to FALSE.
 * @param forcePush     Should the copy process be executed on the source nodes (push) or on the destination nodes (pull)?
 *                      Default is to pull.
 * @param transferBufferSize Overrides the size (in bytes) of the internal buffer used to copy the file.  Default is 64k.
 * @return              The DFU workunit id for the job.
 */

EXPORT varstring fCopy(varstring sourceLogicalName, varstring destinationGroup, varstring destinationLogicalName, varstring sourceDali='', integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowOverwrite=FALSE, boolean replicate=FALSE, boolean asSuperfile=FALSE, boolean compress=FALSE, boolean forcePush=FALSE, integer4 transferBufferSize=0, boolean preserveCompression=TRUE) :=
    lib_fileservices.FileServices.fCopy(sourceLogicalName, destinationGroup, destinationLogicalName, sourceDali, timeOut, espServerIpPort, maxConnections, allowOverwrite, replicate, asSuperfile, compress, forcePush, transferBufferSize, preserveCompression);

/**
 * Same as fCopy, but does not return the DFU Workunit ID.
 *
 * @see fCopy
 */

EXPORT Copy(varstring sourceLogicalName, varstring destinationGroup, varstring destinationLogicalName, varstring sourceDali='', integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowOverwrite=FALSE, boolean replicate=FALSE, boolean asSuperfile=FALSE, boolean compress=FALSE, boolean forcePush=FALSE, integer4 transferBufferSize=0, boolean preserveCompression=TRUE) :=
    lib_fileservices.FileServices.Copy(sourceLogicalName, destinationGroup, destinationLogicalName, sourceDali, timeOut, espServerIpPort, maxConnections, allowOverwrite, replicate, asSuperfile, compress, forcePush, transferBufferSize, preserveCompression);

/**
 * Ensures the specified file is replicated to its mirror copies.
 *
 * @param logicalName   The name of the file to replicate.
 * @param timeOut       The time in ms to wait for the operation to complete.  A value of 0 causes the call to return immediately.
 *                      Defaults to no timeout (-1).
 * @param espServerIpPort The url of the ESP file copying service. Defaults to the value of ws_fs_server in the environment.
 * @return              The DFU workunit id for the job.
 */

EXPORT varstring fReplicate(varstring logicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.fReplicate(logicalName, timeOut, espServerIpPort);

/**
 * Same as fReplicated, but does not return the DFU Workunit ID.
 *
 * @see fReplicate
 */

EXPORT Replicate(varstring logicalName, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.Replicate(logicalName, timeOut, espServerIpPort);

/**
 * Copies a distributed file to a distributed file on remote system.  Similar to fCopy, except the copy executes
 * remotely.  Since the DFU workunit executes on the remote DFU server, the user name authentication must be the same
 * on both systems, and the user must have rights to copy files on both systems.
 *
 * @param remoteEspFsURL The url of the remote ESP file copying service.
 * @param sourceLogicalName The name of the file to despray.
 * @param destinationGroup The name of the group to distribute the file across.
 * @param destinationLogicalName The logical name of the file to create.
 * @param timeOut       The time in ms to wait for the operation to complete.  A value of 0 causes the call to return immediately.
 *                      Defaults to no timeout (-1).
 * @param maxConnections The maximum number of target nodes to write to concurrently.  Defaults to 1.
 * @param allowOverwrite Is it valid to overwrite an existing file of the same name?  Defaults to FALSE
 * @param replicate     Should the copied file also be replicated on the destination?  Defaults to FALSE
 * @param asSuperfile   Should the file be copied as a superfile?  If TRUE and source is a superfile, then the
 *                      operation creates a superfile on the target, creating sub-files as needed and only overwriting
 *                      existing sub-files whose content has changed. If FALSE a single file is created.  Defaults to FALSE.
 * @param compress      Whether to compress the new file. Defaults to FALSE.
 * @param forcePush     Should the copy process should be executed on the source nodes (push) or on the destination nodes (pull)?
 *                      Default is to pull.
 * @param transferBufferSize Overrides the size (in bytes) of the internal buffer used to copy the file.  Default is 64k.
 * @param wrap          Should the fileparts be wrapped when copying to a smaller sized cluster?  The default is FALSE.
 * @return              The DFU workunit id for the job.
 */

EXPORT varstring fRemotePull(varstring remoteEspFsURL, varstring sourceLogicalName, varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, integer4 maxConnections=-1, boolean allowOverwrite=FALSE, boolean replicate=FALSE, boolean asSuperfile=FALSE, boolean forcePush=FALSE, integer4 transferBufferSize=0, boolean wrap=FALSE, boolean compress=FALSE) :=
    lib_fileservices.FileServices.fRemotePull(remoteEspFsURL, sourceLogicalName, destinationGroup, destinationLogicalName, timeOut, maxConnections, allowOverwrite, replicate, asSuperfile, forcePush, transferBufferSize, wrap, compress);

/**
 * Same as fRemotePull, but does not return the DFU Workunit ID.
 *
 * @see fRemotePull
 */

EXPORT RemotePull(varstring remoteEspFsURL, varstring sourceLogicalName, varstring destinationGroup, varstring destinationLogicalName, integer4 timeOut=-1, integer4 maxConnections=-1, boolean allowOverwrite=FALSE, boolean replicate=FALSE, boolean asSuperfile=FALSE, boolean forcePush=FALSE, integer4 transferBufferSize=0, boolean wrap=FALSE, boolean compress=FALSE) :=
    lib_fileservices.FileServices.RemotePull(remoteEspFsURL, sourceLogicalName, destinationGroup, destinationLogicalName, timeOut, maxConnections, allowOverwrite, replicate, asSuperfile, forcePush, transferBufferSize, wrap, compress);

/*------------------------------------- File monitoring functions -------------------------------------------------------*/

/**
 * Creates a file monitor job in the DFU Server. If an appropriately named file arrives in this interval it will fire
 * the event with the name of the triggering object as the event subtype (see the EVENT function).
 *
 * @param eventToFire   The user-defined name of the event to fire when the filename appears. This value is used as
 *                      the first parameter to the EVENT function.
 * @param name          The name of the logical file to monitor.  This may contain wildcard characters ( * and ?)
 * @param shotCount     The number of times to generate the event before the monitoring job completes. A value
 *                      of -1 indicates the monitoring job continues until manually aborted. The default is 1.
 * @param espServerIpPort The url of the ESP file copying service. Defaults to the value of ws_fs_server in the environment.
 * @return              The DFU workunit id for the job.
 */

EXPORT varstring fMonitorLogicalFileName(varstring eventToFire, varstring name, integer4 shotCount=1, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.fMonitorLogicalFileName(eventToFire, name, shotCount, espServerIpPort);

/**
 * Same as fMonitorLogicalFileName, but does not return the DFU Workunit ID.
 *
 * @see fMonitorLogicalFileName
 */

EXPORT MonitorLogicalFileName(varstring eventToFire, varstring name, integer4 shotCount=1, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.MonitorLogicalFileName(eventToFire, name, shotCount, espServerIpPort);

/**
 * Creates a file monitor job in the DFU Server. If an appropriately named file arrives in this interval it will fire
 * the event with the name of the triggering object as the event subtype (see the EVENT function).
 *
 * @param eventToFire   The user-defined name of the event to fire when the filename appears. This value is used as
 *                      the first parameter to the EVENT function.
 * @param ip            The the IP address for the file to monitor. This may be omitted if the filename parameter
 *                      contains a complete URL.
 * @param filename      The full path of the file(s) to monitor.  This may contain wildcard characters ( * and ?)
 * @param subDirs       Whether to include files in sub-directories (when the filename contains wildcards).  Defaults to FALSE.
 * @param shotCount     The number of times to generate the event before the monitoring job completes. A value
 *                      of -1 indicates the monitoring job continues until manually aborted. The default is 1.
 * @param espServerIpPort The url of the ESP file copying service. Defaults to the value of ws_fs_server in the environment.
 * @return              The DFU workunit id for the job.
 */

EXPORT varstring fMonitorFile(varstring eventToFire, varstring ip, varstring filename, boolean subDirs=FALSE, integer4 shotCount=1, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.fMonitorFile(eventToFire, ip, filename, subDirs, shotCount, espServerIpPort);

/**
 * Same as fMonitorFile, but does not return the DFU Workunit ID.
 *
 * @see fMonitorFile
 */

EXPORT MonitorFile(varstring eventToFire, varstring ip, varstring filename, boolean subdirs=FALSE, integer4 shotCount=1, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.MonitorFile(eventToFire, ip, filename, subdirs, shotCount, espServerIpPort);

/**
 * Waits for the specified DFU workunit to finish.
 *
 * @param wuid          The dfu wfid to wait for.
 * @param timeOut       The time in ms to wait for the operation to complete.  A value of 0 causes the call to return immediately.
 *                      Defaults to no timeout (-1).
 * @param espServerIpPort The url of the ESP file copying service. Defaults to the value of ws_fs_server in the environment.
 * @return              A string containing the final status string of the DFU workunit.
 */

EXPORT varstring WaitDfuWorkunit(varstring wuid, integer4 timeOut=-1, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.WaitDfuWorkunit(wuid, timeOut, espServerIpPort);

/**
 * Aborts the specified DFU workunit.
 *
 * @param wuid          The dfu wfid to abort.
 * @param espServerIpPort The url of the ESP file copying service. Defaults to the value of ws_fs_server in the environment.
 */

EXPORT AbortDfuWorkunit(varstring wuid, varstring espServerIpPort=GETENV('ws_fs_server')) :=
    lib_fileservices.FileServices.AbortDfuWorkunit(wuid, espServerIpPort);

/*------------------------------------- Superfile functions -------------------------------------------------------*/

/**
 * Creates an empty superfile. This function is not included in a superfile transaction.
 *
 * @param superName     The logical name of the superfile.
 * @param sequentialParts Whether the sub-files must be sequentially ordered. Default to FALSE.
 * @param allowExist    Indicating whether to post an error if the superfile already exists. If TRUE, no error is
 *                      posted. Defaults to FALSE.
 */

EXPORT CreateSuperFile(varstring superName, boolean sequentialParts=FALSE, boolean allowExist=FALSE) :=
    lib_fileservices.FileServices.CreateSuperFile(superName, sequentialParts, allowExist);

/**
 * Checks if the specified filename is present in the Distributed File Utility (DFU) and is a SuperFile.
 *
 * @param superName     The logical name of the superfile.
 * @return              Whether the file exists.
 *
 * @see FileExists
 */

EXPORT boolean SuperFileExists(varstring superName) :=
    lib_fileservices.FileServices.SuperFileExists(superName);

/**
 * Deletes the superfile.
 *
 * @param superName     The logical name of the superfile.
 *
 * @see FileExists
 */

EXPORT DeleteSuperFile(varstring superName, boolean deletesub=FALSE) :=
    lib_fileservices.FileServices.DeleteSuperFile(superName, deletesub);

/**
 * Returns the number of sub-files contained within a superfile.
 *
 * @param superName     The logical name of the superfile.
 * @return              The number of sub-files within the superfile.
 */

EXPORT unsigned4 GetSuperFileSubCount(varstring superName) :=
    lib_fileservices.FileServices.GetSuperFileSubCount(superName);

/**
 * Returns the name of the Nth sub-file within a superfile.
 *
 * @param superName     The logical name of the superfile.
 * @param fileNum       The 1-based position of the sub-file to return the name of.
 * @param absPath       Whether to prepend '~' to the name of the resulting logical file name.
 * @return              The logical name of the selected sub-file.
 */

EXPORT varstring GetSuperFileSubName(varstring superName, unsigned4 fileNum, boolean absPath=FALSE) :=
    lib_fileservices.FileServices.GetSuperFileSubName(superName, fileNum, absPath);

/**
 * Returns the position of a file within a superfile.
 *
 * @param superName     The logical name of the superfile.
 * @param subName       The logical name of the sub-file.
 * @return              The 1-based position of the sub-file within the superfile.
 */

EXPORT unsigned4 FindSuperFileSubName(varstring superName, varstring subName) :=
    lib_fileservices.FileServices.FindSuperFileSubName(superName, subName);

/**
 * Starts a superfile transaction.  All superfile operations within the transaction will either be
 * executed atomically or rolled back when the transaction is finished.
 */

EXPORT StartSuperFileTransaction() :=
    lib_fileservices.FileServices.StartSuperFileTransaction();

/**
 * Adds a file to a superfile.
 *
 * @param superName     The logical name of the superfile.
 * @param subName       The name of the logical file to add.
 * @param atPos         The position to add the sub-file, or 0 to append.  Defaults to 0.
 * @param addContents   Controls whether adding a superfile adds the superfile, or its contents.  Defaults to FALSE (do not expand).
 * @param strict        Check addContents only if subName is a superfile, and ensure superfiles exist.
 */

EXPORT AddSuperFile(varstring superName, varstring subName, unsigned4 atPos=0, boolean addContents=FALSE, boolean strict=FALSE) :=
    lib_fileservices.FileServices.AddSuperFile(superName, subName, atPos, addContents, strict);

/**
 * Removes a sub-file from a superfile.
 *
 * @param superName     The logical name of the superfile.
 * @param subName       The name of the sub-file to remove.
 * @param del           Indicates whether the sub-file should also be removed from the disk.  Defaults to FALSE.
 * @param removeContents Controls whether the contents of a sub-file which is a superfile should be recursively removed.  Defaults to FALSE.
 */

EXPORT RemoveSuperFile(varstring superName, varstring subName, boolean del=FALSE, boolean removeContents=FALSE) :=
    lib_fileservices.FileServices.RemoveSuperFile(superName, subName, del, removeContents);

/**
 * Removes all sub-files from a superfile.
 *
 * @param superName     The logical name of the superfile.
 * @param del           Indicates whether the sub-files should also be removed from the disk.  Defaults to FALSE.
 */

EXPORT ClearSuperFile(varstring superName, boolean del=FALSE) :=
    lib_fileservices.FileServices.ClearSuperFile(superName, del);

/**
 * Removes all soley-owned sub-files from a superfile.  If a sub-file is also contained within another superfile
 * then it is retained.
 *
 * @param superName     The logical name of the superfile.
 */

EXPORT RemoveOwnedSubFiles(varstring superName, boolean del=FALSE) :=
    lib_fileservices.FileServices.RemoveOwnedSubFiles(superName, del);

/**
 * Legacy version of RemoveOwnedSubFiles which was incorrectly named in a previous version.
 *
 * @see RemoveOwnedSubFIles
 */

EXPORT DeleteOwnedSubFiles(varstring superName) :=  // Obsolete, use RemoteOwnedSubFiles
    lib_fileservices.FileServices.DeleteOwnedSubFiles(superName);

/**
 * Swap the contents of two superfiles.
 *
 * @param superName1    The logical name of the first superfile.
 * @param superName2    The logical name of the second superfile.
 */

EXPORT SwapSuperFile(varstring superName1, varstring superName2) :=
    lib_fileservices.FileServices.SwapSuperFile(superName1, superName2);

/**
 * Removes a sub-file from a superfile and replaces it with another.
 *
 * @param superName     The logical name of the superfile.
 * @param oldSubFile    The logical name of the sub-file to remove.
 * @param newSubFile    The logical name of the sub-file to replace within the superfile.
 */

EXPORT ReplaceSuperFile(varstring superName, varstring oldSubFile, varstring newSubFile) :=
    lib_fileservices.FileServices.ReplaceSuperFile(superName, oldSubFile, newSubFile);

/**
 * Finishes a superfile transaction.  This executes all the operations since the matching StartSuperFileTransaction().
 * If there are any errors, then all of the operations are rolled back.
 */

EXPORT FinishSuperFileTransaction(boolean rollback=FALSE) :=
    lib_fileservices.FileServices.FinishSuperFileTransaction(rollback);

/**
 * Returns the list of sub-files contained within a superfile.
 *
 * @param superName     The logical name of the superfile.
 * @param recurse       Should the contents of child-superfiles be expanded.  Default is FALSE.
 * @return              A dataset containing the names of the sub-files.
 */

EXPORT dataset(FsLogicalFileNameRecord) SuperFileContents(varstring superName, boolean recurse=FALSE) :=
    lib_fileservices.FileServices.SuperFileContents(superName, recurse);

/**
 * Returns the list of superfiles that a logical file is contained within.
 *
 * @param name          The name of the logical file.
 * @return              A dataset containing the names of the superfiles.
 */

EXPORT dataset(FsLogicalFileNameRecord) LogicalFileSuperOwners(varstring name) :=
    lib_fileservices.FileServices.LogicalFileSuperOwners(name);

/**
 * Returns the list of all the superfiles in the system and their component sub-files.
 *
 * @return              A dataset containing pairs of superName,subName for each component file.
 */

EXPORT dataset(FsLogicalSuperSubRecord) LogicalFileSuperSubList() :=
    lib_fileservices.FileServices.LogicalFileSuperSubList();

/**
 * Moves the sub-files from the first entry in the list of superfiles to the next in the list, repeating the process
 * through the list of superfiles.
 *
 * @param superNames    A set of the names of the superfiles to act on. Any that do not exist will be created.
 *                      The contents of each superfile will be moved to the next in the list.
 * @param addHead       A string containing a comma-delimited list of logical file names to add to the first superfile
 *                      after the promotion process is complete.  Defaults to ''.
 * @param delTail       Indicates whether to physically delete the contents moved out of the last superfile. The default is FALSE.
 * @param createOnlyOne Specifies whether to only create a single superfile (truncate the list at the first
 *                      non-existent superfile). The default is FALSE.
 * @param reverse       Reverse the order of processing the superfiles list, effectively 'demoting' instead of 'promoting' the sub-files. The default is FALSE.
 *
 * @return              A string containing a comma separated list of the previous sub-file contents of the emptied superfile.
 */
EXPORT varstring fPromoteSuperFileList(set of varstring superNames, varstring addHead='', boolean delTail=FALSE, boolean createOnlyOne=FALSE, boolean reverse=FALSE) :=
    lib_fileservices.FileServices.fPromoteSuperFileList(superNames, addHead, delTail, createOnlyOne, reverse);


/**
 * Same as fPromoteSuperFileList, but does not return the DFU Workunit ID.
 *
 * @see fPromoteSuperFileList
 */
EXPORT PromoteSuperFileList(set of varstring superNames, varstring addHead='', boolean delTail=FALSE, boolean createOnlyOne=FALSE, boolean reverse=FALSE) :=
    lib_fileservices.FileServices.PromoteSuperFileList(superNames, addHead, delTail, createOnlyOne, reverse);

/**
 * Returns the full URL to an ESP server process
 *
 * @param username      String containing a username to use for authenticated
 *                      access to the ESP process; an empty string value
 *                      indicates that no user authentication is required;
 *                      OPTIONAL, defaults to an empty string
 * @param userPW        String containing the password to be used with the
 *                      user cited in the username argument; if username is
 *                      empty then this will be ignored; OPTIONAL, defaults
 *                      to an empty string
 *
 * @return              A string containing the full URL (including HTTP scheme
 *                      and port) to an ESP server process; if more than one
 *                      process is defined then the first found process will
 *                      be returned; will return an empty string if an ESP
 *                      server process cannot be found
 */
EXPORT varstring GetEspURL(const varstring username = '', const varstring userPW = '') :=
    lib_fileservices.FileServices.GetEspURL(username, userPW);

END;
