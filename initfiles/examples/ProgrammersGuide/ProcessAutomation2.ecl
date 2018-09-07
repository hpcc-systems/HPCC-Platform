//
//  Example code - use without restriction.  
//
IMPORT $;
IMPORT Std;

Linux := TRUE;
Slash := IF(Linux = TRUE,'/','\\');			//default is Linux. If the LZ is a Windows box, this must change to '\\'
Layout_Sprays := MODULE
  export	Layout_Names := RECORD
		  STRING name;
		END;
  export	ChildNames := RECORD
		  STRING RemoteFilename;
		  STRING ThorLogicalFilename;
		END;
  export	Info := RECORD
			STRING    SourceIP;				                      // Landing Zone's IP address
			STRING    SourceDirectory;			                // Absolute path of directory on Landing Zone where files are located
			STRING    directory_filter     := '*';		      // Regular expression filter for files to be sprayed, default = '*'
			UNSIGNED4 RECORD_size          := 0;		        // RECORD length of files to be sprayed (for fixed length files only)
			STRING    Thor_filename_template;		            // template filename for files to be sprayed, ex. '~thor::in::@version@::cont'
			DATASET(Layout_Names) dSuperfilenames;		      // DATASET of superfiles to add the sprayed files to.
			STRING    GroupName            := '\'thor\'';	  // Thor Group (cluster) name
			STRING    FileDate             := '';		        // version of all of the sprayed files (overrides the next field, dateregex). 
			STRING    date_regex           := '[0-9]{8}';	  // regular expression to get the date from the remote filenames.  
			STRING    file_type            := 'FIXED';  	  // Type of file format.  Possible types are 'FIXED', 'VARIABLE', OR 'XML'.  
			STRING    sourceRowTagXML      := '';		        // XML Row tag.  Only used when file_type = 'XML'.  
			INTEGER4  sourceMaxRecordSize  := 8192;		      // Maximum RECORD size for Variable OR XML files.  
			STRING    sourceCsvSeparate    := '\\,'; 	      // Field separator for variable length RECORDs only.
			STRING    sourceCsvTerminate   := '\\n,\\r\\n';	// Record separator for variable length RECORDs only.  
			STRING    sourceCsvQuote       := '"';         	// Quoted Field delimiter for variable length RECORDs only.  
			BOOLEAN   compress             := FALSE;	      // TRUE = Compress, FALSE = Don't compress.  
		END;
  export	InfoOut := RECORD
			STRING    SourceIP;
			STRING    SourceDirectory;
			STRING    directory_filter     := '*';
			UNSIGNED4 RECORD_size          := 0;
			STRING    Thor_filename_template;
			DATASET(ChildNames)   dFilesToSpray;
			DATASET(Layout_Names) dSuperfilenames;
			STRING    GroupName            := '\'thor\'';
			STRING    FileDate             := '';
			STRING    date_regex           := '[0-9]{8}';
			STRING    file_type            := 'FIXED';         // CAN BE 'VARIABLE', OR 'XML'
			STRING    sourceRowTagXML      := '';
			INTEGER4  sourceMaxRecordSize  := 8192;
			STRING    sourceCsvSeparate    := '\\,';
			STRING    sourceCsvTerminate   := '\\n,\\r\\n';
			STRING    sourceCsvQuote       := '"';
			BOOLEAN   compress             := TRUE;
			UNSIGNED4 remotefilescount     := 0   ;
		END;
	END;
	
fSprayInputFiles(DATASET(Layout_Sprays.Info) pSprayInformation,
													STRING  pSprayRecordSuperfile = '',
													BOOLEAN pIsTesting = FALSE) := FUNCTION
	 
		 // -- Get directory listings using the passed filter for all RECORDs in passed DATASET
		layout_directory_listings := RECORD
			pSprayInformation;
			DATASET(Std.File.FsFileNameRecord) dDirectoryListing;
			UNSIGNED4   remotefilescount;
		END;

		layout_directory_listings tGetDirs(Layout_Sprays.Info l) := TRANSFORM
			SELF.dDirectoryListing := Std.File.remotedirectory(l.SourceIP, l.SourceDirectory, l.directory_filter);
			SELF.remotefilescount  := COUNT(SELF.dDirectoryListing);
			SELF := l;
		END;

		directory_listings := PROJECT(pSprayInformation, tGetDirs(left));

		fSetLogicalFilenames(DATASET(Std.File.FsFileNameRecord) pRemotefilenames,
												 STRING    pTemplatefilename,
												 STRING    pDate,
												 STRING1   pFlag,
												 UNSIGNED4 pRemotefilesCount) := FUNCTION
			
			Layout_Sprays.ChildNames tSetLogicalFilenames(pRemotefilenames l, UNSIGNED4 pCounter) := TRANSFORM
				 // first check to see if filedate is set
				 // if not, then use regex if not blank
				 // if not, use current date
				 filenameversion := MAP(pFlag = 'F' AND pDate != '' =>  pDate,
																pFlag = 'R' AND pDate != '' AND REGEXFIND(pDate,l.name) => REGEXFIND(pDate,l.name, 0),
																WORKUNIT[2..9]);
				 LogicalFilename := REGEXREPLACE( '@version@',pTemplatefilename,filenameversion);
				 SELF.RemoteFilename      := l.name;
				 SELF.ThorLogicalFilename := IF(pRemotefilesCount = 1,  
																				LogicalFilename, 
																				LogicalFilename + '_' + (STRING) pCounter);
			END;
			RETURN PROJECT(pRemotefilenames, tSetLogicalFilenames(LEFT,COUNTER));
		END;

		 // -- finalize DATASET before spray
		Layout_Sprays.InfoOut tGetReadyToSpray(layout_directory_listings l) :=  TRANSFORM
			lDate := MAP(l.FileDate   != '' => l.Filedate,
									 l.date_regex != '' => l.date_regex,
									 '');
			lFlag := MAP(l.FileDate   != '' => 'F',
									 l.date_regex != '' => 'R',
									 '');
			SELF.dFilesToSpray := fSetLogicalFilenames(l.dDirectoryListing, 
																								 l.Thor_filename_template,  
																								 lDate, lFlag, l.remotefilescount);
			SELF  := l;
		END;
	 
		dReadyToSpray := PROJECT(directory_listings, tGetReadyToSpray(LEFT));
	 
		outputwork := OUTPUT(dReadyToSpray,  ALL);
	 
		 // -- Spray Files
		SprayFixedFiles(STRING   pSourceIP,
										STRING   pSourcePath,
										DATASET(Layout_Sprays.childnames) pfilestospray,
										INTEGER4 pRecordLength,
										STRING   pGroupName,
										BOOLEAN  pCompress) :=
					 APPLY(pfilestospray, IF(NOT(Std.File.FileExists(ThorLogicalFilename) OR
																			 Std.File.SuperFileExists(ThorLogicalFilename)), 
																	 Std.File.SprayFixed(pSourceIP,
																													 pSourcePath + Slash + RemoteFilename,
																													 pRecordLength,
																													 pGroupName,
																													 ThorLogicalFilename,,,,,TRUE,
																													 pCompress), 
																	 OUTPUT(ThorLogicalFilename + ' already exists, skipping spray.')));
		 
		// AddSuperfile(DATASET(Layout_Sprays.childnames) plogicalnames, STRING pSuperfilename) :=
					 // APPLY(plogicalnames, Std.File.addsuperfile(pSuperfilename, ThorLogicalFilename));
		AddSuperfile(DATASET(Layout_Sprays.childnames) plogicalnames, STRING pSuperfilename) :=
					 SEQUENTIAL(Std.File.StartSuperfileTransaction(),
											APPLY(plogicalnames, Std.File.AddSuperfile(pSuperfilename, ThorLogicalFilename)),
											Std.File.FinishSuperfileTransaction());
	 
		AddToSuperfiles(DATASET(Layout_Sprays.childnames)   plogicalnames,
										DATASET(Layout_Sprays.Layout_Names) pSuperfilenames) :=
					 APPLY(pSuperfilenames, AddSuperfile(plogicalnames, name));
								 
	 
		SprayVariableFiles(STRING    pSourceIP,
											 STRING    pSourcePath,
											 INTEGER4  pMaxRecordSize,
											 VARSTRING pSourceCsvSeparate,
											 VARSTRING pSourceCsvTerminate,
											 VARSTRING pSourceCsvQuote,
											 DATASET(Layout_Sprays.childnames) pfilestospray,
											 STRING    pGroupName,
											 BOOLEAN   pCompress) :=
								 APPLY(pfilestospray, IF(NOT(Std.File.FileExists(ThorLogicalFilename) OR
																						 Std.File.SuperFileExists(ThorLogicalFilename)),
																				 Std.File.SprayVariable(pSourceIP,
																																		pSourcePath + Slash + RemoteFilename,
																																		pMaxRecordSize,
																																		pSourceCsvSeparate,
																																		pSourceCsvTerminate,
																																		pSourceCsvQuote,
																																		pGroupName,
																																		ThorLogicalFilename,,,,,
																																		TRUE,pCompress), 
																				 OUTPUT(ThorLogicalFilename + ' already exists, skipping spray.')));
	 
		SprayXMLFiles(STRING    pSourceIP,
									STRING    pSourcePath,
									INTEGER4  pMaxRecordSize,
									VARSTRING psourceRowTag,
									DATASET(Layout_Sprays.childnames) pfilestospray,
									STRING    pGroupName,
									BOOLEAN   pCompress) :=
								 APPLY(pfilestospray, IF(NOT(Std.File.FileExists(ThorLogicalFilename) OR    
																						 Std.File.SuperFileExists(ThorLogicalFilename)),
																				 Std.File.SprayXml(pSourceIP,
																																								pSourcePath + Slash + RemoteFilename,
																																								pMaxRecordSize,
																																								psourceRowTag,,
																																								pGroupName,
																																								ThorLogicalFilename,,,,,
																																								TRUE,
																																								pCompress), 
																				 OUTPUT(ThorLogicalFilename + ' already exists, skipping spray.')));
														 
	 
		spray_files :=  APPLY(dReadyToSpray,
													SEQUENTIAL(CASE(file_type,
																					'FIXED' => SprayFixedFiles(SourceIP,
																																		 SourceDirectory,
																																		 dFilesToSpray,
																																		 RECORD_size,
																																		 GroupName,
																																		 compress),
																					'VARIABLE' => SprayVariableFiles(SourceIP,
																																					 SourceDirectory,
																																					 sourceMaxRecordSize,
																																					 sourceCsvSeparate,
																																					 sourceCsvTerminate,
																																					 sourceCsvQuote,
																																					 dFilesToSpray,
																																					 GroupName,
																																					 compress),
																					 'XML' => SprayXMLFiles(SourceIP,
																																	SourceDirectory,
																																	sourceMaxRecordSize,
																																	sourceRowTagXML,
																																	dFilesToSpray,
																																	GroupName,
																																	compress),
																					 OUTPUT('Bad File type: ' + file_type + ' on record')),
																		 AddToSuperfiles(dFilesToSpray, dSuperfilenames)));
		RETURN IF(pIsTesting = FALSE,
							SEQUENTIAL(OUTPUTwork,NOTHOR(spray_files)),
							SEQUENTIAL(OUTPUTwork));
	 
	END;

FilesToSpray := DATASET([
			{$.DeclareData.LZ_IP,				
			 $.DeclareData.LZ_Dir,				
			 'People*.d00',				  
			 89,		  
			 '~$.DeclareData::AUTOMATION::@version@::Fixed',
			 [{'~$.DeclareData::AUTOMATION::Fixed'}],		  
			 'thor',
			 '20061013',
			 '[0-9]{8}',
			 'FIXED',
			 '',
			 8192,
			 '\\,',
			 '\\n,\\r\\n',
			 '"',
			 FALSE
			 }//,					  

			// {LZ_IP,
			 // LZ_Dir,
			 // 'People*.xml',            
			 // 0,                
			 // '~$.DeclareData::AUTOMATION::@version@::XML', 
			 // [{'~$.DeclareData::AUTOMATION::XML'}], 
			 // 'thor',            
			 // '20061013',             
			 // '',               
			 // 'XML',               
			 // 'Row'},

			// {LZ_IP,
			 // LZ_Dir,
			 // 'People*.csv',            
			 // 0,                
			 // '~$.DeclareData::AUTOMATION::@version@::CSV' ,
			 // [{'~$.DeclareData::AUTOMATION::CSV'}], 
			 // 'thor',            
			 // '20061013',             
			 // '',               
			 // 'VARIABLE'}                    
   ], Layout_Sprays.Info);


   // output(FilesToSpray);
   fSprayInputFiles(FilesToSpray);

