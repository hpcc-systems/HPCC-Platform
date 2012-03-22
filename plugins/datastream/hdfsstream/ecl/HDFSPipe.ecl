/* HDFSPipe
Pipe data to and from Hadoop

It is necessary to add this option to your workunit:
#option('pickBestEngine', 0);

This will force your HadoopPipe job to run on the target cluster (as opposed to the optimizer
picking hThor when you've selected Thor, for instance) so that the data lands where you want
it.

For HadoopOut to work HDFS must have append support enabled.  Be default it's disabled.  To
enable it add this to hdfs-site.xml:
		<property>
			<name>dfs.support.append</name>
			<value>true</value>
			<final>true</value>
		</property>
*/

import std;

EXPORT HDFSPipe := MODULE

	/*
   * HDFSPipe.PipeIn - this macro to be called by the user to pipe in data from the Hadoop file system (HDFS).
	 *
	 * @param HadoopRS        	The target recordset.
	 * @param HadoopFile      	The Hadoop data file name as it exists in the HDFS.
	 * @param Layout        		Layout representing the structure of the data.
	 * @param HadoopFileFormat  The Hadoop data file format FLAT | CSV | XML.
	 * @param Host    			    The target hadoop dfs hostname, or ip address.
	 * @param Port		      	  The target hadoop dfs port number.
	 * 													If targeting a local HDFS Host='default' and Port=0 will work
	 *													As long as the local hadoop conf folder is visible to the 'hdfspipe' script
	*/

	export PipeIn(HadoopRS, HadoopFile, Layout, HadoopFileFormat, Host, Port) := MACRO
	#uniquename(formatstr)
		%formatstr% := STD.Str.CleanSpaces(#TEXT(HadoopFileFormat));
		#IF(%formatstr%[1..3] = 'XML')
			#IF (LENGTH(%formatstr%) > 3)
				#uniquename(rowtagcont)
				#uniquename(firsttok)
				%firsttok% := STD.Str.Extract(%formatstr%[4..],1);
				%rowtagcont% := %firsttok%[STD.Str.Find(%firsttok%, '\'',1)+1..STD.Str.Find(%firsttok%, '\'',2)-1];

				#uniquename(headingpos)
				%headingpos% := STD.Str.Find(%formatstr%, 'HEADING');
				#IF (%headingpos% > 0)
					#uniquename(headingcont)
					#uniquename(headingcont2)
					#uniquename(headertext)
					#uniquename(footertext)
					%headingcont% := %formatstr%[%headingpos%+SIZEOF('HEADING')..];
					%headingcont2%:= %headingcont%[STD.Str.Find(%headingcont%, '(')+1..STD.Str.Find(%headingcont%, ')')-1];

					%headertext% := 	STD.Str.Extract(%headingcont2%,1);
					%footertext% := 	STD.Str.Extract(%headingcont2%,2);
				#END
			#ELSE
				%rowtagcont% := 'Row';
			#END
			HadoopRS := PIPE('hdfspipe -si '
				+ ' -nodeid ' + STD.system.Thorlib.node()
				+ ' -clustercount ' + STD.system.Thorlib.nodes()
				+ ' -filename ' + HadoopFile
				+ ' -format '	+  %formatstr%[1..3]
				+ ' -rowtag ' + %rowtagcont%
				// + ' -headertext ' + '???'
				// + ' -footertext ' + '???'
				+ ' -host ' + Host + ' -port ' + Port,
				Layout, HadoopFileFormat);

		#ELSEIF (%formatstr%[1..3] = 'CSV')
		 #uniquename(termpos)
		 %termpos% := STD.Str.Find(%formatstr%, 'TERMINATOR');

			#IF(%termpos% > 0)
				#uniquename(termcont)
				#uniquename(termcont2)
				%termcont% := %formatstr%[%termpos%+11..];
				%termcont2%:= %termcont%[..STD.Str.Find(%termcont%, ')')-1];

				HadoopRS := PIPE('hdfspipe -si '
				+ ' -nodeid ' + STD.system.Thorlib.node()
				+ ' -clustercount ' + STD.system.Thorlib.nodes()
				+ ' -reclen ' + sizeof(Layout)
				+ ' -filename ' + HadoopFile
				+ ' -format '	+  %formatstr%[1..3]
				+ ' -terminator ' + %termcont2%
				//+ ' -outputterminator 1'
				+ ' -host ' + Host	+ ' -port ' + Port,
				Layout, HadoopFileFormat);
			#ELSE
				HadoopRS := PIPE('hdfspipe -si '
				+ ' -nodeid ' + STD.system.Thorlib.node()
				+ ' -clustercount ' + STD.system.Thorlib.nodes()
				+ ' -reclen ' + sizeof(Layout)
				+ ' -filename ' + HadoopFile
				+ ' -format '	+  %formatstr%[1..3]
				+ ' -host ' + Host	+ ' -port ' + Port,
				Layout, HadoopFileFormat);
			#END
		#ELSE
				HadoopRS := PIPE('hdfspipe -si'
				+ ' -nodeid ' + STD.system.Thorlib.node()
				+ ' -clustercount ' + STD.system.Thorlib.nodes()
				+ ' -reclen ' + sizeof(Layout)
				+ ' -filename ' + HadoopFile
				+ ' -format '	+  %formatstr%
				+ ' -host ' + Host 	+ ' -port ' + Port,
				Layout);
		#END
	ENDMACRO;

	/*
	HadoopPipe.PipeOut - writes the given recordset 'RSToHadoop' to the target HDFS system in
												file parts. One file part for each HPCC node.

	RSToHadoop 				- The recordset to stream out.
	HadoopFile 				- The fully qualified target HDFS file name.
	Layout						- Record layout which describes the recordset
	HadoopFileFormat 	-	FLAT|CSV  no support for XML right now.
	Host							- HDFS master host
	Port							- HDFS master port
	HDFSUser					- HDFS user name to log on to HDFS in order to the the file write.
											must have permission to write to the target HDFS location.

	Example:

	HadoopPipe.PipeOut(sue, '/user/hadoop/HDFSAccounts', Layout_CSV_Accounts, CSV, '192.168.56.102', '54310', 'hadoop');
	HadoopPipe.PipeOut(sue, '/user/hadoop/HDFSPersons', Layout_Flat_Persons, FLAT, '192.168.56.102', '54310', 'hadoop');
	*/

	export PipeOut(RSToHadoop, HadoopFile, Layout, HadoopFileFormat, Host, Port, HDFSUser) := MACRO
	#uniquename(formatstr)
	#uniquename(outpartaction)
	#uniquename(mergepartsaction)
		%formatstr% := STD.Str.CleanSpaces(#TEXT(HadoopFileFormat));
		#IF(%formatstr%[1..4] != 'FLAT')
		OUTPUT(RSToHadoop,,
				PIPE('hdfspipe -sop '
				+ ' -host ' + Host
				+ ' -port ' + Port
				+ ' -filename ' + HadoopFile
				+ ' -nodeid ' + STD.system.Thorlib.node()
				+ ' -clustercount ' + STD.system.Thorlib.nodes()
				+ ' -hdfsuser ' + HDFSUser, HadoopFileFormat));
		#ELSE
		OUTPUT(RSToHadoop,,
				PIPE('hdfspipe -sop '
				+ ' -host ' + Host
				+ ' -port ' + Port
				+ ' -filename ' + HadoopFile
				+ ' -nodeid ' + STD.system.Thorlib.node()
				+ ' -clustercount ' + STD.system.Thorlib.nodes()
				+ ' -hdfsuser ' + HDFSUser));
		#END
	ENDMACRO;

	/*
	HadoopPipe.PipeOutAndMerge - writes the given recordset 'RSToHadoop' to the target HDFS system
															 in file parts and merges them together to form a single target file
															 on the HDFS system.

	RSToHadoop 				- The recordset to stream out.
	HadoopFile 				- The fully qualified target HDFS file name.
	Layout						- Record layout which describes the recordset
	HadoopFileFormat 	-	FLAT|CSV  no support for XML right now.
	Host							- HDFS master host
	Port							- HDFS master port
	HDFSUser					- HDFS user name to log on to HDFS in order to the the file write.
											must have permission to write to the target HDFS location.

	Example:

	HadoopPipe.PipeOut(sue, '/user/hadoop/HDFSAccounts', Layout_CSV_Accounts, CSV, '192.168.56.102', '54310', 'hadoop');
	HadoopPipe.PipeOut(sue, '/user/hadoop/HDFSPersons', Layout_Flat_Persons, FLAT, '192.168.56.102', '54310', 'hadoop');
	*/

	export PipeOutAndMerge(RSToHadoop, HadoopFile, Layout, HadoopFileFormat, Host, Port, HDFSUser) := MACRO
	#uniquename(formatstr)
	#uniquename(outpartaction)
	#uniquename(mergepartsaction)
		%formatstr% := STD.Str.CleanSpaces(#TEXT(HadoopFileFormat));
		#IF(%formatstr%[1..4] != 'FLAT')
		//%mergepartsaction% :=DISTRIBUTE(RSToHadoop , 1);
		%outpartaction%:=OUTPUT(RSToHadoop,,
				PIPE('hdfspipe -sop '
				+ ' -host ' + Host
				+ ' -port ' + Port
				+ ' -filename ' + HadoopFile
				+ ' -nodeid ' + STD.system.Thorlib.node()
				+ ' -clustercount ' + STD.system.Thorlib.nodes()
				+ ' -hdfsuser ' + HDFSUser, HadoopFileFormat));

				%mergepartsaction%:=OUTPUT(PIPE('hdfspipe -mf'
				 + ' -nodeid ' + STD.system.Thorlib.node()
				 + ' -clustercount ' + STD.system.Thorlib.nodes()
				 + ' -filename ' + HadoopFile
				 + ' -cleanmerge  1'
				 + ' -hdfsuser ' + HDFSUser
				 + ' -host ' + Host 	+ ' -port ' + Port, Layout));
				 SEQUENTIAL(%outpartaction%, %mergepartsaction%);
		#ELSE
		%outpartaction%:=OUTPUT(RSToHadoop,,
				PIPE('hdfspipe -sop '
				+ ' -host ' + Host
				+ ' -port ' + Port
				+ ' -filename ' + HadoopFile
				+ ' -nodeid ' + STD.system.Thorlib.node()
				+ ' -clustercount ' + STD.system.Thorlib.nodes()
				+ ' -hdfsuser ' + HDFSUser));

				%mergepartsaction%:=OUTPUT(PIPE('hdfspipe -mf'
				 + ' -nodeid ' + STD.system.Thorlib.node()
				 + ' -clustercount ' + STD.system.Thorlib.nodes()
				 + ' -filename ' + HadoopFile
				 + ' -cleanmerge  1'
				 + ' -hdfsuser ' + HDFSUser
				 + ' -host ' + Host 	+ ' -port ' + Port, Layout));
				 SEQUENTIAL(%outpartaction%, %mergepartsaction%);
		#END
	ENDMACRO;

END;
