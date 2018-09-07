//
//  Example code - use without restriction.  
//
IMPORT Std;

EXPORT DeclareData := MODULE

	EXPORT LZ_IP  := '10.173.9.4';		//This MUST be changed to the Landing Zone IP for your configuration
	EXPORT LZ_Dir := 'ProgGuide/';   

	EXPORT Layout_Person := RECORD
		UNSIGNED3 PersonID;
		QSTRING15 FirstName;
		QSTRING25 LastName;
		STRING1   MiddleInitial;
		STRING1   Gender;
		QSTRING42 Street;
		QSTRING20 City;
		STRING2   State;
		QSTRING5  Zip;
	END;
	EXPORT Layout_Accounts := RECORD
		QSTRING20 Account;
		QSTRING8  OpenDate;
		STRING2   IndustryCode;
		STRING1   AcctType;
		STRING1   AcctRate;
		UNSIGNED1 Code1;
		UNSIGNED1 Code2;
		UNSIGNED4 HighCredit;
		UNSIGNED4 Balance;
	END;
	EXPORT Layout_Accounts_Link := RECORD
		UNSIGNED3 PersonID;
		Layout_Accounts;
	END;
	SHARED Layout_Combined := RECORD
		Layout_Person;
		DATASET(Layout_Accounts) Accounts;
	END;

  EXPORT Person := MODULE
	  EXPORT File     := DATASET('~PROGGUIDE::EXAMPLEDATA::People',
													     Layout_Person, THOR);
	  EXPORT FilePlus := DATASET('~PROGGUIDE::EXAMPLEDATA::People',
													     {Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
	END;												 
  EXPORT Accounts := DATASET('~PROGGUIDE::EXAMPLEDATA::Accounts',
														 {Layout_Accounts_Link,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
  EXPORT PersonAccounts := DATASET('~PROGGUIDE::EXAMPLEDATA::PeopleAccts',
																	 {Layout_Combined,UNSIGNED8 RecPos{virtual(fileposition)}},THOR);

  EXPORT IDX_Person_PersonID := INDEX(Person.FilePlus,{PersonID,RecPos},'~PROGGUIDE::EXAMPLEDATA::KEYS::People.PersonID');
  EXPORT IDX_Accounts_PersonID := INDEX(Accounts,{PersonID,RecPos},'~PROGGUIDE::EXAMPLEDATA::KEYS::Accounts.PersonID');
  EXPORT IDX_Accounts_PersonID_Payload := INDEX(Accounts,{PersonID},{Account,OpenDate,IndustryCode,AcctType,AcctRate,Code1,Code2,HighCredit,Balance,RecPos},'~PROGGUIDE::EXAMPLEDATA::KEYS::Accounts.PersonID.Payload');
  EXPORT IDX_PersonAccounts_PersonID := INDEX(PersonAccounts,{PersonID,RecPos},'~PROGGUIDE::EXAMPLEDATA::KEYS::PeopleAccts.PersonID');

  EXPORT IDX__Person_LastName_FirstName := INDEX(Person.FilePlus,{LastName,FirstName,RecPos},'~PROGGUIDE::EXAMPLEDATA::KEYS::People.LastName.FirstName');
  EXPORT IDX__Person_PersonID_Payload := INDEX(Person.FilePlus,{PersonID},{FirstName,LastName,MiddleInitial,Gender,Street,City,State,Zip,RecPos},'~PROGGUIDE::EXAMPLEDATA::KEYS::People.PersonID.Payload');
  EXPORT IDX__Person_LastName_FirstName_Payload := INDEX(Person.FilePlus,{LastName,FirstName},{Person.FilePlus},'~PROGGUIDE::EXAMPLEDATA::KEYS::People.LastName.FirstName.Payload');
  EXPORT IDX__Person_State_City_Zip_LastName_FirstName_Payload := INDEX(Person.FilePlus,{State,City,Zip,Lastname,Firstname},{Person.FilePlus},'~PROGGUIDE::EXAMPLEDATA::KEYS::People.State.City.Zip.LastName.FirstName.Payload');

  EXPORT TimeZonesXML := DATASET('~PROGGUIDE::EXAMPLEDATA::XML_timezones',
															   {STRING code,
																  STRING state,
																	STRING description,
																	STRING timezone{XPATH('zone')}},
															   XML('timezones/area') );

	EXPORT MAC_CountFieldValues(infile,infield) := MACRO
		// Create the count of unique values in the infield
		COUNT(DEDUP(TABLE(infile,{infile.infield}),infield,ALL));
		// Create the crosstab report
		#UNIQUENAME(r_macro)
		%r_macro% := RECORD
			infile.infield;
			INTEGER cnt := COUNT(GROUP);
		END;
		#UNIQUENAME(y_macro)
		%y_macro% := TABLE(infile,%r_macro%,infield,FEW);
		OUTPUT(CHOOSEN(%y_macro%,50000));
	ENDMACRO;

  //These attributes directly support the SuperFiles articles
	EXPORT AllPeople  := '~PROGGUIDE::SUPERFILE::AllPeople';
	EXPORT BaseFile   := '~PROGGUIDE::SUPERFILE::Base';
	EXPORT WeeklyFile := '~PROGGUIDE::SUPERFILE::Weekly';
	EXPORT DailyFile  := '~PROGGUIDE::SUPERFILE::Daily';
	EXPORT SubFile1   := '~PROGGUIDE::SUPERFILE::People1';
	EXPORT SubFile2   := '~PROGGUIDE::SUPERFILE::People2';
	EXPORT SubFile3   := '~PROGGUIDE::SUPERFILE::People3';
	EXPORT SubFile4   := '~PROGGUIDE::SUPERFILE::People4';
	EXPORT SubFile5   := '~PROGGUIDE::SUPERFILE::People5';
	EXPORT SubFile6   := '~PROGGUIDE::SUPERFILE::People6';
  EXPORT SuperFile1 := DATASET(BaseFile,Layout_Person,FLAT);
  EXPORT SuperFile2 := DATASET(AllPeople,Layout_Person,FLAT);

  //this macro lists the sub-files in a SuperFile	
  EXPORT MAC_ListSFsubfiles(SuperFile) := MACRO
		#UNIQUENAME(SeedRec)
		%SeedRec% := DATASET([{''}], {STRING fn});
		#UNIQUENAME(Xform)
		TYPEOF(%SeedRec%) %Xform%(%SeedRec% L, INTEGER C) := TRANSFORM 
			SELF.fn := Std.File.GetSuperFileSubName(SuperFile,C); 
		END; 
		OUTPUT(NORMALIZE(%SeedRec%,
										 Std.File.GetSuperFileSubCount(SuperFile),
										 %Xform%(LEFT,COUNTER))); 
  ENDMACRO;

  //this macro clears then deletes a SuperFile	
  EXPORT MAC_DeleteSF(SuperFile) := MACRO
		SEQUENTIAL(
			Std.File.StartSuperFileTransaction(),
			Std.File.ClearSuperFile(SuperFile),
			Std.File.FinishSuperFileTransaction(),
			Std.File.DeleteSuperFile(SuperFile));
  ENDMACRO;

  //These attributes directly support the "Indexes Into SuperFiles" article
  EXPORT i1name := '~PROGGUIDE::SUPERKEY::IDX1';
  EXPORT i2name := '~PROGGUIDE::SUPERKEY::IDX2';
	EXPORT i3name := '~PROGGUIDE::SUPERKEY::IDX3';
  EXPORT SFname := '~PROGGUIDE::SUPERKEY::SF1';
  EXPORT SKname := '~PROGGUIDE::SUPERKEY::SK1';
  EXPORT ds1 := DATASET(SubFile1,{Layout_Person,UNSIGNED8 RecPos {VIRTUAL(fileposition)}},THOR);
  EXPORT ds2 := DATASET(SubFile2,{Layout_Person,UNSIGNED8 RecPos {VIRTUAL(fileposition)}},THOR);
  EXPORT i1  := INDEX(ds1,{personid,RecPos},i1name);
  EXPORT i2  := INDEX(ds2,{personid,RecPos},i2name);
  EXPORT sf1 := DATASET(SFname,{Layout_Person,UNSIGNED8 RecPos {VIRTUAL(fileposition)}},THOR);
  EXPORT sk1 := INDEX(sf1,{personid,RecPos},SKname);
  EXPORT sk2 := INDEX(sf1,{personid,RecPos},i3name);

  //These attributes directly support the "Using SuperKeys" article
	EXPORT SubKey1   := '~PROGGUIDE::SUPERKEY::Accounts1';
	EXPORT SubKey2   := '~PROGGUIDE::SUPERKEY::Accounts2';
	EXPORT SubKey3   := '~PROGGUIDE::SUPERKEY::Accounts3';
	EXPORT SubKey4   := '~PROGGUIDE::SUPERKEY::Accounts4';
	EXPORT SubIDX1   := '~PROGGUIDE::SUPERKEY::KEY::AcctsIDX1';
	EXPORT SubIDX2   := '~PROGGUIDE::SUPERKEY::KEY::AcctsIDX2';
	EXPORT SubIDX3   := '~PROGGUIDE::SUPERKEY::KEY::AcctsIDX3';
	EXPORT SubIDX4   := '~PROGGUIDE::SUPERKEY::KEY::AcctsIDX4';
  EXPORT AcctSKname   := '~PROGGUIDE::SUPERKEY::KEY::AcctsSK';
  EXPORT AcctSK := INDEX(Accounts,{PersonID},
												 {Account,OpenDate,IndustryCode,AcctType,AcctRate,Code1,Code2,HighCredit,Balance,RecPos},
												 AcctSKname);

  //This MACRO directly supports the "Scanning LandingZone Files" article
	EXPORT MAC_ScanFile(IP, infile, scansize) := MACRO
		ds := DATASET(Std.File.ExternalLogicalFileName(IP, infile),{DATA1 S}, THOR )[1..scansize];
		
		OUTPUT(TABLE(ds,{hex := ds.s,txt := (STRING1)ds.s}),ALL);
		
		Rec := RECORD 
			UNSIGNED2 C;
			DATA S {MAXLENGTH(8*1024)};
		END;
		Rec XF1(ds L,INTEGER C) := TRANSFORM
			SELF.C := C;
			SELF.S := L.s;
		END;
		ds2  := PROJECT(ds,XF1(LEFT,COUNTER));
		Rec XF2(Rec L,Rec R) := TRANSFORM
			SELF.S := L.S[1 .. R.C-1] + R.S[1];
			SELF := L;
		END;
		Rolled := ROLLUP(ds2,TRUE,XF2(LEFT,RIGHT));

		OUTPUT(TRANSFER(Rolled[1].S,STRING));
	ENDMACRO;
		 
	

END;
