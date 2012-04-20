IMPORT TrainingBobForeman AS X;
//Change to your module
//define an interface - normally an EXPORT attribute
IPeopleFileSearch := INTERFACE  
	EXPORT STRING25 lname_val := '';
  EXPORT STRING15 fname_val := '';
END;
//*****************************************************************
//define a function that uses that interface - normally an EXPORT attribute
Fetch_Address(IPeopleFileSearch opts) := FUNCTION

  //define passed values tests
	LnamePassed := opts.lname_val <> '';
	FnamePassed := opts.fname_val <> '';

	//define passed value filters
	LNFilter := X.File_Persons.File.LastName  = opts.lname_val;
	FNFilter := X.File_Persons.File.FirstName = opts.fname_val;

	//define the actual filter to use based on the passed values
	NameFilter := MAP(LnamePassed AND FnamePassed => LNFilter AND FNFilter,
										LnamePassed => LNFilter ,
										FnamePassed => FNFilter,
										FALSE);
								
	RETURN X.File_Persons.File(Namefilter);
END;


//*****************************************************************
//define a MODULE that implements that INTERFACE and pass that as the parameter
InstanceMod := MODULE(IPeopleFileSearch)
	EXPORT STRING25 lname_val := '';
  EXPORT STRING15 fname_val := 'Bob';
END;

InstanceMod2 := MODULE(IPeopleFileSearch)
	EXPORT STRING25 lname_val := 'Smith';
  EXPORT STRING15 fname_val := 'Namit';
END;

OUTPUT(X.File_Persons.File);
OUTPUT(Fetch_Address(InstanceMod));
OUTPUT(Fetch_Address(InstanceMod2));
