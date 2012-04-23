IMPORT $;
Person := $.File_People;
OUTPUT(Person);
PtblRec := RECORD
  STRING2  State := Person.state;
  STRING20 City  := Person.city;
  STRING25 Lname := Person.lastname;
  STRING15 Fname := Person.firstname;
END;

DataFile := 'BFTEMP::TestFetch';
KeyFile  := 'BFTEMP::TestFetchKey';

PtblOut := OUTPUT(TABLE(Person(lastname[1..3]='Wik'),PtblRec),,DataFile,OVERWRITE);

Ptbl := DATASET(DataFile,
		            {PtblRec,UNSIGNED8 RecPos {virtual(fileposition)}},
		            FLAT);

AlphaInStateCity := INDEX(Ptbl,
				                  {state,city,lname,fname,RecPos},
				                  KeyFile);

Bld := BUILD(AlphaInStateCity,OVERWRITE);


AlphaPeople := FETCH(Ptbl, 
										 AlphaInStateCity(WILD(state),
																			WILD(city),
																			KEYED(Lname='Wikoff')
																			), 
										 RIGHT.RecPos);

OutFile := OUTPUT(CHOOSEN(AlphaPeople,10));

SEQUENTIAL(PtblOut,Bld,OutFile)
/* Expected output: 3 tabs -- 66 rows on tab 1-2 and 1 row on tab 3 */