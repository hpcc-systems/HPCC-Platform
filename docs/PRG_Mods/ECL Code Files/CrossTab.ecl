//
//  Example code - use without restriction.  
//
IMPORT $;
Person := $.DeclareData.PersonAccounts;

CountAccts := COUNT(Person.Accounts);

MyReportFormat := RECORD
	State 		 := Person.State;
 	A1 		     := SUM(GROUP,CountAccts);
	GroupCount := COUNT(GROUP);
END;

RepTable := TABLE(Person,MyReportFormat,state);
OUTPUT(RepTable);
/* The result set would look something like this:
	State		A1	GroupCount
	 AK		 1		7
	 AK		 2		3
	 AL		 1		42
	 AL		 2		54
	 AR		 1		103
	 AR		 2		89
	 AR		 3		2		*/	
	 
	 
MyReportFormat2 := RECORD
	State{cardinality(56)}	:= Person.State;
 	A1 		:= CountAccts;
	GroupCount 	:= COUNT(GROUP);
	MaleCount   := COUNT(GROUP,Person.Gender = 'M');
	FemaleCount := COUNT(GROUP,Person.Gender = 'F');
 END;

RepTable2 := TABLE(Person,MyReportFormat2,State,CountAccts );
OUTPUT(RepTable2);

IsValidType(STRING1 t) := t IN ['O', 'R', 'I'];

IsRevolv := Person.Accounts.AcctType = 'R' OR 
		        (~IsValidType(Person.Accounts.AcctType) AND 
		         Person.Accounts.Account[1] IN ['4', '5', '6']);

SetBankIndCodes := ['BB', 'ON', 'FS', 'FC'];

IsBank := Person.Accounts.IndustryCode IN SetBankIndCodes;

IsBankCard := IsBank AND IsRevolv;

AvgBal := AVE(Person.Accounts(isBankCard),Balance);
TotBal := SUM(Person.Accounts(isBankCard),Balance);
AvgHC  := AVE(Person.Accounts(isBankCard),HighCredit);

R1 := RECORD
  Person.state;
  Person.gender;
  Number 		      := COUNT(GROUP);
  AverageBal 	    := AVE(GROUP,AvgBal);
  AverageTotalBal := AVE(GROUP,TotBal);
  AverageHC 	  	:= AVE(GROUP,AvgHC);
END;

T1 := TABLE(Person, R1,  state, gender);
OUTPUT(T1);


//******************
pointRec := { REAL x, REAL y };

analyze( ds ) := MACRO

  #uniquename(rec)
  %rec% := RECORD
    c     := COUNT(GROUP),
  	sx    := SUM(GROUP, ds.x),
	  sy    := SUM(GROUP, ds.y),
	  sxx   := SUM(GROUP, ds.x * ds.x),
	  sxy   := SUM(GROUP, ds.x * ds.y),
	  syy   := SUM(GROUP, ds.y * ds.y),
	  varx  := VARIANCE(GROUP, ds.x);
  	vary  := VARIANCE(GROUP, ds.y);
	  varxy := COVARIANCE(GROUP, ds.x, ds.y);
	  rc    := CORRELATION(GROUP, ds.x, ds.y) ;
  END;	

  #uniquename(stats)
  %stats% := TABLE(ds,%rec% );
  OUTPUT(%stats%);

  OUTPUT(%stats%, { varx - (sxx-sx*sx/c)/c,
              			vary - (syy-sy*sy/c)/c,
		  							varxy - (sxy-sx*sy/c)/c,
			  						rc - (varxy/SQRT(varx*vary)) });
				
  OUTPUT(%stats%, { 'bestFit: y=' + 
	  								(STRING)((sy-sx*varxy/varx)/c) + 
		  							' + ' + 
			  						(STRING)(varxy/varx)+'x' });
ENDMACRO;

ds1 := DATASET([{1,1},{2,2},{3,3},{4,4},{5,5},{6,6}], pointRec);
ds2 := DATASET([{1.93896e+009, 2.04482e+009},
								{1.77971e+009, 8.54858e+008},
								{2.96181e+009, 1.24848e+009},
								{2.7744e+009,  1.26357e+009},
								{1.14416e+009, 4.3429e+008},
								{3.38728e+009, 1.30238e+009},
								{3.19538e+009, 1.71177e+009} ], pointRec);

ds3 := DATASET([{1, 1.00039},
								{2, 2.07702},
								{3, 2.86158},
								{4, 3.87114},
								{5, 5.12417},
								{6, 6.20283} ], pointRec);

analyze(ds1);
analyze(ds2);
analyze(ds3);

 