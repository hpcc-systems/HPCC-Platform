//
//  Example code - use without restriction.  
//
Layout_Person := RECORD
  UNSIGNED1 PersonID;
  STRING15  FirstName;
  STRING25  LastName;
END;
Person := DATASET([{1,'Fred','Smith'},
                   {2,'Joe','Blow'},
									 {3,'Jane','Smith'}],Layout_Person);

Layout_Accounts := RECORD
  STRING10  Account;
  UNSIGNED4 Balance;
END;

Layout_Accounts_Link := RECORD
  UNSIGNED1 PersonID;
  Layout_Accounts;				//nested RECORD structure
END;

Accounts := DATASET([{1,'45621234',452},
                     {1,'55621234',5000},
										 {2,'45629876',4215},
										 {3,'45628734',8525}],Layout_Accounts_Link);


Layout_Combined := RECORD
  Layout_Person;
  DATASET(Layout_Accounts) Accounts;	//nested child DATASET 
END;

P_recs := PROJECT(Person,TRANSFORM(Layout_Combined,SELF := LEFT; SELF := []));

Layout_Combined CombineRecs(Layout_Combined L,
														Layout_Accounts_Link R) := TRANSFORM
  SELF.Accounts := L.Accounts + ROW({R.Account,R.Balance}, Layout_Accounts);
  SELF := L;
END;							//input and output

NestedPeopleAccts := DENORMALIZE(P_recs,
                                 Accounts,
																 LEFT.personid=RIGHT.personid,
																 CombineRecs(LEFT,RIGHT));

OUTPUT(NestedPeopleAccts);
