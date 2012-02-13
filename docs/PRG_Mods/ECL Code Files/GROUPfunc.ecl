//
//  Example code - use without restriction.  
//
IMPORT $;

accounts := $.DeclareData.Accounts;

rec := RECORD
  accounts.PersonID;
	accounts.Account;
	accounts.opendate;
	accounts.balance;
	UNSIGNED1 Ranking := 0;
END;

tbl := TABLE(accounts,rec);

rec RankGrpAccts(rec L, rec R) := TRANSFORM
  SELF.Ranking := L.Ranking + 1;
  SELF := R;
END;
GrpRecs  := SORT(GROUP(SORT(tbl,PersonID),PersonID),-Opendate,-Balance);
i1 := ITERATE(GrpRecs,RankGrpAccts(LEFT,RIGHT));
OUTPUT(i1);

rec RankSrtAccts(rec L, rec R) := TRANSFORM
  SELF.Ranking := IF(L.PersonID = R.PersonID,L.Ranking + 1, 1);
  SELF := R;
END;
SortRecs := SORT(tbl,PersonID,-Opendate,-Balance);
i2 := ITERATE(SortRecs,RankSrtAccts(LEFT,RIGHT));
OUTPUT(i2);



//*********************************************************
// create a reasonably large dataset
bf := NORMALIZE(accounts,
                CLUSTERSIZE * 2,
								TRANSFORM(RECORDOF(accounts),SELF := LEFT));
								
// that's randomly distributed and persisted
ds0 := DISTRIBUTE(bf,RANDOM()) 								
								: PERSIST('~$.DeclareData::PERSIST::TestGroupSort');

ds1 := DISTRIBUTE(ds0,HASH32(personid));

// do a global sort
s1 := SORT(ds0,personid,opendate,-balance);
a  := OUTPUT(s1,,'~$.DeclareData::EXAMPLEDATA::TestGroupSort1',OVERWRITE);

// do a distributed local sort
s3  := SORT(ds1,personid,opendate,-balance,LOCAL);
b   := OUTPUT(s3,,'~$.DeclareData::EXAMPLEDATA::TestGroupSort2',OVERWRITE);

// do a grouped local sort
s4 := SORT(ds1,personid,LOCAL);
g2 := GROUP(s4,personid,LOCAL);
s5 := SORT(g2,opendate,-balance);
c  := OUTPUT(s5,,'~$.DeclareData::EXAMPLEDATA::TestGroupSort3',OVERWRITE);

SEQUENTIAL(a,b,c);
/**/

