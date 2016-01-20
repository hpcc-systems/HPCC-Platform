//******************************************************************
//* Generate some data
//******************************************************************

Layout := RECORD
    Integer  DID;
    String20 fname;
    String20 mname;
    String40 lname;
END;

SET OF STRING20 FirstNames := ['JAMES','JOHN','ROBERT','MICHAEL','WILLIAM','DAVID','RICHARD', 'CHARLES','JOSEPH','THOMAS','CHRISTOPHER','DANIEL','PAUL'];
STRING30 MiddleInitials := 'ABCDEFGHIJKLMNOPQRSTUVWXYZ    ';
SET OF STRING30 Surnames := ['SMITH','JOHNSON','WILLIAMS','JONES','BROWN','DAVIS','MILLER','WILSON','MOORE'];

RecordCount := 30000;
PersonCount := 10000;
GeneratorSeed := 3421513407;

Layout CreatePerson(Layout P, INTEGER C, INTEGER Seed) := TRANSFORM
    INTEGER C_HashValue := HASH32(Seed + C + 1);
    INTEGER ID := C_HashValue % PersonCount;
    INTEGER FN_HashValue := HASH32(Seed + C);
    INTEGER MN_HashValue := HASH32(FN_HashValue);
    INTEGER LN_HashValue := HASH32(MN_HashValue);

    SELF.did := ID;
    SELF.fname := FirstNames[FN_HashValue % COUNT(FirstNames) + 1];
    SELF.mname := MiddleInitials[MN_HashValue % LENGTH(MiddleInitials) + 1];
    SELF.lname := SurNames[LN_HashValue % COUNT(SurNames)];
    SELF := [];
END;

BlankPerson := DATASET([{0,'','',''}], Layout);
Persons := NORMALIZE(BlankPerson, RecordCount, CreatePerson(LEFT, COUNTER, GeneratorSeed));

//******************************************************************
//* Load it into KEL like nullable values
//******************************************************************
nstr := { STRING v := '', UNSIGNED1 f := 1 };
InLayout := RECORD
    UNSIGNED UID;
    nstr fname;
    nstr mname;
    nstr lname;
END;
KelLike := PROJECT(Persons, TRANSFORM(InLayout, SELF.UID:=LEFT.DID, SELF.fname.v:=LEFT.fname, SELF.mname.v:=LEFT.mname, SELF.lname.v:=LEFT.lname));

//******************************************************************
//* EXIST and COUNT on false value
//******************************************************************
Test := TABLE(KelLike, {UID,
            fname,                                      // Variable size record
            BOOLEAN E := EXISTS(GROUP, FALSE),
            string C := COUNT(GROUP, FALSE),            // This should not count as a variable size aggregate
            mname,
            string Z := MAX(GROUP, fname.v),
            mname2 := mname }, UID, fname, mname, lname)(E OR (integer)C > 0);

// This should be empty
OUTPUT(Test);
