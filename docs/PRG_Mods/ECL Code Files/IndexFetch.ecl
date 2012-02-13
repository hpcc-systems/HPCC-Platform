//
//  Example code - use without restriction.  
//
IMPORT $;

F1 := FETCH($.DeclareData.Person.FilePlus,
            $.DeclareData.IDX_Person_PersonID(PersonID=1),  
					  RIGHT.RecPos);
					 
OUTPUT(F1);					 

r := RECORD
  STRING FullName;
	STRING Address;
	STRING CSZ;
END;

r Xform($.DeclareData.Person.FilePlus L) := TRANSFORM
  SELF.Fullname := TRIM(L.Firstname) + TRIM(' ' + L.MiddleInitial) + ' ' + L.Lastname;
	SELF.Address  := L.Street;
	SELF.CSZ      := TRIM(L.City) + ', ' + L.State + ' ' + L.Zip;
END;

F2 := FETCH($.DeclareData.Person.FilePlus,
            $.DeclareData.IDX_Person_PersonID(PersonID=1),
					  RIGHT.RecPos,
						Xform(LEFT));
						
OUTPUT(F2);					 
