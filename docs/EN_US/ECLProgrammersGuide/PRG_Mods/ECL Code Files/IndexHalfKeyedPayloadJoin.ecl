//
//  Example code - use without restriction.  
//
IMPORT $;

r1 := RECORD
  $.DeclareData.Layout_Person;
	$.DeclareData.Layout_Accounts;
END;

r1 Xform($.DeclareData.Person.FilePlus L, $.DeclareData.IDX_Accounts_PersonID_Payload R) := TRANSFORM
  SELF := L;
  SELF := R;
END;

J2 := JOIN($.DeclareData.Person.FilePlus(PersonID BETWEEN 1 AND 100),
           $.DeclareData.IDX_Accounts_PersonID_Payload,
					 LEFT.PersonID=RIGHT.PersonID,
					 Xform(LEFT,RIGHT));		
					 
OUTPUT(J2,ALL);
