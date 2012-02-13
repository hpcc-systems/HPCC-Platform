//
//  Example code - use without restriction.  
//
IMPORT $;

r1 := RECORD
  $.DeclareData.Layout_Person;
  $.DeclareData.Layout_Accounts;
END;

r1 Xform1($.DeclareData.Person.FilePlus L, 
          $.DeclareData.Accounts R) := TRANSFORM
  SELF := L;
  SELF := R;
END;
J1 := JOIN($.DeclareData.Person.FilePlus(PersonID BETWEEN 1 AND 100),
           $.DeclareData.Accounts,
	         LEFT.PersonID=RIGHT.PersonID,
	         Xform1(LEFT,RIGHT),
	         KEYED($.DeclareData.IDX_Accounts_PersonID));			

OUTPUT(J1,ALL);
