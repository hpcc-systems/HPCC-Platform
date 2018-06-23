//
//  Example code - use without restriction.  
//
IMPORT $;

r1 := RECORD
  $.DeclareData.Layout_Person;
	$.DeclareData.Layout_Accounts;
END;

r1 Xform($.DeclareData.Person.FilePlus L, $.DeclareData.AcctSK R) := TRANSFORM
  SELF := L;
  SELF := R;
END;

J3 := JOIN($.DeclareData.Person.FilePlus(PersonID BETWEEN 1 AND 100),
           $.DeclareData.AcctSK,
					 LEFT.PersonID=RIGHT.PersonID,
					 Xform(LEFT,RIGHT));		
					 
OUTPUT(J3,ALL);
// output($.DeclareData.AcctSK);