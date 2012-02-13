//
//  Example code - use without restriction.  
//
IMPORT $;

F1  := FETCH($.DeclareData.sf1,$.DeclareData.sk2(personid=$.DeclareData.ds1[1].personid),RIGHT.RecPos);
F2  := FETCH($.DeclareData.sf1,$.DeclareData.sk2(personid=$.DeclareData.ds2[1].personid),RIGHT.RecPos);
Get := PARALLEL(OUTPUT(F1),OUTPUT(F2));

SEQUENTIAL(BUILDINDEX($.DeclareData.sk2,OVERWRITE),Get);
