//
//  Example code - use without restriction.  
//
IMPORT $;

Bld := PARALLEL(BUILDINDEX($.DeclareData.i1,OVERWRITE),BUILDINDEX($.DeclareData.i2,OVERWRITE));

F1 := FETCH($.DeclareData.ds1,$.DeclareData.i1(personid=$.DeclareData.ds1[1].personid),RIGHT.RecPos);
F2 := FETCH($.DeclareData.ds2,$.DeclareData.i2(personid=$.DeclareData.ds2[1].personid),RIGHT.RecPos);

Get := PARALLEL(OUTPUT(F1),OUTPUT(F2));
SEQUENTIAL(Bld,Get);




