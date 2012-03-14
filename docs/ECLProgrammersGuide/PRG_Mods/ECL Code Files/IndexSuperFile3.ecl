//
//  Example code - use without restriction.  
//
IMPORT $;
IMPORT Std;

BldSF := SEQUENTIAL(
	Std.File.CreateSuperFile($.DeclareData.SFname),
	Std.File.CreateSuperFile($.DeclareData.SKname),
	Std.File.StartSuperFileTransaction(),
	Std.File.AddSuperFile($.DeclareData.SFname,$.DeclareData.SubFile1),
	Std.File.AddSuperFile($.DeclareData.SFname,$.DeclareData.SubFile2),
	Std.File.AddSuperFile($.DeclareData.SKname,$.DeclareData.i1name),
	Std.File.AddSuperFile($.DeclareData.SKname,$.DeclareData.i2name),
	Std.File.FinishSuperFileTransaction()
  );

F1  := FETCH($.DeclareData.sf1,$.DeclareData.sk1(personid=$.DeclareData.ds1[1].personid),RIGHT.RecPos);
F2  := FETCH($.DeclareData.sf1,$.DeclareData.sk1(personid=$.DeclareData.ds2[1].personid),RIGHT.RecPos);
Get := PARALLEL(OUTPUT(F1),OUTPUT(F2));
SEQUENTIAL(BldSF,Get);

