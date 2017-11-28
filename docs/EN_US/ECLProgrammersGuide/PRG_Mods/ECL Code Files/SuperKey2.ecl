//
//  Example code - use without restriction.  
//
IMPORT $;
IMPORT Std;

SEQUENTIAL(
	IF(~Std.File.SuperFileExists($.DeclareData.AcctSKname),
	   Std.File.CreateSuperFile($.DeclareData.AcctSKname)),
	Std.File.StartSuperFileTransaction(),
	Std.File.ClearSuperFile($.DeclareData.AcctSKname),
	Std.File.AddSuperFile($.DeclareData.AcctSKname,$.DeclareData.SubIDX1),
	Std.File.AddSuperFile($.DeclareData.AcctSKname,$.DeclareData.SubIDX2),
	Std.File.FinishSuperFileTransaction());



