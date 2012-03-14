//
//  Example code - use without restriction.  
//
IMPORT $;
IMPORT Std;

SEQUENTIAL(
	Std.File.CreateSuperFile($.DeclareData.BaseFile),
	Std.File.StartSuperFileTransaction(),
	Std.File.AddSuperFile($.DeclareData.BaseFile,$.DeclareData.SubFile1),
	Std.File.AddSuperFile($.DeclareData.BaseFile,$.DeclareData.SubFile2),
	Std.File.FinishSuperFileTransaction());



  // IMPORT $;
  // COUNT($.DeclareData.SuperFile1(PersonID <> 0));
  // OUTPUT($.DeclareData.SuperFile1);


