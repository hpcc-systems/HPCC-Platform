//
//  Example code - use without restriction.  
//
IMPORT $;
IMPORT Std;

SEQUENTIAL(
	Std.File.CreateSuperFile($.DeclareData.AllPeople),
	Std.File.CreateSuperFile($.DeclareData.WeeklyFile),
	Std.File.CreateSuperFile($.DeclareData.DailyFile),
	Std.File.StartSuperFileTransaction(),
	Std.File.AddSuperFile($.DeclareData.AllPeople,$.DeclareData.BaseFile),
	Std.File.AddSuperFile($.DeclareData.AllPeople,$.DeclareData.WeeklyFile),
	Std.File.AddSuperFile($.DeclareData.AllPeople,$.DeclareData.DailyFile),
	Std.File.FinishSuperFileTransaction());
