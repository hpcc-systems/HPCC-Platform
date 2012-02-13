//
//  Example code - use without restriction.  
//
IMPORT $;
IMPORT Std;

SEQUENTIAL(
	Std.File.StartSuperFileTransaction(),
	Std.File.AddSuperFile($.DeclareData.WeeklyFile,$.DeclareData.DailyFile,,TRUE),
	Std.File.ClearSuperFile($.DeclareData.DailyFile	),
	Std.File.FinishSuperFileTransaction());
