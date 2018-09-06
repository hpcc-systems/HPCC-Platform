//
//  Example code - use without restriction.  
//
IMPORT $;
IMPORT Std;

/* this allows you to easily start over again */ 
  SEQUENTIAL(
		Std.File.StartSuperFileTransaction(),

		IF(Std.File.SuperFileExists($.DeclareData.BaseFile),
			 Std.File.ClearSuperFile($.DeclareData.BaseFile)),
		IF(Std.File.SuperFileExists($.DeclareData.WeeklyFile),
			 Std.File.ClearSuperFile($.DeclareData.WeeklyFile)),
		IF(Std.File.SuperFileExists($.DeclareData.DailyFile),
			 Std.File.ClearSuperFile($.DeclareData.DailyFile)),
		IF(Std.File.SuperFileExists($.DeclareData.AllPeople),
			 Std.File.ClearSuperFile($.DeclareData.AllPeople)),
		
		Std.File.FinishSuperFileTransaction(),
    OUTPUT('Done Clearing'),

		IF(Std.File.SuperFileExists($.DeclareData.BaseFile),
			 Std.File.DeleteSuperFile($.DeclareData.BaseFile)),
		IF(Std.File.SuperFileExists($.DeclareData.WeeklyFile),
			 Std.File.DeleteSuperFile($.DeclareData.WeeklyFile)),
		IF(Std.File.SuperFileExists($.DeclareData.DailyFile),
			 Std.File.DeleteSuperFile($.DeclareData.DailyFile)),
		IF(Std.File.SuperFileExists($.DeclareData.AllPeople),
			 Std.File.DeleteSuperFile($.DeclareData.AllPeople)),

    OUTPUT('Done Deleting')
		);

