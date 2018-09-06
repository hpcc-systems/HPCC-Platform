//
//  Example code - use without restriction.  
//
IMPORT $;
IMPORT Std;

/* this allows you to easily start over again */ 
  SEQUENTIAL(
		Std.File.StartSuperFileTransaction(),

		IF(Std.File.SuperFileExists($.DeclareData.AcctSKname),
			 Std.File.ClearSuperFile($.DeclareData.AcctSKname)),
		
		Std.File.FinishSuperFileTransaction(),
    OUTPUT('Done Clearing'),

		IF(Std.File.SuperFileExists($.DeclareData.AcctSKname),
			 Std.File.DeleteSuperFile($.DeclareData.AcctSKname)),

    OUTPUT('Done Deleting')
		);

