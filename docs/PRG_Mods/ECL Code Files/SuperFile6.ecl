//
//  Example code - use without restriction.  
//
IMPORT $;
IMPORT Std;

/* Run the OUTPUT first, then toggle the comments (use Ctrl+A, Ctrl+Q) and run the transaction*/
OUTPUT($.DeclareData.SuperFile2,,'~$.DeclareData::SUPERFILE::People14',OVERWRITE);


// IMPORT $;
// IMPORT Std;
// SEQUENTIAL(
	// Std.File.StartSuperFileTransaction(),
	// Std.File.ClearSuperFile($.DeclareData.BaseFile),
	// Std.File.ClearSuperFile($.DeclareData.WeeklyFile),
	// Std.File.ClearSuperFile($.DeclareData.DailyFile),
	// Std.File.AddSuperFile($.DeclareData.BaseFile,'~$.DeclareData::SUPERFILE::People14'),
	// Std.File.FinishSuperFileTransaction());
