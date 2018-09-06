//
//  Example code - use without restriction.  
//
IMPORT Std;

rec := RECORD
	  STRING3 code;
	  STRING  desc;
	  STRING  zone;
END;
       
srcnode := '10.173.9.4';
srcdir  := '/c$/training/import/DFUtest/';

dir := Std.File.RemoteDirectory(srcnode,srcdir,'BIG*0?.csv',true);

NOTHOR(
  SEQUENTIAL(
    Std.File.DeleteSuperFile('MultiSuper1'),
    Std.File.CreateSuperFile('MultiSuper1'),
    Std.File.StartSuperFileTransaction(),
    APPLY(dir,Std.File.AddSuperFile('MultiSuper1',Std.File.ExternalLogicalFileName(srcnode,srcdir+name))),
    Std.File.FinishSuperFileTransaction()));

F1 := DATASET('MultiSuper1', rec, csv);

OUTPUT(F1,,'~RTTEST::testmulti1',overwrite);
// OUTPUT(F1);
