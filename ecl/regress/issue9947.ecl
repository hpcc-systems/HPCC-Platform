import std;

f1 := '~thor::test::20130902::cricketscore';
f2 := '~thor::test::20130903::cricketscore';
f3 := '~thor::test::20130901::cricketscore';
f4 := '~thor::test::20130828::cricketscore';
f5 := '~thor::test::20130827::cricketscore';
f6 := '~thor::test::20130826::cricketscore';

FileDS := DATASET([{f1,f2},{f3,f4},{f5,f6}],{STRING f1, STRING f2});
CompLay := RECORD
   STRING F1;
   STRING F2;
   INTEGER CompResult
END;


CompLay FILE_TRANS(FileDS L) := TRANSFORM
   file1 := L.f1;
   file2 := L.f2;

   //FileCompRes := STD.File.CompareFiles(file1,file2);
   FileCompRes := NOTHOR(STD.File.CompareFiles(file1,file2));

   SELF.CompResult := FileCompRes;
   SELF.F1 := file1;
   SELF.F2 := file2;

END;
ComapreResult := PROJECT(FileDS, FILE_TRANS(LEFT));
ComapreResult;
