//noroxie

import Std.File;

rec := RECORD
 string f;
END;
ds := DATASET([{'a'}], rec);

string localDir := 'var::lib::^h^p^c^c^systems::mydropzone' : stored('localdir');

string getFName(string ext) := FUNCTION
 RETURN '~file::localhost::' + localDir + '::external' + ext;
END;

external1 := getFName('1');
external2 := getFName('2');

SEQUENTIAL(
File.DeleteLogicalFile(external1, true),
File.DeleteLogicalFile(external2, true),
OUTPUT(ds,,external1),
OUTPUT(DATASET(external1, rec, FLAT), , external2),
OUTPUT(DATASET(external2, rec, FLAT), , external1, OVERWRITE),
File.DeleteLogicalFile(external1), // NB: will fail if doesn't exist
File.DeleteLogicalFile(external2) // NB: will fail if doesn't exist
);
