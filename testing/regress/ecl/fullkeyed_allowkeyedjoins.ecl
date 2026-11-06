//class=file
//class=index
//nokey

#option('allowKeyedJoins', false);
#option('translateDFSlayouts', true);
#option('layoutTranslation', true);

import $.setup;
Files := setup.Files(false, false, true);

leftRows := DATASET([{Files.DG_fnames[1]}], { STRING DG_FirstName });
joinedRows := JOIN(leftRows, Files.DG_FlatFile, LEFT.DG_firstname = RIGHT.DG_firstname, KEYED(Files.DG_indexFile), KEYED);
OUTPUT(COUNT(joinedRows));