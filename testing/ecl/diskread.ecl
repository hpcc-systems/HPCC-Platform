/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

//UseStandardFiles
// straight disk count
#option('workflow', 0);
/*
count(DG_FlatFile);
count(DG_FlatFileEvens);
//count(DG_CSVFile);
//count(DG_XMLFile);
count(DG_VarFile);

// straight disk read

COUNT(DEDUP(DG_FlatFile, RECORD));
COUNT(DEDUP(DG_FlatFileEvens, RECORD));
//COUNT(DEDUP(DG_CSVFile, RECORD));
//COUNT(DEDUP(DG_XMLFile, RECORD));
COUNT(DEDUP(DG_VarFile, RECORD));

// filtered disk read

output(DG_FlatFile(DG_firstname='CLAIRE'));
output(DG_FlatFileEvens(DG_firstname='CLAIRE'));
//output(DG_CSVFile(DG_firstname='CLAIRE'));
//output(DG_XMLFile(DG_firstname='CLAIRE'));
output(DG_VarFile(DG_firstname='CLAIRE'));

// keyed disk read

//output(DG_FlatFile(KEYED(DG_firstname='CLAIRE')));
//output(DG_FlatFileEvens(KEYED(DG_firstname='CLAIRE')));
//output(DG_CSVFile(KEYED(DG_firstname='CLAIRE')));
//output(DG_XMLFile(KEYED(DG_firstname='CLAIRE')));
//output(DG_VarFile(KEYED(DG_firstname='CLAIRE')));
*/

// now repeat with preload....


DG_FlatFile_pl0 := PRELOAD(DG_FlatFile);
DG_FlatFileEvens_pl0 := PRELOAD(DG_FlatFileEvens);
//DG_CSVFile_pl0 := PRELOAD(DG_CSVFile);
//DG_XMLFile_pl0 := PRELOAD(DG_XMLFile);
DG_VarFile_pl0 := PRELOAD(DG_VarFile);

// straight disk count

count(DG_FlatFile_pl0);
count(DG_FlatFileEvens_pl0);
//count(DG_CSVFile_pl0);
//count(DG_XMLFile_pl0);
count(DG_VarFile_pl0);

// straight disk read

COUNT(DEDUP(DG_FlatFile_pl0, RECORD));
COUNT(DEDUP(DG_FlatFileEvens_pl0, RECORD));
//COUNT(DEDUP(DG_CSVFile_pl0, RECORD));
//COUNT(DEDUP(DG_XMLFile_pl0, RECORD));
COUNT(DEDUP(DG_VarFile_pl0, RECORD));

// filtered disk read

output(DG_FlatFile_pl0(DG_firstname='CLAIRE'));
output(DG_FlatFileEvens_pl0(DG_firstname='CLAIRE'));
//output(DG_CSVFile_pl0(DG_firstname='CLAIRE'));
//output(DG_XMLFile_pl0(DG_firstname='CLAIRE'));
output(DG_VarFile_pl0(DG_firstname='CLAIRE'));

// keyed disk read

output(DG_FlatFile_pl0(KEYED(DG_firstname='CLAIRE')));
output(DG_FlatFileEvens_pl0(KEYED(DG_firstname='CLAIRE')));
//output(DG_CSVFile_pl0(KEYED(DG_firstname='CLAIRE')));
//output(DG_XMLFile_pl0(KEYED(DG_firstname='CLAIRE')));
output(DG_VarFile_pl0(KEYED(DG_firstname='CLAIRE')));

/*
// now repeat with preload(n)....

DG_FlatFile_pl2 := PRELOAD(DG_FlatFile, 2);
DG_FlatFileEvens_pl2 := PRELOAD(DG_FlatFileEvens, 2);
//DG_CSVFile_pl2 := PRELOAD(DG_CSVFile, 2);
//DG_XMLFile_pl2 := PRELOAD(DG_XMLFile, 2);
DG_VarFile_pl2 := PRELOAD(DG_VarFile, 2);

// straight disk count

count(DG_FlatFile_pl2);
count(DG_FlatFileEvens_pl2);
//count(DG_CSVFile_pl2);
//count(DG_XMLFile_pl2);
count(DG_VarFile_pl2);

// straight disk read

COUNT(DEDUP(DG_FlatFile_pl2, RECORD));
COUNT(DEDUP(DG_FlatFileEvens_pl2, RECORD));
//COUNT(DEDUP(DG_CSVFile_pl2, RECORD));
//COUNT(DEDUP(DG_XMLFile_pl2, RECORD));
COUNT(DEDUP(DG_VarFile_pl2, RECORD));

// filtered disk read

output(DG_FlatFile_pl2(DG_firstname='CLAIRE'));
output(DG_FlatFileEvens_pl2(DG_firstname='CLAIRE'));
//output(DG_CSVFile_pl2(DG_firstname='CLAIRE'));
//output(DG_XMLFile_pl2(DG_firstname='CLAIRE'));
output(DG_VarFile_pl2(DG_firstname='CLAIRE'));

// keyed disk read

output(DG_FlatFile_pl2(KEYED(DG_firstname='CLAIRE')));
output(DG_FlatFileEvens_pl2(KEYED(DG_firstname='CLAIRE')));
//output(DG_CSVFile_pl2(KEYED(DG_firstname='CLAIRE')));
//output(DG_XMLFile_pl2(KEYED(DG_firstname='CLAIRE')));
output(DG_VarFile_pl2(KEYED(DG_firstname='CLAIRE')));

*/

// MORE - tests: make sure that we can handle a combination of keyed/unkeyed filtering...
// MORE - preload and non-preload for the same dataset is not really testing what you think, in roxie
// MORE - in fact, unless queries get deleted, roxie will use first sight of the file even if we separate into multiple .ecl files.
