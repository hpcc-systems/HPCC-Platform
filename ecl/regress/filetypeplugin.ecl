/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

// If option is FALSE or omitted, generic disk reads are off by default but
// the use of DATASET(,TYPE()) should enable generic disk reads for that
// one activity
// #OPTION('genericDiskReadWrites', TRUE);

NamesLayout := RECORD
    STRING20        surname;
    STRING10        forename;
    INTEGER2        age := 25;
END;

//------------------------------------------------------------------------------

namesTableParquet_1 := DATASET(DYNAMIC('~parquet_in::no_options'), NamesLayout, TYPE(PARQUET), OPT);
// OUTPUT(namesTableParquet_1, {namesTableParquet_1}, '~parquet_out::no_options', TYPE(PARQUET), OVERWRITE);
OUTPUT(namesTableParquet_1);

namesTableParquet_2 := DATASET(DYNAMIC('~parquet_in::with_options'), NamesLayout, TYPE(PARQUET : RANDOMFLAG, FOO(TRUE), BAR('BAZ')), OPT);
// OUTPUT(namesTableParquet_2, {namesTableParquet_2}, '~parquet_out::with_options', TYPE(PARQUET : BACKWARDS(TRUE)), OVERWRITE);
OUTPUT(namesTableParquet_2);

// Following is present just to see what the IR output looks like
testCSV_1 := DATASET(DYNAMIC('~csv_in::no_options'), NamesLayout, CSV, OPT);
OUTPUT(testCSV_1);
testCSV_2 := DATASET(DYNAMIC('~csv_in::with_options'), NamesLayout, CSV(HEADING(1)), OPT);
OUTPUT(testCSV_2);
