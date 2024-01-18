/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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

NamesLayout := RECORD
    STRING20        surname;
    STRING10        forename;
    INTEGER2        age := 25;
END;

//==============================================================================
// The following are built-in file formats using the new syntax
//==============================================================================

namesTableFlat_1 := DATASET(DYNAMIC('x'), NamesLayout, TYPE(FLAT), __COMPRESSED__, __GROUPED__, OPT);
OUTPUT(namesTableFlat_1, {namesTableFlat_1}, '~filetypetest::namestableflat_1', OVERWRITE);

namesTableThor_1 := DATASET(DYNAMIC('x'), NamesLayout, TYPE(THOR), OPT);
OUTPUT(namesTableThor_1, {namesTableThor_1}, '~filetypetest::namestablethor_1', OVERWRITE);

namesTableCSV_1 := DATASET(DYNAMIC('x'), NamesLayout, TYPE(CSV), OPT);
// OUTPUT(namesTableCSV_1, {namesTableCSV_1}, '~filetypetest::namestablecsv_1', TYPE(CSV), OVERWRITE);

namesTableCSV_2 := DATASET(DYNAMIC('x'), NamesLayout, TYPE(CSV : HEADING(1), SEPARATOR([',', '==>']), QUOTE(['\'', '"', '$$']), TERMINATOR(['\r\n', '\r', '\n']), NOTRIM, UTF8, MAXLENGTH(10000)), OPT);
// OUTPUT(namesTableCSV_2, {namesTableCSV_2}, '~filetypetest::namestablecsv_2', TYPE(CSV : SEPARATOR('\t')), OVERWRITE);

namesTableXML_1 := DATASET(DYNAMIC('x'), NamesLayout, TYPE(XML), OPT);
// OUTPUT(namesTableXML_1, {namesTableXML_1}, '~filetypetest::namestablexml_1', TYPE(XML), OVERWRITE);

namesTableXML_2 := DATASET(DYNAMIC('x'), NamesLayout, TYPE(XML : '/', NOROOT), OPT);
// OUTPUT(namesTableXML_2, {namesTableXML_2}, '~filetypetest::namestablexml_2', TYPE(XML : HEADING('<foo>', '</foo>')), OVERWRITE);

namesTableJSON_1 := DATASET(DYNAMIC('x'), NamesLayout, TYPE(JSON), OPT);
// OUTPUT(namesTableJSON_1, {namesTableJSON_1}, '~filetypetest::namestablejson_1', TYPE(JSON), OVERWRITE);

namesTableJSON_2 := DATASET(DYNAMIC('x'), NamesLayout, TYPE(JSON : '/', NOROOT), OPT);
// OUTPUT(namesTableJSON_2, {namesTableJSON_2}, '~filetypetest::namestablejson_2', TYPE(JSON : TRIM), OVERWRITE);

//==============================================================================
// The following are new file formats with new syntax
//==============================================================================

namesTableParquet_1 := DATASET(DYNAMIC('x'), NamesLayout, TYPE(PARQUET), OPT);
// OUTPUT(namesTableParquet_1, {namesTableParquet_1}, '~filetypetest::namestableparquet_1', TYPE(PARQUET), OVERWRITE);
