/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

//class=parquet

// This test writes Parquet files using ParquetIO.Write called from within a FunctionMacro.
// It tests that the Parquet plugin properly handles write operations when invoked from a function macro context.

IMPORT Std;
IMPORT Parquet;

myRec := RECORD
    STRING name;
    UNSIGNED4 id;
END;

testData := DATASET([{'Alice', 1},{'Bob', 2},{'Charlie', 3}], myRec);

basePath := Std.File.GetDefaultDropZone() + '/regress/parquet/';

// ParquetIO.Write called from a FunctionMacro
writeFunctionMacro(dset, fileName) := FUNCTIONMACRO
    outPath := basePath + fileName + '_macro.parquet';
    RETURN ParquetIO.Write(dset, outPath, TRUE, 'LZ4');
ENDMACRO;

// Read back the data to verify
readMacroData := ParquetIO.Read(myRec, basePath + 'testwrite_macro.parquet');

SEQUENTIAL(
    writeFunctionMacro(testData, 'testwrite'),
    OUTPUT(readMacroData, NAMED('MacroWrite'))
);
