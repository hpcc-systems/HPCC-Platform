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
//fail
//nothor
//noroxie

// This test writes Parquet files using ParquetIO.Write called from within a Function.
// When called from a function, the Record structure of the dataset is not passed to the
// plugin. If the function is called on datasets of different record structures, there
// is no way to pass that information to the plugin (aside from FUNCTIONMACRO).
// (Dataset(myRec) dset works for static record structures).

IMPORT Std;
IMPORT Parquet;

myRec := RECORD
    STRING name;
    UNSIGNED4 id;
END;

testData := DATASET([{'Alice', 1},{'Bob', 2},{'Charlie', 3}], myRec);

basePath := Std.File.GetDefaultDropZone() + '/regress/parquet/';

// ParquetIO.Write called from a Function
writeFunction(Dataset dset, String fileName) := FUNCTION
    overwriteOption := TRUE;
    compressionOption := 'LZ4';
    outPath := basePath + fileName + '_function.parquet';
    parquetWrite := ParquetIO.Write(DISTRIBUTE(dset, SKEW(1)), outPath, overwriteOption, compressionOption);
    RETURN parquetWrite;
END;

// Read back the data to verify
readFunctionData := ParquetIO.Read(myRec, basePath + 'testwrite_function.parquet');

SEQUENTIAL(
    writeFunction(testData, 'testwrite'),
    OUTPUT(readFunctionData, NAMED('FunctionWrite'))
);
