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

//class=parquet

IMPORT Std;
IMPORT Parquet;

layout := RECORD
    STRING10 s1;
    STRING20 s2;
    STRING30 s3;
END;

stringData := DATASET([
    {'Hello', 'World', 'Test Data 1'},
    {'HPCC', 'Systems', 'Test Data 2'},
    {'Parquet', 'I/O', 'Test Data 3'}
], layout);

dropzoneDirectory := Std.File.GetDefaultDropZone();
parquetFilePath := dropzoneDirectory + '/regress/string_test.parquet';

ParquetIO.Write(stringData, parquetFilePath, TRUE);

parquetString := ParquetIO.Read(layout, parquetFilePath);

layout compareTransform(layout original, layout fromParquet) := TRANSFORM
    SELF.s1 := IF(original.s1 = fromParquet.s1, '', 'Mismatch in s1');
    SELF.s2 := IF(original.s2 = fromParquet.s2, '', 'Mismatch in s2');
    SELF.s3 := IF(original.s3 = fromParquet.s3, '', 'Mismatch in s3');
END;

result := JOIN(stringData, parquetString,
               LEFT.s1 = RIGHT.s1 AND LEFT.s2 = RIGHT.s2 AND LEFT.s3 = RIGHT.s3,
               compareTransform(LEFT, RIGHT),
               FULL OUTER);

OUTPUT(result, NAMED('ComparisonResult'));

mismatchCount := COUNT(result(s1 != '' OR s2 != '' OR s3 != ''));
OUTPUT(IF(mismatchCount = 0, 'All records match', 'Mismatches found'), NAMED('TestResult'));

