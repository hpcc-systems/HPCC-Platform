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
//nothor

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

basePath := Std.File.GetDefaultDropZone() + '/regress/parquet/'; 
parquetFilePath := basePath + 'stringData.parquet';

ParquetIO.Write(stringData, parquetFilePath, TRUE); 
parquetString := ParquetIO.Read(layout, parquetFilePath);

result := JOIN(stringData, parquetString,
              LEFT.s1 = RIGHT.s1 AND 
              LEFT.s2 = RIGHT.s2 AND 
              LEFT.s3 = RIGHT.s3,
              TRANSFORM(layout, 
                  SELF := LEFT
              ),
              ALL);

OUTPUT(result, NAMED('ComparisonResult')); 

mismatchCount := COUNT(stringData) - COUNT(result);

OUTPUT(IF(mismatchCount = 0, 'All records match', 'Mismatches found'), NAMED('TestResult'));