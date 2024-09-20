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
//nothor,noroxie

IMPORT STD;
IMPORT PARQUET;

layout := RECORD
    DECIMAL d1;
    DECIMAL1_0 d2;
    DECIMAL16_8 d3;
    DECIMAL32_16 d4;
    UDECIMAL64_32 d5;
    DECIMAL64_32 d6;
END;

decimalData := DATASET([{(DECIMAL) '0.12345678901234567890123456789012',
                          (DECIMAL1_0) '1',
                          (DECIMAL16_8) '12345678.12345678',
                          (DECIMAL32_16) '1234567890123456.1234567890123456',
                          (UDECIMAL64_32) '12345678901234567890123456789012.12345678901234567890123456789012',
                          (DECIMAL64_32) '-12345678901234567890123456789012.12345678901234567890123456789012'
                          }], layout);

overwriteOption := TRUE;
dropzoneDirectory := Std.File.GetDefaultDropZone();
parquetFilePath := dropzoneDirectory + '/regress/decimal.parquet';

ParquetIO.Write(decimalData, parquetFilePath, overwriteOption);

parquetDecimal := ParquetIO.Read(layout, parquetFilePath);

layout joinTransform (decimalData d, parquetDecimal p) := TRANSFORM
    SELF.d1 := d.d1 - p.d1;
    SELF.d2 := d.d2 - p.d2;
    SELF.d3 := d.d3 - p.d3;
    SELF.d4 := d.d4 - p.d4;
    SELF.d5 := d.d5 - p.d5;
    SELF.d6 := d.d6 - p.d6;
END;

result := JOIN(decimalData, parquetDecimal, true, joinTransform(LEFT, RIGHT), ALL);

OUTPUT(result);
