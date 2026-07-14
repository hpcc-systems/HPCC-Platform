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
//noroxie
// Requires Azure storage planes (azureblob:/azurefile:) which are not configured in the
// standard regression/CI environment, so skip there. Run manually against a real Azure plane.
//skip type==hthor requiresAzurePlanes
//skip type==thor requiresAzurePlanes
//version apiType='blob'
//version apiType='file'

IMPORT Std.Str;
IMPORT Parquet;
IMPORT ^ as root;

UNSIGNED4 numRecs := 1000;

layout := RECORD
    STRING10 key;
    UNSIGNED8 seq;
    STRING40 payload;
END;

layout mkRow(UNSIGNED4 c) := TRANSFORM
    STRING1 c1 := (STRING1)((c % 26) + 65);
    STRING1 c2 := (STRING1)(((c + 7) % 26) + 65);
    SELF.key := (STRING)c + c1 + c2;
    SELF.seq := c;
    SELF.payload := c1 + c2 + c1 + c2 + c1 + c2 + c1 + c2 +
                    c1 + c2 + c1 + c2 + c1 + c2 + c1 + c2 +
                    c1 + c2 + c1 + c2;
END;

outData := DATASET(numRecs, mkRow(COUNTER), DISTRIBUTED);

apiType := #IFDEFINED(root.apiType, 'blob');
parquetPath := 'azure' + apiType + ':data-' + apiType + '/regress/parquet/' + Str.ToLowerCase(WORKUNIT) + '_' + apiType + 'RoundTrip.parquet';

writeParquet := ParquetIO.Write(outData, parquetPath, TRUE, 'LZ4');
inParquet := ParquetIO.Read(layout, parquetPath);

compareRec := RECORD
    BOOLEAN same;
END;

compareRec compareRows(layout l, layout r) := TRANSFORM
    SELF.same := l = r;
END;

rowCompare := JOIN(outData, inParquet, LEFT.seq = RIGHT.seq, compareRows(LEFT, RIGHT), HASH);
mismatchCount := COUNT(rowCompare(NOT same));
countMatch := COUNT(outData) = COUNT(inParquet);

resultRec := RECORD
    STRING test;
    STRING value;
END;

results := DATASET([
    {'RowsRead', (STRING)COUNT(inParquet)},
    {'MismatchCount', (STRING)mismatchCount},
    {'Status', IF(countMatch AND mismatchCount = 0, 'Pass', 'Fail')}
], resultRec);

SEQUENTIAL(
    writeParquet,
    OUTPUT(results, NAMED('ParquetAzureAPIRoundTrip'))
);
