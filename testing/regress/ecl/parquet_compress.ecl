/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
##############################################################################*/
//class=parquet
//nothor
//noroxie
//class=embedded
//class=file
//version compressionType='UNCOMPRESSED'
//version compressionType='Snappy'
//version compressionType='GZip'
//version compressionType='Brotli'
//version compressionType='LZ4'
//version compressionType='ZSTD'

import ^ as root;
compressionType := #IFDEFINED(root.compressionType, 'UNCOMPRESSED');

IMPORT Parquet;

// Define datasets
BooleanData := DATASET([{000, 'aaa', 0},
                        {001, 'aab', false},
                        {002, 'aac', 1}], {UNSIGNED testid, STRING3 testname, BOOLEAN value});

IntegerData := DATASET([{010, 'aai', 123},
                        {011, 'aaj', -987},
                        {012, 'aak', 0}], {UNSIGNED testid, STRING3 testname, INTEGER value});

RealData := DATASET([{020, 'aas', 3.14},
                     {021, 'aat', -0.5},
                     {022, 'aau', 123.456}], {UNSIGNED testid, STRING3 testname, REAL value});

DecimalData := DATASET([{030, 'abc', 123.456789},
                        {031, 'abd', -987.654321},
                        {032, 'abe', 0.000001}], {UNSIGNED testid, STRING3 testname, DECIMAL value});

StringData := DATASET([{040, 'abm', 'Hello, World!'},
                       {041, 'abn', 'Data Science'},
                       {042, 'abo', '12345'}], {UNSIGNED testid, STRING3 testname, STRING value});

QStringData := DATASET([{050, 'abw', 'This is a "Q" string.'},
                        {051, 'abx', 'Another "example" here.'},
                        {052, 'aby', 'Qstrings are useful!'}], {UNSIGNED testid, STRING3 testname, QSTRING value});

UnicodeData := DATASET([{060, 'acg', U'こんにちは、世界！'},
                        {061, 'ach', U'Unicode characters: ḸḹḾ'},
                        {062, 'aci', U'Ṏ Beautiful Unicode Ṙ'}], {UNSIGNED testid, STRING3 testname, UNICODE value});

UTF8Data := DATASET([{070, 'acq', U'Café au lait ☕'},
                     {071, 'acr', U'🎉 UTF-8 Characters 🎉'},
                     {072, 'acs', U'Special characters: ©®™'}], {UNSIGNED testid, STRING3 testname, UTF8 value});

DataData := DATASET([{080, 'ada', x'01a48d8414d848e900'},
                     {081, 'adb', x'01f48ab446a76f8923'},
                     {082, 'adc', x'01a48ec793a76f9400'}], {UNSIGNED testid, STRING3 testname, DATA value});

VarstringData := DATASET([{090, 'adk', U'Short text'},
                          {091, 'adl', U'A longer variable-length string'},
                          {092, 'adm', U'Strings are flexible!'}], {UNSIGNED testid, STRING3 testname, VARSTRING value});

VarunicodeData := DATASET([{100, 'adu', U'Variable-length Unicode: こんにちは、世界！'},
                           {101, 'adv', U'🌟 Variable-length Unicode Symbols 🌟'},
                           {102, 'adw', U'Unicode flexibility is awesome!'}], {UNSIGNED testid, STRING3 testname, VARUNICODE value});

// Write datasets to Parquet files
SEQUENTIAL(
    PARALLEL(
        ParquetIO.write(BooleanData, '/var/lib/HPCCSystems/mydropzone/Boolean.parquet', TRUE, compressionType),
        ParquetIO.write(IntegerData, '/var/lib/HPCCSystems/mydropzone/Integer.parquet', TRUE, compressionType),
        ParquetIO.write(RealData, '/var/lib/HPCCSystems/mydropzone/Real.parquet', TRUE, compressionType),
        ParquetIO.write(DecimalData, '/var/lib/HPCCSystems/mydropzone/Decimal.parquet', TRUE, compressionType),
        ParquetIO.write(StringData, '/var/lib/HPCCSystems/mydropzone/String.parquet', TRUE, compressionType),
        ParquetIO.write(QStringData, '/var/lib/HPCCSystems/mydropzone/QString.parquet', TRUE, compressionType),
        ParquetIO.write(UnicodeData, '/var/lib/HPCCSystems/mydropzone/Unicode.parquet', TRUE, compressionType),
        ParquetIO.write(UTF8Data, '/var/lib/HPCCSystems/mydropzone/UTF8.parquet', TRUE, compressionType),
        ParquetIO.write(DataData, '/var/lib/HPCCSystems/mydropzone/Data.parquet', TRUE, compressionType),
        ParquetIO.write(VarstringData, '/var/lib/HPCCSystems/mydropzone/Varstring.parquet', TRUE, compressionType),
        ParquetIO.write(VarunicodeData, '/var/lib/HPCCSystems/mydropzone/Varunicode.parquet', TRUE, compressionType)
    )
);

// Output datasets
OUTPUT(BooleanData, NAMED('BooleanData'));
OUTPUT(IntegerData, NAMED('IntegerData'));
OUTPUT(RealData, NAMED('RealData'));
OUTPUT(DecimalData, NAMED('DecimalData'));
OUTPUT(StringData, NAMED('StringData'));
OUTPUT(QStringData, NAMED('QStringData'));
OUTPUT(UnicodeData, NAMED('UnicodeData'));
OUTPUT(UTF8Data, NAMED('UTF8Data'));
OUTPUT(DataData, NAMED('DataData'));
OUTPUT(VarstringData, NAMED('VarstringData'));
OUTPUT(VarunicodeData, NAMED('VarunicodeData'));
