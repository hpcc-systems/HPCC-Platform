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
//noroxie
//version compressionType='UNCOMPRESSED'
//version compressionType='Snappy'
//version compressionType='GZip'
//version compressionType='Brotli'
//version compressionType='LZ4'
//version compressionType='ZSTD'

import STD.File AS FileServices;
import ^ as root;
compressionType := #IFDEFINED(root.compressionType, 'UNCOMPRESSED');

IMPORT Std;
IMPORT Parquet;

dropzoneDirectory := Std.File.GetDefaultDropZone() + '/regress/parquet/' + WORKUNIT + '-';

// Covers data types supported by ECL and Arrow

// ======================== BOOLEAN ========================

booleanRecord := {UNSIGNED testid, STRING3 testname, BOOLEAN value};

booleanDatasetOut := DATASET([
    {000, 'aaa', TRUE},
    {001, 'aab', FALSE}
], booleanRecord);

booleanDatasetIn := ParquetIO.Read(booleanRecord, dropzoneDirectory + 'BooleanTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} booleanJoin (booleanDatasetOut a, booleanDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

booleanCompareResult := JOIN(booleanDatasetOut, booleanDatasetIn, LEFT.testid = RIGHT.testid, booleanJoin(LEFT, RIGHT), ALL);

booleanResult := IF(COUNT(booleanCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: Boolean data mismatch');

// ======================== INTEGER ========================

integerRecord := {UNSIGNED testid, STRING testname, INTEGER1 v1, INTEGER2 v2, INTEGER3 v3, INTEGER4 v4, INTEGER5 v5, INTEGER6 v6, INTEGER7 v7, INTEGER v8};

integerDatasetOut := DATASET([
    {100, 'minvalues', -128, -32768, -8388608, -2147483648, -549755813888, -140737488355328, -36028797018963968, -9223372036854775808},
    {101, 'maxvalues', 127, 32767, 8388607, 2147483647, 549755813887, 140737488355327, 36028797018963967, 9223372036854775807}
], integerRecord);

integerDatasetIn := ParquetIO.Read(integerRecord, dropzoneDirectory + 'IntegerTest.parquet');

{UNSIGNED testid, STRING testname, BOOLEAN isEqual} integerJoin (integerDatasetOut a, integerDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.v1 = b.v1 AND
                    a.v2 = b.v2 AND
                    a.v3 = b.v3 AND
                    a.v4 = b.v4 AND
                    a.v5 = b.v5 AND
                    a.v6 = b.v6 AND
                    a.v7 = b.v7 AND
                    a.v8 = b.v8;
END;

integerCompareResult := JOIN(integerDatasetOut, integerDatasetIn, LEFT.testid = RIGHT.testid, integerJoin(LEFT, RIGHT), ALL);

integerResult := IF(COUNT(integerCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: Integer data mismatch');

// ======================== UNSIGNED ========================

unsignedRecord := {UNSIGNED testid, STRING testname, UNSIGNED1 v1, UNSIGNED2 v2, UNSIGNED3 v3, UNSIGNED4 v4, UNSIGNED5 v5, UNSIGNED6 v6, UNSIGNED7 v7, UNSIGNED v8};

unsignedDatasetOut := DATASET([
    {200, 'minvalues', 0, 0, 0, 0, 0, 0, 0, 0},
    {201, 'maxvalues', 255, 65535, 16777215, 4294967295, 1099511627775, 281474976710655, 72057594037927935, 18446744073709551615}
], unsignedRecord);

unsignedDatasetIn := ParquetIO.Read(unsignedRecord, dropzoneDirectory + 'UnsignedTest.parquet');

{UNSIGNED testid, STRING testname, BOOLEAN isEqual} unsignedJoin (unsignedDatasetOut a, unsignedDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.v1 = b.v1 AND
                    a.v2 = b.v2 AND
                    a.v3 = b.v3 AND
                    a.v4 = b.v4 AND
                    a.v5 = b.v5 AND
                    a.v6 = b.v6 AND
                    a.v7 = b.v7 AND
                    a.v8 = b.v8;
END;

unsignedCompareResult := JOIN(unsignedDatasetOut, unsignedDatasetIn, LEFT.testid = RIGHT.testid, unsignedJoin(LEFT, RIGHT), ALL);

unsignedResult := IF(COUNT(unsignedCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: Unsigned data mismatch');

// ======================== REAL ========================

realRecord := {UNSIGNED testid, STRING testname, REAL4 v4, REAL v8};

realDatasetOut := DATASET([
    {300, 'maxvalues', 3.402823E+038, 1.7976931348623157E+308},
    {301, 'minvalues', 1.175494E-38, 2.2250738585072014E-308},
    {302, 'maxdigits', -4.748307D, -1.41421356237309D},
    {303, 'zero', 0.0D, 0.0D},
    {304, 'positive', 1.23D, 6735.12485D},
    {305, 'negative', -9.87D, -234.853D}
], realRecord);

realDatasetIn := ParquetIO.Read(realRecord, dropzoneDirectory + 'RealTest.parquet');

{UNSIGNED testid, STRING testname, BOOLEAN isEqual} realJoin (realDatasetOut a, realDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.v4 = b.v4 AND
                    a.v8 = b.v8;
END;

realCompareResult := JOIN(realDatasetOut, realDatasetIn, LEFT.testid = RIGHT.testid, realJoin(LEFT, RIGHT), ALL);

realResult := IF(COUNT(realCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: Real data mismatch');

// ======================== DECIMAL ========================

decimalRecord := {UNSIGNED testid, STRING3 testname, DECIMAL10_2 value};

decimalDatasetOut := DATASET([
    {400, 'aax', 12.34D},
    {401, 'aay', -56.78D},
    {402, 'abb', 0.00D}
], decimalRecord);

decimalDatasetIn := ParquetIO.Read(decimalRecord, dropzoneDirectory + 'DecimalTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} decimalJoin (decimalDatasetOut a, decimalDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

decimalCompareResult := JOIN(decimalDatasetOut, decimalDatasetIn, LEFT.testid = RIGHT.testid, decimalJoin(LEFT, RIGHT), ALL);

decimalResult := IF(COUNT(decimalCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: Decimal data mismatch');

// ======================== STRING ========================

stringRecord := {UNSIGNED testid, STRING5 name, STRING value};

stringDatasetOut := DATASET([
    {500, 'asciichars', ' !"#$F%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~'},
    {501, 'asciicodes0', '\040\041\042\043\044\045\046\047\050\051\052\053\054\055\056\057\060\061\062\063\064\065\066\067\070'},
    {502, 'asciicodes1', '\071\072\073\074\075\076\077\100\101\102\103\104\105\106\107\110\111\112\113\114\115\116\117\120\121'},
    {503, 'asciicodes2', '\122\123\124\125\126\127\130\131\132\133\134\135\136\137\140\141\142\143\144\145\146\147\150\151\152'},
    {504, 'asciicodes3', '\153\154\155\156\157\160\161\162\163\164\165\166\167\170\171\172\173\174\175\176'},
    {505, 'data0', (STRING)X'FEDCBA9876543210'},
    {506, 'data1', (STRING)X'00FF00FF00FF00FF'}
], stringRecord);

stringDatasetIn := ParquetIO.Read(stringRecord, dropzoneDirectory + 'StringTest.parquet');

{STRING5 name, BOOLEAN value} stringJoin (stringDatasetOut a, stringDatasetIn b) := TRANSFORM
    SELF.name := a.name;
    SELF.value := a.value = b.value;
END;

stringCompareResult := JOIN(stringDatasetOut, stringDatasetIn, LEFT.testid = RIGHT.testid, stringJoin(LEFT, RIGHT), ALL);

stringResult := IF(COUNT(stringCompareResult(value = FALSE)) = 0, 'Pass', 'Fail: String data mismatch');

// ======================== DATA ========================

DATA10 REALToBinary(REAL val) := (DATA10)val;
DATA16 REALToLargeBinary(REAL val) := (DATA16)val;

dataRecord := RECORD
    UNSIGNED testid;
    STRING5 name;
    DATA value1;
    DATA10 value2;
    DATA16 value3;
END;

dataDatasetOut := DATASET([
    {600, 'abh', X'0123456789ABCDEF', (DATA10)X'0123456789ABCDEF', (DATA16)X'0123456789ABCDEF01234567'},
    {601, 'abi', X'FEDCBA9876543210', (DATA10)X'FEDCBA9876543210', (DATA16)X'FEDCBA9876543210FEDCBA98'},
    {602, 'abl', X'1234567890ABCDEF', (DATA10)X'1234567890ABCDEF', (DATA16)X'1234567890ABCDEF12345678'},
    {603, 'pos', X'0000000000000000', REALToBinary(3.14159), REALToLargeBinary(3.14159)},
    {604, 'neg', X'0000000000000000', REALToBinary(-2.71828), REALToLargeBinary(-2.71828)}
], dataRecord);

dataDatasetIn := ParquetIO.Read(dataRecord, dropzoneDirectory + 'DataTest.parquet');

{UNSIGNED testid, STRING5 name, BOOLEAN allEqual} dataJoin(dataDatasetOut a, dataDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.name := a.name;
    SELF.allEqual := a.value1 = b.value1 AND
                     a.value2 = b.value2 AND
                     a.value3 = b.value3;
END;

dataCompareResult := JOIN(dataDatasetOut, dataDatasetIn, LEFT.testid = RIGHT.testid, dataJoin(LEFT, RIGHT), ALL);
dataResult := IF(COUNT(dataCompareResult(allEqual = FALSE)) = 0, 'Pass', 'Fail: Data mismatch');

// ======================== VARSTRING ========================

varstringRecord := {UNSIGNED testid, STRING testname, VARSTRING value};

varStringDatasetOut := DATASET([
    {700, 'asciichars', ' !"#$F%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~'},
    {701, 'asciicodes0', '\040\041\042\043\044\045\046\047\050\051\052\053\054\055\056\057\060\061\062\063\064\065\066\067\070'},
    {702, 'asciicodes1', '\071\072\073\074\075\076\077\100\101\102\103\104\105\106\107\110\111\112\113\114\115\116\117\120\121'},
    {703, 'asciicodes2', '\122\123\124\125\126\127\130\131\132\133\134\135\136\137\140\141\142\143\144\145\146\147\150\151\152'},
    {704, 'asciicodes3', '\153\154\155\156\157\160\161\162\163\164\165\166\167\170\171\172\173\174\175\176'},
    {705, 'data0', (STRING)X'FEDCBA9876543210'},
    {706, 'data1', (STRING)X'00FF00FF00FF00FF'}
], varstringRecord);

varStringDatasetIn := ParquetIO.Read(varstringRecord, dropzoneDirectory + 'VarStringTest.parquet');

{UNSIGNED testid, STRING testname, BOOLEAN isEqual} varstringJoin (varStringDatasetOut a, varStringDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

varstringCompareResult := JOIN(varStringDatasetOut, varStringDatasetIn, LEFT.testid = RIGHT.testid, varstringJoin(LEFT, RIGHT), ALL);

varstringResult := IF(COUNT(varstringCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: VARSTRING data mismatch');

// ======================== QSTRING ========================

qstringRecord := {UNSIGNED testid, STRING testname, QSTRING value};

qStringDatasetOut := DATASET([
    {800, 'asciichars', ' !"#$F%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_'},
    {801, 'asciicodes0', '\040\041\042\043\044\045\046\047\050\051\052\053\054\055\056\057\060\061\062\063\064\065\066\067\070'},
    {802, 'asciicodes1', '\071\072\073\074\075\076\077\100\101\102\103\104\105\106\107\110\111\112\113\114\115\116\117\120\121'},
    {803, 'asciicodes2', '\122\123\124\125\126\127\130\131\132\133\134\135\136\137'},
    {804, 'data0', (STRING)X'FEDCBA9876543210'},
    {805, 'data1', (STRING)X'00FF00FF00FF00FF'}
], qstringRecord);

qStringDatasetIn := ParquetIO.Read(qstringRecord, dropzoneDirectory + 'QStringTest.parquet');

{UNSIGNED testid, STRING testname, BOOLEAN isEqual} qstringJoin (qStringDatasetOut a, qStringDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

qstringCompareResult := JOIN(qStringDatasetOut, qStringDatasetIn, LEFT.testid = RIGHT.testid, qstringJoin(LEFT, RIGHT), ALL);

qstringResult := IF(COUNT(qstringCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: QSTRING data mismatch');

// ======================== UTF8 ========================

utf8Record := {UNSIGNED testid, STRING testname, UTF8 value};

utf8DatasetOut := DATASET([
    {900, 'asciichars', U8' !"#$F%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_'},
    {901, 'extrachars0', U8'\241\242\243\244\245\246\247\250\251\252\253\254\255\256\257\260\261\262\263\264\265\266\267\270'},
    {902, 'extrachars1', U8'\316\221\316\222\316\223\316\224\316\225\316\226\316\227\316\230\316\231\316\232\316\233\316\234'}
], utf8Record);

utf8DatasetIn := ParquetIO.Read(utf8Record, dropzoneDirectory + 'UTF8Test.parquet');

{UNSIGNED testid, STRING testname, BOOLEAN isEqual} utf8Join (utf8DatasetOut a, utf8DatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

utf8CompareResult := JOIN(utf8DatasetOut, utf8DatasetIn, LEFT.testid = RIGHT.testid, utf8Join(LEFT, RIGHT), ALL);

utf8Result := IF(COUNT(utf8CompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: UTF8 data mismatch');

// ======================== UNICODE ========================

unicodeRecord := {UNSIGNED testid, STRING testname, UNICODE value};

unicodeDatasetOut := DATASET([
    {1000, 'asciichars', U' !"#$F%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_'},
    {1001, 'emojis0', U'➕️➖️➗️⛳️⛷️⛸️⛹️⛺️🆑🆒🆓🆔🆙🆚🌀🌁🌂🌃🌄🌅🌆🌇🌈🌉🌊🌋🌌🌍🌎🌏'},
    {1002, 'emojis1', U'🍀🍁🍂🍃🍄🍅🍆🍇🍈🍉🍊🍋🍌🍍🍎🍏📒📓📔📕📖📗📘📙📚📛📜📝📞📟'},
    {1003, 'adw', U'ᄠᄡᄢᄣᄤᄥᄦᄨᄩᄪᄫᄬᄭᄮᄯᆰᆱᆲᆳᆴᆵᆶᆷᆸᆹᆼᆽᇲᇳᇴᇵᇶᇷᇸ㈸㋄㋅㋆㋇㋈㋉㋊㋋㋌'}
], unicodeRecord);

unicodeDatasetIn := ParquetIO.Read(unicodeRecord, dropzoneDirectory + 'UnicodeTest.parquet');

{UNSIGNED testid, STRING testname, BOOLEAN isEqual} unicodeJoin(unicodeDatasetOut a, unicodeDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

unicodeCompareResult := JOIN(unicodeDatasetOut, unicodeDatasetIn, LEFT.testid = RIGHT.testid, unicodeJoin(LEFT, RIGHT), ALL);
unicodeResult := IF(COUNT(unicodeCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail');

// ========================= SET ==========================

setRecord := {UNSIGNED testid, STRING testname, SET OF BOOLEAN s0, SET OF INTEGER s1, SET OF UNSIGNED s2, SET OF REAL s3, SET OF DECIMAL s4,
              SET OF STRING s5, SET OF VARSTRING s6, SET OF QSTRING s7, SET OF UTF8 s8, SET OF UNICODE s9, SET OF DATA s10};

setDatasetOut := DATASET([
    {1100, 'empty', [], [], [], [], [], [], [], [], [], [], []},
    {1101, 'single', [TRUE], [1], [1], [1.0], [1.0], ['a'], ['a'], ['a'], [U'a'], [U'a'], [X'FFFF']},
    {1102, 'multiple', [TRUE, FALSE], [1, 2], [1, 2], [1.0, 2.0], [1.0, 2.0], ['a', 'b'], ['a', 'b'], ['a', 'b'], [U'a', U'b'], [U'a', U'b'], [X'0000', X'FFFF']}
], setRecord);

setResult := ParquetIO.Read(setRecord, dropzoneDirectory + 'SetTest.parquet');

// ======================== OUTPUT ========================

SEQUENTIAL(
    // Set up test files
    PARALLEL(
        ParquetIO.Write(booleanDatasetOut, dropzoneDirectory + 'BooleanTest.parquet', TRUE, compressionType),
        ParquetIO.Write(integerDatasetOut, dropzoneDirectory + 'IntegerTest.parquet', TRUE, compressionType),
        ParquetIO.Write(unsignedDatasetOut, dropzoneDirectory + 'UnsignedTest.parquet', TRUE, compressionType),
        ParquetIO.Write(realDatasetOut, dropzoneDirectory + 'RealTest.parquet', TRUE, compressionType),
        ParquetIO.Write(decimalDatasetOut, dropzoneDirectory + 'DecimalTest.parquet', TRUE, compressionType),
        ParquetIO.Write(stringDatasetOut, dropzoneDirectory + 'StringTest.parquet', TRUE, compressionType),
        ParquetIO.Write(dataDatasetOut, dropzoneDirectory + 'DataTest.parquet', TRUE, compressionType),
        ParquetIO.Write(varStringDatasetOut, dropzoneDirectory + 'VarStringTest.parquet', TRUE, compressionType),
        ParquetIO.Write(qStringDatasetOut, dropzoneDirectory + 'QStringTest.parquet', TRUE, compressionType),
        ParquetIO.Write(utf8DatasetOut, dropzoneDirectory + 'UTF8Test.parquet', TRUE, compressionType),
        ParquetIO.Write(unicodeDatasetOut, dropzoneDirectory + 'UnicodeTest.parquet', TRUE, compressionType),
        ParquetIO.Write(setDatasetOut, dropzoneDirectory + 'SetTest.parquet', TRUE, compressionType)
    ),
    // Read and compare results
    OUTPUT(booleanResult, NAMED('BooleanTest')),
    OUTPUT(integerResult, NAMED('IntegerTest')),
    OUTPUT(unsignedResult, NAMED('UnsignedTest')),
    OUTPUT(realResult, NAMED('RealTest')),
    OUTPUT(decimalResult, NAMED('DecimalTest')),
    OUTPUT(stringResult, NAMED('StringTest')),
    OUTPUT(dataResult, NAMED('DataTest')),
    OUTPUT(varStringResult, NAMED('VarStringTest')),
    OUTPUT(qStringResult, NAMED('QStringTest')),
    OUTPUT(utf8Result, NAMED('UTF8Test')),
    OUTPUT(unicodeResult, NAMED('UnicodeTest')),
    OUTPUT(setResult, NAMED('SetTest')),
    // Clean up temporary files
    PARALLEL(
        FileServices.DeleteExternalFile('.', dropzoneDirectory + 'BooleanTest.parquet'),
        FileServices.DeleteExternalFile('.', dropzoneDirectory + 'IntegerTest.parquet'),
        FileServices.DeleteExternalFile('.', dropzoneDirectory + 'UnsignedTest.parquet'),
        FileServices.DeleteExternalFile('.', dropzoneDirectory + 'RealTest.parquet'),
        FileServices.DeleteExternalFile('.', dropzoneDirectory + 'DecimalTest.parquet'),
        FileServices.DeleteExternalFile('.', dropzoneDirectory + 'StringTest.parquet'),
        FileServices.DeleteExternalFile('.', dropzoneDirectory + 'DataTest.parquet'),
        FileServices.DeleteExternalFile('.', dropzoneDirectory + 'VarStringTest.parquet'),
        FileServices.DeleteExternalFile('.', dropzoneDirectory + 'QStringTest.parquet'),
        FileServices.DeleteExternalFile('.', dropzoneDirectory + 'UTF8Test.parquet'),
        FileServices.DeleteExternalFile('.', dropzoneDirectory + 'UnicodeTest.parquet'),
        FileServices.DeleteExternalFile('.', dropzoneDirectory + 'SetTest.parquet')
    )
);
