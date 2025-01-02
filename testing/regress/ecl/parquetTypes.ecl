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
//Covers data types supported by ECL and Arrow
//version compressionType='UNCOMPRESSED'
//version compressionType='Snappy'
//version compressionType='GZip'
//version compressionType='Brotli'
//version compressionType='LZ4'
//version compressionType='ZSTD'

import ^ as root;
compressionType := #IFDEFINED(root.compressionType, 'UNCOMPRESSED');

IMPORT Std;
IMPORT Parquet;

dropzoneDirectory := Std.File.GetDefaultDropZone();

// ======================== BOOLEAN ========================

booleanRecord := {UNSIGNED testid, STRING3 testname, BOOLEAN value};

booleanDatasetOut := DATASET([
    {000, 'aaa', TRUE},
    {001, 'aab', FALSE}
], booleanRecord);

ParquetIO.Write(booleanDatasetOut, dropzoneDirectory + '/BooleanTest.parquet', TRUE, compressionType);

booleanDatasetIn := ParquetIO.Read(booleanRecord, dropzoneDirectory + '/BooleanTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} booleanJoin (booleanDatasetOut a, booleanDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

booleanCompareResult := JOIN(booleanDatasetOut, booleanDatasetIn, LEFT.testid = RIGHT.testid, booleanJoin(LEFT, RIGHT), ALL);

booleanResult := IF(COUNT(booleanCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: Boolean data mismatch');

// ======================== INTEGER ========================

integerRecord := {UNSIGNED testid, STRING3 testname, INTEGER4 value, UNSIGNED testid1, STRING3 testname1, INTEGER8 value1};

integerDatasetOut := DATASET([
    {10, 'min', -2147483648, 10, 'min', -2147483648},
    {11, 'max', 2147483647, 11, 'max', 2147483647},
    {12, 'afa', (INTEGER4)32767, 12, 'afa', 32767},
    {13, 'afb', (INTEGER4)2147483647, 13, 'afb', 2147483647},
    {14, 'afc', (INTEGER4)2147483647, 14, 'afc', 9223372036854775807},
    {15, 'afp', (INTEGER4)127, 340, 'afp', (INTEGER8)127},
    {16, 'afq', (INTEGER4)-128, 341, 'afq', (INTEGER8)-128},
    {17, 'afr', (INTEGER4)0, 342, 'afr', (INTEGER8)0},
    {18, 'acg', 1, 110, 'acg', 1},
    {19, 'acj', 10, 113, 'acj', 10},
    {20, 'ack', 13, 114, 'ack', 13}
], integerRecord);

ParquetIO.Write(integerDatasetOut, dropzoneDirectory + '/IntegerTest.parquet', TRUE, compressionType);

integerDatasetIn := ParquetIO.Read(integerRecord, dropzoneDirectory + '/IntegerTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} integerJoin (integerDatasetOut a, integerDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value AND a.value1 = b.value1;
END;

integerCompareResult := JOIN(integerDatasetOut, integerDatasetIn, LEFT.testid = RIGHT.testid, integerJoin(LEFT, RIGHT), ALL);

integerResult := IF(COUNT(integerCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: Integer data mismatch');

// ======================== UNSIGNED ========================

unsignedRecord := {UNSIGNED testid, STRING3 testname, UNSIGNED value};

unsignedDatasetOut := DATASET([
    {20, 'aan', 0},
    {21, 'aao', 12345},
    {22, 'aap', 4294967295},
    {23, 'afd', (UNSIGNED8)65535},
    {24, 'afe', (UNSIGNED8)4294967295},
    {25, 'aff', (UNSIGNED8)18446744073709551615}
], unsignedRecord);

ParquetIO.Write(unsignedDatasetOut, dropzoneDirectory + '/UnsignedTest.parquet', TRUE, compressionType);

unsignedDatasetIn := ParquetIO.Read(unsignedRecord, dropzoneDirectory + '/UnsignedTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} unsignedJoin (unsignedDatasetOut a, unsignedDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

unsignedCompareResult := JOIN(unsignedDatasetOut, unsignedDatasetIn, LEFT.testid = RIGHT.testid, unsignedJoin(LEFT, RIGHT), ALL);

unsignedResult := IF(COUNT(unsignedCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: Unsigned data mismatch');

// ======================== REAL ========================

realRecord := {UNSIGNED testid, STRING3 testname, REAL value};

realDatasetOut := DATASET([
    {30, 'max', 1.7976931348623157E+308},
    {31, 'min', 5.0E-324},
    {32, 'nor', -123.456},
    {34, 'adk', 1.23D},
    {35, 'adl', -9.87D},
    {36, 'ado', -1.41421356237309D},
    {37, 'afg', (REAL4)1.23},
    {38, 'afh', (REAL4)-9.87},
    {39, 'afi', (REAL4)3.14159}
], realRecord);

ParquetIO.Write(realDatasetOut, dropzoneDirectory + '/RealTest.parquet', TRUE, compressionType);

realDatasetIn := ParquetIO.Read(realRecord, dropzoneDirectory + '/RealTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} realJoin (realDatasetOut a, realDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

realCompareResult := JOIN(realDatasetOut, realDatasetIn, LEFT.testid = RIGHT.testid, realJoin(LEFT, RIGHT), ALL);

realResult := IF(COUNT(realCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: Real data mismatch');

// ======================== DECIMAL ========================

decimalRecord := {UNSIGNED testid, STRING3 testname, DECIMAL10_2 value};

decimalDatasetOut := DATASET([
    {040, 'aax', 12.34D},
    {041, 'aay', -56.78D},
    {042, 'abb', 0.00D}
], decimalRecord);

ParquetIO.Write(decimalDatasetOut, dropzoneDirectory + '/DecimalTest.parquet', TRUE, compressionType);

decimalDatasetIn := ParquetIO.Read(decimalRecord, dropzoneDirectory + '/DecimalTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} decimalJoin (decimalDatasetOut a, decimalDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

decimalCompareResult := JOIN(decimalDatasetOut, decimalDatasetIn, LEFT.testid = RIGHT.testid, decimalJoin(LEFT, RIGHT), ALL);

decimalResult := IF(COUNT(decimalCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: Decimal data mismatch');

// ======================== STRING ========================

stringRecord := {STRING5 name, STRING value};

stringDatasetOut := DATASET([
    {'abc', 'Hello'},
    {'abd', 'World'},
    {'abg', 'Types'},
    {'data1', (STRING)X'0123456789ABCDEF'},
    {'data2', (STRING)X'FEDCBA9876543210'},
    {'data3', (STRING)X'00FF00FF00FF00FF'},
    {'adp', '[\'Set\',\'Of\',\'String\',\'Test\']'},
    {'adq', '[\'ECL\',\'Data\',\'Types\']'},
    {'adt', '[\'A\',\'B\',\'C\',\'D\',\'E\']'}
], stringRecord);

ParquetIO.Write(stringDatasetOut, dropzoneDirectory + '/StringTest.parquet', TRUE, compressionType);

stringDatasetIn := ParquetIO.Read(stringRecord, dropzoneDirectory + '/StringTest.parquet');

{STRING5 name, BOOLEAN value} stringJoin (stringDatasetOut a, stringDatasetIn b) := TRANSFORM
    SELF.name := a.name;
    SELF.value := a.value = b.value;
END;

stringCompareResult := JOIN(stringDatasetOut, stringDatasetIn,
    LEFT.name = RIGHT.name,
    stringJoin(LEFT, RIGHT),
    ALL);

stringResult := IF(COUNT(stringCompareResult(value = FALSE)) = 0, 'Pass', 'Fail: String data mismatch');

// ======================== DATA ========================

DATA10 REALToBinary(REAL val) := (DATA10)val;
DATA16 REALToLargeBinary(REAL val) := (DATA16)val;

dataRecord := RECORD
    STRING5 name;
    DATA value1;
    DATA10 value2;
    DATA16 value3;
END;

dataDatasetOut := DATASET([
    {'abh', X'0123456789ABCDEF', (DATA10)X'0123456789ABCDEF', (DATA16)X'0123456789ABCDEF01234567'},
    {'abi', X'FEDCBA9876543210', (DATA10)X'FEDCBA9876543210', (DATA16)X'FEDCBA9876543210FEDCBA98'},
    {'abl', X'1234567890ABCDEF', (DATA10)X'1234567890ABCDEF', (DATA16)X'1234567890ABCDEF12345678'},
    {'pos', X'0000000000000000', REALToBinary(3.14159), REALToLargeBinary(3.14159)},
    {'neg', X'0000000000000000', REALToBinary(-2.71828), REALToLargeBinary(-2.71828)}
], dataRecord);

ParquetIO.Write(dataDatasetOut, dropzoneDirectory + '/DataTest.parquet', TRUE, compressionType);

dataDatasetIn := ParquetIO.Read(dataRecord, dropzoneDirectory + '/DataTest.parquet');

{STRING5 name, BOOLEAN value1, BOOLEAN value2, BOOLEAN value3, BOOLEAN overallValue} dataJoin(dataDatasetOut a, dataDatasetIn b) := TRANSFORM
    SELF.name := a.name;
    SELF.value1 := a.value1 = b.value1;
    SELF.value2 := a.value2 = b.value2;
    SELF.value3 := a.value3 = b.value3;
    SELF.overallValue := SELF.value1 AND SELF.value2 AND SELF.value3;
END;

dataCompareResult := JOIN(dataDatasetOut, dataDatasetIn, LEFT.name = RIGHT.name, dataJoin(LEFT, RIGHT), ALL);
dataResult := IF(COUNT(dataCompareResult(overallValue = FALSE)) = 0, 'Pass', 'Fail: Data mismatch');

// ======================== VARSTRING ========================

varstringRecord := {UNSIGNED testid, STRING3 testname, VARSTRING value};

varStringDatasetOut := DATASET([
    {070, 'abm', 'VarString1'},
    {071, 'abn', ''},
    {072, 'abo', U'UTF8_测试'}
], varstringRecord);

ParquetIO.Write(varStringDatasetOut, dropzoneDirectory + '/VarStringTest.parquet', TRUE, compressionType);

varStringDatasetIn := ParquetIO.Read(varstringRecord, dropzoneDirectory + '/VarStringTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} varstringJoin (varStringDatasetOut a, varStringDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

varstringCompareResult := JOIN(varStringDatasetOut, varStringDatasetIn, LEFT.testid = RIGHT.testid, varstringJoin(LEFT, RIGHT), ALL);

varstringResult := IF(COUNT(varstringCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: VARSTRING data mismatch');

// ======================== QSTRING ========================

qstringRecord := {UNSIGNED testid, STRING3 testname, QSTRING value};

qStringDatasetOut := DATASET([
    {080, 'abr', ''},
    {081, 'abs', 'NormalString'},
    {082, 'abt', U'Special_字符'}
], qstringRecord);

ParquetIO.Write(qStringDatasetOut, dropzoneDirectory + '/QStringTest.parquet', TRUE, compressionType);

qStringDatasetIn := ParquetIO.Read(qstringRecord, dropzoneDirectory + '/QStringTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} qstringJoin (qStringDatasetOut a, qStringDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

qstringCompareResult := JOIN(qStringDatasetOut, qStringDatasetIn, LEFT.testid = RIGHT.testid, qstringJoin(LEFT, RIGHT), ALL);

qstringResult := IF(COUNT(qstringCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: QSTRING data mismatch');

// ======================== UTF8 ========================

utf8Record := {UNSIGNED testid, STRING3 testname, UTF8 value};

utf8DatasetOut := DATASET([
    {090, 'abu', U'UTF8_Normal'},
    {091, 'abv', U'UTF8_测试'},
    {092, 'abw', U''}
], utf8Record);

ParquetIO.Write(utf8DatasetOut, dropzoneDirectory + '/UTF8Test.parquet', TRUE, compressionType);

utf8DatasetIn := ParquetIO.Read(utf8Record, dropzoneDirectory + '/UTF8Test.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} utf8Join (utf8DatasetOut a, utf8DatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

utf8CompareResult := JOIN(utf8DatasetOut, utf8DatasetIn, LEFT.testid = RIGHT.testid, utf8Join(LEFT, RIGHT), ALL);

utf8Result := IF(COUNT(utf8CompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: UTF8 data mismatch');

// ======================== UNICODE ========================

unicodeRecord := {UNSIGNED testid, STRING3 testname, UNICODE value};
setOfUnicodeRecord := {UNSIGNED testid, STRING3 testname, SET OF UNICODE value};

unicodeDatasetOut := DATASET([
    {100, 'acb', U'Unicode1'}, {101, 'acc', U'Unicode2'}, {102, 'acf', U'Unicode5'}
], unicodeRecord);

setOfUnicodeDatasetOut := DATASET([
    {103, 'adw', [U'Á', U'É', U'Í', U'Ó', U'Ú']},
    {104, 'adx', [U'α', U'β', U'γ', U'δ', U'ε']},
    {105, 'ady', [U'☀', U'☁', U'☂', U'☃', U'☄']}
], setOfUnicodeRecord);

ParquetIO.Write(unicodeDatasetOut, dropzoneDirectory + '/UnicodeTest.parquet', TRUE, compressionType);
ParquetIO.Write(setOfUnicodeDatasetOut, dropzoneDirectory + '/SetOfUnicodeTest.parquet', TRUE, compressionType);

unicodeDatasetIn := ParquetIO.Read(unicodeRecord, dropzoneDirectory + '/UnicodeTest.parquet');
setOfUnicodeDatasetIn := ParquetIO.Read(setOfUnicodeRecord, dropzoneDirectory + '/SetOfUnicodeTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} unicodeJoin(unicodeDatasetOut a, unicodeDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

unicodeCompareResult := JOIN(unicodeDatasetOut, unicodeDatasetIn, LEFT.testid = RIGHT.testid, unicodeJoin(LEFT, RIGHT), ALL);
unicodeResult := IF(COUNT(unicodeCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail');

PARALLEL(
    OUTPUT(booleanResult, NAMED('BooleanTest'), OVERWRITE),
    OUTPUT(integerResult, NAMED('IntegerTest'), OVERWRITE),
    OUTPUT(unsignedResult, NAMED('UnsignedTest'), OVERWRITE),
    OUTPUT(realResult, NAMED('RealTest'), OVERWRITE),
    OUTPUT(decimalResult, NAMED('DecimalTest'), OVERWRITE),
    OUTPUT(stringResult, NAMED('StringTest'), OVERWRITE),
    OUTPUT(dataResult, NAMED('DataAsStringTest'), OVERWRITE),
    OUTPUT(varStringResult, NAMED('VarStringTest'), OVERWRITE),
    OUTPUT(qStringResult, NAMED('QStringTest'), OVERWRITE),
    OUTPUT(utf8Result, NAMED('UTF8Test'), OVERWRITE),
    OUTPUT(unicodeResult, NAMED('UnicodeTest'), OVERWRITE)
);
