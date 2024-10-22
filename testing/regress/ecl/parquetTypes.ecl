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
//Cover's data type's supported by ECL and arrow

IMPORT Std;
IMPORT Parquet;

dropzoneDirectory := Std.File.GetDefaultDropZone();

// ======================== BOOLEAN ========================

booleanRecord := {UNSIGNED testid, STRING3 testname, BOOLEAN value};

booleanDatasetOut := DATASET([
    {000, 'aaa', TRUE},
    {001, 'aab', FALSE}
], booleanRecord);

ParquetIO.Write(booleanDatasetOut, dropzoneDirectory + '/BooleanTest.parquet', TRUE);

booleanDatasetIn := ParquetIO.Read(booleanRecord, dropzoneDirectory + '/BooleanTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} booleanJoin (booleanDatasetOut a, booleanDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

booleanCompareResult := JOIN(booleanDatasetOut, booleanDatasetIn, LEFT.testid = RIGHT.testid, booleanJoin(LEFT, RIGHT), ALL);

booleanResult := IF(COUNT(booleanCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: Boolean data mismatch');

// ======================== INTEGER ========================

integerRecord := {UNSIGNED testid, STRING3 testname, INTEGER4 value};

integerDatasetOut := DATASET([
    {010, 'min', -2147483648},
    {011, 'max', 2147483647}
], integerRecord);

ParquetIO.Write(integerDatasetOut, dropzoneDirectory + '/IntegerTest.parquet', TRUE);

integerDatasetIn := ParquetIO.Read(integerRecord, dropzoneDirectory + '/IntegerTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} integerJoin (integerDatasetOut a, integerDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

integerCompareResult := JOIN(integerDatasetOut, integerDatasetIn, LEFT.testid = RIGHT.testid, integerJoin(LEFT, RIGHT), ALL);

integerResult := IF(COUNT(integerCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: Integer data mismatch');

// ======================== UNSIGNED ========================

unsignedRecord := {UNSIGNED testid, STRING3 testname, UNSIGNED value};

unsignedDatasetOut := DATASET([
    {020, 'aan', 0},
    {021, 'aao', 12345},
    {022, 'aap', 4294967295}
], unsignedRecord);

ParquetIO.Write(unsignedDatasetOut, dropzoneDirectory + '/UnsignedTest.parquet', TRUE);

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
    {030, 'max', 1.7976931348623157E+308},
    {031, 'min', 5.0E-324},
    {032, 'nor', -123.456}
], realRecord);

ParquetIO.Write(realDatasetOut, dropzoneDirectory + '/RealTest.parquet', TRUE);

realDatasetIn := ParquetIO.Read(realRecord, dropzoneDirectory + '/RealTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} realJoin (realDatasetOut a, realDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

realCompareResult := JOIN(realDatasetOut, realDatasetIn, LEFT.testid = RIGHT.testid, realJoin(LEFT, RIGHT), ALL);

realResult := IF(COUNT(realCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: Real data mismatch');

OUTPUT(realResult, NAMED('RealTestResult'));

// ======================== DECIMAL ========================

decimalRecord := {UNSIGNED testid, STRING3 testname, DECIMAL10_2 value};

decimalDatasetOut := DATASET([
    {040, 'aax', 12.34D},
    {041, 'aay', -56.78D},
    {042, 'abb', 0.00D}
], decimalRecord);

ParquetIO.Write(decimalDatasetOut, dropzoneDirectory + '/DecimalTest.parquet', TRUE);

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
    {'data3', (STRING)X'00FF00FF00FF00FF'}
], stringRecord);

ParquetIO.Write(stringDatasetOut, dropzoneDirectory + '/StringTest.parquet', TRUE);

stringDatasetIn := ParquetIO.Read(stringRecord, dropzoneDirectory + '/StringTest.parquet');

{STRING5 name, BOOLEAN value} stringJoin (stringDatasetOut a, stringDatasetIn b) := TRANSFORM
    SELF.name := a.name;
    SELF.value := a.value = b.value;
END;

stringCompareResult := JOIN(stringDatasetOut, stringDatasetIn, 
    LEFT.name = RIGHT.name, 
    stringJoin(LEFT, RIGHT), 
    ALL
);

stringResult := IF(COUNT(stringCompareResult(value = FALSE)) = 0, 'Pass', 'Fail: String data mismatch');

// ======================== DATA ========================

dataRecord := {STRING5 name, DATA value1, DATA10 value2};

dataDatasetOut := DATASET([
    {'abh', X'0123456789ABCDEF', (DATA10)X'0123456789ABCDEF'},
    {'abi', X'FEDCBA9876543210', (DATA10)X'FEDCBA9876543210'},
    {'abl', X'1234567890ABCDEF', (DATA10)X'1234567890ABCDEF'}
], dataRecord);

ParquetIO.Write(dataDatasetOut, dropzoneDirectory + '/DataTest.parquet', TRUE);

dataDatasetIn := ParquetIO.Read(dataRecord, dropzoneDirectory + '/DataTest.parquet');

{STRING5 name, BOOLEAN value1, BOOLEAN value2, BOOLEAN overallValue} dataJoin (dataDatasetOut a, dataDatasetIn b) := TRANSFORM
    SELF.name := a.name;
    SELF.value1 := a.value1 = b.value1;
    SELF.value2 := a.value2 = b.value2;
    SELF.overallValue := SELF.value1 AND SELF.value2;
END;

dataCompareResult := JOIN(dataDatasetOut, dataDatasetIn, 
    LEFT.name = RIGHT.name, 
    dataJoin(LEFT, RIGHT), 
    ALL
);

dataAsStringResult := IF(COUNT(dataCompareResult(overallValue = FALSE)) = 0, 'Pass', 'Fail: Data mismatch');

// ======================== VARSTRING ========================

varstringRecord := {UNSIGNED testid, STRING3 testname, VARSTRING value};

varStringDatasetOut := DATASET([
    {070, 'abm', 'VarString1'},
    {071, 'abn', ''},
    {072, 'abo', U'UTF8_测试'}
], varstringRecord);

ParquetIO.Write(varStringDatasetOut, dropzoneDirectory + '/VarStringTest.parquet', TRUE);

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

ParquetIO.Write(qStringDatasetOut, dropzoneDirectory + '/QStringTest.parquet', TRUE);

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

ParquetIO.Write(utf8DatasetOut, dropzoneDirectory + '/UTF8Test.parquet', TRUE);

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

unicodeDatasetOut := DATASET([
    {100, 'acb', U'Unicode1'},
    {101, 'acc', U'Unicode2'},
    {102, 'acf', U'Unicode5'}
], unicodeRecord);

ParquetIO.Write(unicodeDatasetOut, dropzoneDirectory + '/UnicodeTest.parquet', TRUE);

unicodeDatasetIn := ParquetIO.Read(unicodeRecord, dropzoneDirectory + '/UnicodeTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} unicodeJoin (unicodeDatasetOut a, unicodeDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

unicodeCompareResult := JOIN(unicodeDatasetOut, unicodeDatasetIn, LEFT.testid = RIGHT.testid, unicodeJoin(LEFT, RIGHT), ALL);

unicodeResult := IF(COUNT(unicodeCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: UNICODE data mismatch');

// ======================== SET OF INTEGER ========================

SetOfIntegerRecord := {UNSIGNED testid, STRING3 testname, SET OF INTEGER value};

setOfIntegerDatasetOut := DATASET([
    {110, 'acg', [1, 2, 3]},
    {113, 'acj', [10, 11, 12]},
    {114, 'ack', [13, 14, 15]}
], SetOfIntegerRecord);

ParquetIO.Write(setOfIntegerDatasetOut, dropzoneDirectory + '/SetOfIntegerTest.parquet', TRUE);

setOfIntegerDatasetIn := ParquetIO.Read(SetOfIntegerRecord, dropzoneDirectory + '/SetOfIntegerTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} setOfIntegerJoin (setOfIntegerDatasetOut a, setOfIntegerDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value; 
END;

setOfIntegerCompareResult := JOIN(setOfIntegerDatasetOut, setOfIntegerDatasetIn, LEFT.testid = RIGHT.testid, setOfIntegerJoin(LEFT, RIGHT), ALL);

setOfIntegerResult := IF(COUNT(setOfIntegerCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: SET OF INTEGER data mismatch');

// ======================== REAL8 ========================

real8Record := {UNSIGNED testid, STRING3 testname, REAL8 value};

real8DatasetOut := DATASET([
    {170, 'adk', 1.23D},
    {171, 'adl', -9.87D},
    {172, 'ado', -1.41421356237309D}
], real8Record);

ParquetIO.Write(real8DatasetOut, dropzoneDirectory + '/Real8Test.parquet', TRUE);

real8DatasetIn := ParquetIO.Read(real8Record, dropzoneDirectory + '/Real8Test.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} real8Join (real8DatasetOut a, real8DatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

real8CompareResult := JOIN(real8DatasetOut, real8DatasetIn, LEFT.testid = RIGHT.testid, real8Join(LEFT, RIGHT), ALL);

real8Result := IF(COUNT(real8CompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: REAL8 data mismatch');

// ======================== SET OF STRING ========================

setOfStringRecord := {UNSIGNED testid, STRING3 testname, SET OF STRING value};

setOfStringDatasetOut := DATASET([
    {180, 'adp', ['Set', 'Of', 'String', 'Test']},
    {181, 'adq', ['ECL', 'Data', 'Types']},
    {184, 'adt', ['A', 'B', 'C', 'D', 'E']}
], setOfStringRecord);

ParquetIO.Write(setOfStringDatasetOut, dropzoneDirectory + '/SetOfStringTest.parquet', TRUE);

setOfStringDatasetIn := ParquetIO.Read(setOfStringRecord, dropzoneDirectory + '/SetOfStringTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} setOfStringJoin (setOfStringDatasetOut a, setOfStringDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value; // Compare SET OF STRING values
END;

setOfStringCompareResult := JOIN(setOfStringDatasetOut, setOfStringDatasetIn, LEFT.testid = RIGHT.testid, setOfStringJoin(LEFT, RIGHT), ALL);

setOfStringResult := IF(COUNT(setOfStringCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: SET OF STRING data mismatch');

// ======================== SET OF UNICODE ========================

setOfUnicodeRecord := {UNSIGNED testid, STRING3 testname, SET OF UNICODE value};

setOfUnicodeDatasetOut := DATASET([
    {192, 'adw', [U'Á', U'É', U'Í', U'Ó', U'Ú']},
    {193, 'adx', [U'α', U'β', U'γ', U'δ', U'ε']},
    {194, 'ady', [U'☀', U'☁', U'☂', U'☃', U'☄']}
], setOfUnicodeRecord);

ParquetIO.Write(setOfUnicodeDatasetOut, dropzoneDirectory + '/SetOfUnicodeTest.parquet', TRUE);

setOfUnicodeDatasetIn := ParquetIO.Read(setOfUnicodeRecord, dropzoneDirectory + '/SetOfUnicodeTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} setOfUnicodeJoin (setOfUnicodeDatasetOut a, setOfUnicodeDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value; 
END;

setOfUnicodeCompareResult := JOIN(setOfUnicodeDatasetOut, setOfUnicodeDatasetIn, LEFT.testid = RIGHT.testid, setOfUnicodeJoin(LEFT, RIGHT), ALL);

setOfUnicodeResult := IF(COUNT(setOfUnicodeCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: SET OF UNICODE data mismatch');

// ======================== INTEGER8 ========================

integer8Record := RECORD
    UNSIGNED testid;
    STRING3 testname;
    INTEGER8 value;
END;

integer8DatasetOut := DATASET([
    {300, 'afa', (INTEGER8)32767},
    {301, 'afb', (INTEGER8)2147483647},
    {302, 'afc', (INTEGER8)9223372036854775807}
], integer8Record);

ParquetIO.Write(integer8DatasetOut, '/var/lib/HPCCSystems/mydropzone/IntegerSizesTest.parquet', TRUE);

integer8DatasetIn := ParquetIO.Read(integer8Record, '/var/lib/HPCCSystems/mydropzone/IntegerSizesTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} integer8Join (integer8DatasetOut a, integer8DatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

integer8CompareResult := JOIN(integer8DatasetOut, integer8DatasetIn, LEFT.testid = RIGHT.testid, integer8Join(LEFT, RIGHT), ALL);

integer8Result := IF(COUNT(integer8CompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: INTEGER8 data mismatch');

// ======================== UNSIGNED8 ========================

unsigned8Record := RECORD
    UNSIGNED testid;
    STRING3 testname;
    UNSIGNED8 value;
END;

unsigned8DatasetOut := DATASET([
    {310, 'afd', (UNSIGNED8)65535},
    {311, 'afe', (UNSIGNED8)4294967295},
    {312, 'aff', (UNSIGNED8)18446744073709551615}
], unsigned8Record);

ParquetIO.Write(unsigned8DatasetOut, '/var/lib/HPCCSystems/mydropzone/UnsignedSizesTest.parquet', TRUE);

unsigned8DatasetIn := ParquetIO.Read(unsigned8Record, '/var/lib/HPCCSystems/mydropzone/UnsignedSizesTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} unsigned8Join (unsigned8DatasetOut a, unsigned8DatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value; 
END;

unsigned8CompareResult := JOIN(unsigned8DatasetOut, unsigned8DatasetIn, LEFT.testid = RIGHT.testid, unsigned8Join(LEFT, RIGHT), ALL);

unsigned8Result := IF(COUNT(unsigned8CompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: UNSIGNED8 data mismatch');

// ======================== REAL4 ========================

real4Record := RECORD
    UNSIGNED testid;
    STRING3 testname;
    REAL4 value;
END;

real4DatasetOut := DATASET([
    {320, 'afg', (REAL4)1.23},
    {321, 'afh', (REAL4)-9.87},
    {322, 'afi', (REAL4)3.14159}
], real4Record);

ParquetIO.Write(real4DatasetOut, '/var/lib/HPCCSystems/mydropzone/Real4Test.parquet', TRUE);

real4DatasetIn := ParquetIO.Read(real4Record, '/var/lib/HPCCSystems/mydropzone/Real4Test.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} real4Join (real4DatasetOut a, real4DatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

real4CompareResult := JOIN(real4DatasetOut, real4DatasetIn, LEFT.testid = RIGHT.testid, real4Join(LEFT, RIGHT), ALL);

real4Result := IF(COUNT(real4CompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: REAL4 data mismatch');

// ======================== INTEGER1 ========================

integer1Record := RECORD
    UNSIGNED testid;
    STRING3 testname;
    INTEGER1 value;
END;

integer1DatasetOut := DATASET([
    {340, 'afp', (INTEGER1)127},
    {341, 'afq', (INTEGER1)-128},
    {342, 'afr', (INTEGER1)0}
], integer1Record);

ParquetIO.Write(integer1DatasetOut, '/var/lib/HPCCSystems/mydropzone/Integer1Test.parquet', TRUE);

integer1DatasetIn := ParquetIO.Read(integer1Record, '/var/lib/HPCCSystems/mydropzone/Integer1Test.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} integer1Join (integer1DatasetOut a, integer1DatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

integer1CompareResult := JOIN(integer1DatasetOut, integer1DatasetIn, LEFT.testid = RIGHT.testid, integer1Join(LEFT, RIGHT), ALL);

integer1Result := IF(COUNT(integer1CompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: INTEGER1 data mismatch');


// ======================== FIXED SIZE BINARY (DATA10) ========================

DATA10 REALToBinary(REAL val) := (DATA10)val;

fixedSizeBinaryRecord := RECORD
    UNSIGNED testid;
    STRING3 testname;
    DATA10 value;
END;

fixedSizeBinaryDatasetOut := DATASET([
    {1, 'pos', REALToBinary(3.14159)},
    {2, 'neg', REALToBinary(-2.71828)}
], fixedSizeBinaryRecord);

ParquetIO.Write(fixedSizeBinaryDatasetOut, '/var/lib/HPCCSystems/mydropzone/FixedSizeBinaryTest.parquet', TRUE);

fixedSizeBinaryDatasetIn := ParquetIO.Read(fixedSizeBinaryRecord, '/var/lib/HPCCSystems/mydropzone/FixedSizeBinaryTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} fixedSizeBinaryJoin (fixedSizeBinaryDatasetOut a, fixedSizeBinaryDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value; // Compare DATA10 (binary) values
END;

fixedSizeBinaryCompareResult := JOIN(fixedSizeBinaryDatasetOut, fixedSizeBinaryDatasetIn, LEFT.testid = RIGHT.testid, fixedSizeBinaryJoin(LEFT, RIGHT), ALL);

fixedSizeBinaryResult := IF(COUNT(fixedSizeBinaryCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: FIXED SIZE BINARY data mismatch');

// ======================== LARGE BINARY (DATA) ========================

DATA REALToLargeBinary(REAL val) := (DATA)val;

largeBinaryRecord := RECORD
    UNSIGNED testid;
    STRING3 testname;
    DATA value;
END;

largeBinaryDatasetOut := DATASET([
    {1, 'pos', REALToLargeBinary(3.14159)},
    {2, 'neg', REALToLargeBinary(-2.71828)}
], largeBinaryRecord);

ParquetIO.Write(largeBinaryDatasetOut, '/var/lib/HPCCSystems/mydropzone/LargeBinaryTest.parquet', TRUE);

largeBinaryDatasetIn := ParquetIO.Read(largeBinaryRecord, '/var/lib/HPCCSystems/mydropzone/LargeBinaryTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} largeBinaryJoin (largeBinaryDatasetOut a, largeBinaryDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

largeBinaryCompareResult := JOIN(largeBinaryDatasetOut, largeBinaryDatasetIn, LEFT.testid = RIGHT.testid, largeBinaryJoin(LEFT, RIGHT), ALL);

largeBinaryResult := IF(COUNT(largeBinaryCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: LARGE BINARY data mismatch');

// ======================== LARGE LIST ========================

largeListRecord := RECORD
    UNSIGNED testid;
    STRING3 testname;
    STRING value;
END;

largeListDatasetOut := DATASET([
    {1, 'lst1', 'apple,banana,cherry'},
    {2, 'lst2', 'dog,cat,bird,fish'},
    {3, 'lst3', 'red,green,blue,yellow,purple'}
], largeListRecord);

ParquetIO.Write(largeListDatasetOut, '/var/lib/HPCCSystems/mydropzone/LargeListTest.parquet', TRUE);

largeListDatasetIn := ParquetIO.Read(largeListRecord, '/var/lib/HPCCSystems/mydropzone/LargeListTest.parquet');

{UNSIGNED testid, STRING3 testname, BOOLEAN isEqual} largeListJoin (largeListDatasetOut a, largeListDatasetIn b) := TRANSFORM
    SELF.testid := a.testid;
    SELF.testname := a.testname;
    SELF.isEqual := a.value = b.value;
END;

largeListCompareResult := JOIN(largeListDatasetOut, largeListDatasetIn, LEFT.testid = RIGHT.testid, largeListJoin(LEFT, RIGHT), ALL);

largeListResult := IF(COUNT(largeListCompareResult(isEqual = FALSE)) = 0, 'Pass', 'Fail: STRING list data mismatch');


PARALLEL(
    OUTPUT(booleanResult, NAMED('BooleanTest'), OVERWRITE),
    OUTPUT(integerResult, NAMED('IntegerTest'), OVERWRITE),
    OUTPUT(unsignedResult, NAMED('UnsignedTest'), OVERWRITE),
    OUTPUT(realResult, NAMED('RealTest'), OVERWRITE),
    OUTPUT(decimalResult, NAMED('DecimalTest'), OVERWRITE),
    OUTPUT(stringResult, NAMED('StringTest'), OVERWRITE),
    OUTPUT(dataAsStringResult, NAMED('DataAsStringTest'), OVERWRITE),
    OUTPUT(varStringResult, NAMED('VarStringTest'), OVERWRITE),
    OUTPUT(qStringResult, NAMED('QStringTest'), OVERWRITE),
    OUTPUT(utf8Result, NAMED('UTF8Test'), OVERWRITE),
    OUTPUT(unicodeResult, NAMED('UnicodeTest'), OVERWRITE),
    OUTPUT(setOfIntegerResult, NAMED('SetOfIntegerTest'), OVERWRITE),
    OUTPUT(real8Result, NAMED('Real8Test'), OVERWRITE),
    OUTPUT(setOfStringResult, NAMED('SetOfStringTest'), OVERWRITE),
    OUTPUT(setOfUnicodeResult, NAMED('SetOfUnicodeTest'), OVERWRITE),
    OUTPUT(integer8Result, NAMED('IntegerSizesTest'), OVERWRITE),
    OUTPUT(unsigned8Result, NAMED('UnsignedSizesTest'), OVERWRITE),
    OUTPUT(real4Result, NAMED('Real4Test'), OVERWRITE),
    OUTPUT(integer1Result, NAMED('Integer1Test'), OVERWRITE),
    OUTPUT(fixedSizeBinaryResult, NAMED('FixedSizeBinaryTest'), OVERWRITE),
    OUTPUT(largeBinaryResult, NAMED('LargeBinaryTest'), OVERWRITE),
    OUTPUT(largeListResult, NAMED('LargeListTest'), OVERWRITE)
);
