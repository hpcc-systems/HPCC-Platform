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

RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    BOOLEAN value;
END;


INTEGER_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    INTEGER value;
END;

UNSIGNED_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    UNSIGNED value;
END;

REAL_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    REAL value;
END;

// Define schema for DECIMAL type
DECIMAL_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    DECIMAL10_2 value;
END;

// Define schema for STRING type
STRING_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    STRING value;
END;


DATA_AS_STRING_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    STRING value;
END;

DATA_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    DATA value;
END;


// VARSTRING
VARSTRING_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    VARSTRING value;
END;

// QSTRING
QSTRING_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    QSTRING value;
END;


//SET OF INTEGER
SET_OF_INTEGER_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    SET OF INTEGER value;
END;

// REAL8
REAL8_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    REAL8 value;
END;

// String Set
SET_OF_STRING_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    SET OF STRING value;
END;

// Unicode
SET_OF_UNICODE_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    STRING value;
END;

// INTEGER8
INTEGER8_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    INTEGER8 value;
END;

// UNSIGNED8
UNSIGNED8_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    STRING value;
END;

// Real4
REAL4_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    REAL4 value;
END;

// Integer
INTEGER1_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    INTEGER1 value;
END;

// Fixed Size Binary
DATA10_RECORDDEF := RECORD
    UNSIGNED1 id;
    STRING3 name;
    DATA10 value;
END;

// Large Binary
LARGE_BINARY_RECORDDEF := RECORD
    UNSIGNED1 id;
    STRING3 name;
    DATA value;
END;

// Large List
LIST_RECORDDEF := RECORD
    UNSIGNED1 id;
    STRING4 name;
    STRING value;
END;

booleanDatasetOut := DATASET([
    {000, 'aaa', TRUE},
    {001, 'aab', FALSE}
], RECORDDEF);

ParquetIO.Write(booleanDatasetOut, '/var/lib/HPCCSystems/mydropzone/BooleanTest.parquet', TRUE);
booleanDatasetIn := ParquetIO.Read(RECORDDEF, '/var/lib/HPCCSystems/mydropzone/BooleanTest.parquet');

booleanResult := IF(booleanDatasetOut[1] = booleanDatasetIn[1] AND booleanDatasetOut[2] = booleanDatasetIn[2], 'Pass', 'Fail');

integerDatasetOut := DATASET([
    {-2147483648, 'min', -2147483648},
    {2147483647, 'max', 2147483647}
], INTEGER_RECORDDEF);

ParquetIO.Write(integerDatasetOut, '/var/lib/HPCCSystems/mydropzone/IntegerTest.parquet', TRUE);

integerDatasetIn := ParquetIO.Read(INTEGER_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/IntegerTest.parquet');

integerResult := IF(integerDatasetOut[1] = integerDatasetIn[1] AND integerDatasetOut[2] = integerDatasetIn[2], 'Pass', 'Fail');

unsignedDatasetOut := DATASET([
    {020, 'aan', 0},
    {021, 'aao', 12345},
    {022, 'aap', 4294967295}
], UNSIGNED_RECORDDEF);

ParquetIO.Write(unsignedDatasetOut, '/var/lib/HPCCSystems/mydropzone/UnsignedTest.parquet', TRUE);

unsignedDatasetIn := ParquetIO.Read(UNSIGNED_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/UnsignedTest.parquet');

unsignedResult := IF(unsignedDatasetOut[1] = unsignedDatasetIn[1] AND unsignedDatasetOut[2] = unsignedDatasetIn[2], 'Pass', 'Fail');


// REAL type test
realDatasetOut := DATASET([
    {001, 'maxValue', 1.7976931348623157E+308},
    {002, 'minValue', 5.0E-324},
    {003, 'normalValue', -123.456}
], REAL_RECORDDEF);

ParquetIO.Write(realDatasetOut, '/var/lib/HPCCSystems/mydropzone/RealTest.parquet', TRUE);

realDatasetIn := ParquetIO.Read(REAL_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/RealTest.parquet');

realResult := IF(realDatasetOut[1] = realDatasetIn[1] AND realDatasetOut[2] = realDatasetIn[2], 'Pass', 'Fail');

// DECIMAL type test
decimalDatasetOut := DATASET([
    {040, 'aax', 12.34D},
    {041, 'aay', -56.78D},
    {044, 'abb', 0.00D}
], DECIMAL_RECORDDEF);

ParquetIO.Write(decimalDatasetOut, '/var/lib/HPCCSystems/mydropzone/DecimalTest.parquet', TRUE);

decimalDatasetIn := ParquetIO.Read(DECIMAL_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/DecimalTest.parquet');

decimalResult := IF(decimalDatasetOut[1] = decimalDatasetIn[1] AND decimalDatasetOut[2] = decimalDatasetIn[2], 'Pass', 'Fail');

// STRING type test
stringDatasetOut := DATASET([
    {050, 'abc', 'Hello'},
    {051, 'abd', 'World'},
    {054, 'abg', 'Types'}
], STRING_RECORDDEF);

ParquetIO.Write(stringDatasetOut, '/var/lib/HPCCSystems/mydropzone/StringTest.parquet', TRUE);

stringDatasetIn := ParquetIO.Read(STRING_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/StringTest.parquet');

stringResult := IF(stringDatasetOut[1] = stringDatasetIn[1] AND stringDatasetOut[2] = stringDatasetIn[2], 'Pass', 'Fail');

dataAsStringDatasetOut := DATASET([
    {060, 'abh', (STRING)X'0123456789ABCDEF'},
    {061, 'abi', (STRING)X'FEDCBA9876543210'},
    {062, 'abj', (STRING)X'00FF00FF00FF00FF'}
], DATA_AS_STRING_RECORDDEF);

ParquetIO.Write(dataAsStringDatasetOut, '/var/lib/HPCCSystems/mydropzone/DataTest.parquet', TRUE);

dataAsStringDatasetIn := ParquetIO.Read(DATA_AS_STRING_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/DataTest.parquet');

dataAsStringResult := IF(dataAsStringDatasetOut[1] = dataAsStringDatasetIn[1] AND dataAsStringDatasetOut[2] = dataAsStringDatasetIn[2], 'Pass', 'Fail');

dataDatasetOut := DATASET([
    {060, 'abh', X'0123456789ABCDEF'},
    {061, 'abi', X'FEDCBA9876543210'},
    {064, 'abl', X'1234567890ABCDEF'}
], DATA_RECORDDEF);

ParquetIO.Write(dataDatasetOut, '/var/lib/HPCCSystems/mydropzone/DataTest.parquet', TRUE);

dataDatasetIn := ParquetIO.Read(DATA_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/DataTest.parquet');

dataResult := IF(dataDatasetOut[1] = dataDatasetIn[1] AND dataDatasetOut[2] = dataDatasetIn[2], 'Pass', 'Fail');

varStringDatasetOut := DATASET([
    {070, 'abm', 'VarString1'},
    {071, 'abn', ''},
    {072, 'abo', U'UTF8_测试'}
], VARSTRING_RECORDDEF);

ParquetIO.Write(varStringDatasetOut, '/var/lib/HPCCSystems/mydropzone/VarStringTest.parquet', TRUE);
varStringDatasetIn := ParquetIO.Read(VARSTRING_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/VarStringTest.parquet');

varStringResult := IF(varStringDatasetOut[1] = varStringDatasetIn[1] AND varStringDatasetOut[2] = varStringDatasetIn[2], 'Pass', 'Fail');

qStringDatasetOut := DATASET([
    {080, 'abr', ''},
    {081, 'abs', 'NormalString'},
    {082, 'abt', U'Special_字符'}
], QSTRING_RECORDDEF);

ParquetIO.Write(qStringDatasetOut, '/var/lib/HPCCSystems/mydropzone/QStringTest.parquet', TRUE);

qStringDatasetIn := ParquetIO.Read(QSTRING_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/QStringTest.parquet');

qStringResult := IF(qStringDatasetOut[1] = qStringDatasetIn[1] AND 
                    qStringDatasetOut[2] = qStringDatasetIn[2] AND 
                    qStringDatasetOut[3] = qStringDatasetIn[3], 
                    'Pass', 'Fail');

// UTF8 type
ParquetIO.write(DATASET([
    {090, 'abw', U'HelloWorld'},
    {091, 'abx', U'こんにちは'},
    {092, 'aby', U'🚀🌟💬'}
], {UNSIGNED testid, STRING3 testname, UTF8 value}), '/var/lib/HPCCSystems/mydropzone/UTF8Test.parquet', TRUE);

utf8Dataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; UTF8 value}, '/var/lib/HPCCSystems/mydropzone/UTF8Test.parquet');
utf8Result := IF(COUNT(utf8Dataset) = 5, 'Pass', 'Fail: UTF8 data count mismatch');

// UNICODE type
ParquetIO.write(DATASET([
    {100, 'acb', U'Unicode1'},
    {101, 'acc', U'Unicode2'},
    {104, 'acf', U'Unicode5'}
], {UNSIGNED testid, STRING3 testname, UNICODE value}), '/var/lib/HPCCSystems/mydropzone/UnicodeTest.parquet', TRUE);

unicodeDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; UNICODE value}, '/var/lib/HPCCSystems/mydropzone/UnicodeTest.parquet');
unicodeResult := IF(COUNT(unicodeDataset) = 5, 'Pass', 'Fail: Unicode data count mismatch');

setOfIntegerDatasetOut := DATASET([
    {110, 'acg', [1, 2, 3]},
    {113, 'acj', [10, 11, 12]},
    {114, 'ack', [13, 14, 15]}
], SET_OF_INTEGER_RECORDDEF);

ParquetIO.Write(setOfIntegerDatasetOut, '/var/lib/HPCCSystems/mydropzone/SetOfIntegerTest.parquet', TRUE);

setOfIntegerDatasetIn := ParquetIO.Read(SET_OF_INTEGER_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/SetOfIntegerTest.parquet');

setOfIntegerResult := IF(
    setOfIntegerDatasetOut[1] = setOfIntegerDatasetIn[1] AND 
    setOfIntegerDatasetOut[2] = setOfIntegerDatasetIn[2] AND 
    setOfIntegerDatasetOut[3] = setOfIntegerDatasetIn[3], 
    'Pass', 'Fail: Set of Integer data mismatch'
);

real8DatasetOut := DATASET([
    {170, 'adk', 1.23D},
    {171, 'adl', -9.87D},
    {172, 'ado', -1.41421356237309D}
], REAL8_RECORDDEF);

ParquetIO.Write(real8DatasetOut, '/var/lib/HPCCSystems/mydropzone/Real8Test.parquet', TRUE);

real8DatasetIn := ParquetIO.Read(REAL8_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/Real8Test.parquet');

real8Result := IF(
    real8DatasetOut[1] = real8DatasetIn[1] AND
    real8DatasetOut[2] = real8DatasetIn[2] AND
    real8DatasetOut[3] = real8DatasetIn[3],
    'Pass', 'Fail: Real8 data mismatch'
);

setOfStringDatasetOut := DATASET([
    {180, 'adp', ['Set', 'Of', 'String', 'Test']},
    {181, 'adq', ['ECL', 'Data', 'Types']},
    {184, 'adt', ['A', 'B', 'C', 'D', 'E']}
], SET_OF_STRING_RECORDDEF);

ParquetIO.Write(setOfStringDatasetOut, '/var/lib/HPCCSystems/mydropzone/SetOfStringTest.parquet', TRUE);

setOfStringDatasetIn := ParquetIO.Read(SET_OF_STRING_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/SetOfStringTest.parquet');

setOfStringResult := IF(
    setOfStringDatasetOut[1] = setOfStringDatasetIn[1] AND
    setOfStringDatasetOut[2] = setOfStringDatasetIn[2] AND
    setOfStringDatasetOut[3] = setOfStringDatasetIn[3],
    'Pass', 'Fail: Set of String data mismatch'
);

setOfUnicodeDatasetOut := DATASET([
    {192, 'adw', U'Á,É,Í,Ó,Ú'},
    {193, 'adx', U'α,β,γ,δ,ε'},
    {194, 'ady', U'☀,☁,☂,☃,☄'}
], SET_OF_UNICODE_RECORDDEF);

ParquetIO.Write(setOfUnicodeDatasetOut, '/var/lib/HPCCSystems/mydropzone/SetOfUnicodeTest.parquet', TRUE);

setOfUnicodeDatasetIn := ParquetIO.Read(SET_OF_UNICODE_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/SetOfUnicodeTest.parquet');

setOfUnicodeResult := IF(
    setOfUnicodeDatasetOut[1] = setOfUnicodeDatasetIn[1] AND
    setOfUnicodeDatasetOut[2] = setOfUnicodeDatasetIn[2] AND
    setOfUnicodeDatasetOut[3] = setOfUnicodeDatasetIn[3],
    'Pass', 'Fail: Set of Unicode data mismatch'
);

integer8DatasetOut := DATASET([
    {300, 'afa', (INTEGER8)32767},
    {301, 'afb', (INTEGER8)2147483647},
    {302, 'afc', (INTEGER8)9223372036854775807}
], INTEGER8_RECORDDEF);

ParquetIO.Write(integer8DatasetOut, '/var/lib/HPCCSystems/mydropzone/IntegerSizesTest.parquet', TRUE);

integer8DatasetIn := ParquetIO.Read(INTEGER8_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/IntegerSizesTest.parquet');

integer8Result := IF(
    integer8DatasetOut[1] = integer8DatasetIn[1] AND
    integer8DatasetOut[2] = integer8DatasetIn[2] AND
    integer8DatasetOut[3] = integer8DatasetIn[3],
    'Pass', 'Fail: Integer8 data mismatch'
);

unsigned8DatasetOut := DATASET([
    {310, 'afd', (STRING)(UNSIGNED8)65535},
    {311, 'afe', (STRING)(UNSIGNED8)4294967295},
    {312, 'aff', (STRING)(UNSIGNED8)18446744073709551615}
], UNSIGNED8_RECORDDEF);

ParquetIO.Write(unsigned8DatasetOut, '/var/lib/HPCCSystems/mydropzone/UnsignedSizesTest.parquet', TRUE);

unsigned8DatasetIn := ParquetIO.Read(UNSIGNED8_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/UnsignedSizesTest.parquet');

unsigned8Result := IF(
    unsigned8DatasetOut[1] = unsigned8DatasetIn[1] AND
    unsigned8DatasetOut[2] = unsigned8DatasetIn[2] AND
    unsigned8DatasetOut[3] = unsigned8DatasetIn[3],
    'Pass', 'Fail: Unsigned8 data mismatch'
);

real4DatasetOut := DATASET([
    {320, 'afg', (REAL4)1.23},
    {321, 'afh', (REAL4)-9.87},
    {322, 'afi', (REAL4)3.14159}
], REAL4_RECORDDEF);

ParquetIO.Write(real4DatasetOut, '/var/lib/HPCCSystems/mydropzone/Real4Test.parquet', TRUE);

real4DatasetIn := ParquetIO.Read(REAL4_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/Real4Test.parquet');

real4Result := IF(
    real4DatasetOut[1] = real4DatasetIn[1] AND
    real4DatasetOut[2] = real4DatasetIn[2] AND
    real4DatasetOut[3] = real4DatasetIn[3],
    'Pass', 'Fail: Real4 data mismatch'
);

integer1DatasetOut := DATASET([
    {340, 'afp', 127},
    {341, 'afq', -128},
    {342, 'afr', 0}
], INTEGER1_RECORDDEF);

ParquetIO.Write(integer1DatasetOut, '/var/lib/HPCCSystems/mydropzone/Integer1Test.parquet', TRUE);

integer1DatasetIn := ParquetIO.Read(INTEGER1_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/Integer1Test.parquet');

integer1Result := IF(
    integer1DatasetOut[1] = integer1DatasetIn[1] AND
    integer1DatasetOut[2] = integer1DatasetIn[2] AND
    integer1DatasetOut[3] = integer1DatasetIn[3],
    'Pass', 'Fail: Integer1 data mismatch'
);

DATA10 REALToBinary(REAL val) := (DATA10)val;

dataset_fixed_size_binaryOut := DATASET([
    {1, 'pos', REALToBinary(3.14159)},
    {2, 'neg', REALToBinary(-2.71828)}
], DATA10_RECORDDEF);

ParquetIO.Write(dataset_fixed_size_binaryOut, '/var/lib/HPCCSystems/mydropzone/FixedSizeBinaryTest.parquet', TRUE);

fixedSizeBinaryDatasetIn := ParquetIO.Read(DATA10_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/FixedSizeBinaryTest.parquet');

fixedSizeBinaryResult := IF(
    dataset_fixed_size_binaryOut[1] = fixedSizeBinaryDatasetIn[1] AND
    dataset_fixed_size_binaryOut[2] = fixedSizeBinaryDatasetIn[2],
    'Pass', 'Fail: Fixed Size Binary data mismatch'
);

DATA REALToLargeBinary(REAL val) := (DATA)val;

dataset_large_binaryOut := DATASET([
    {1, 'pos', REALToLargeBinary(3.14159)},
    {2, 'neg', REALToLargeBinary(-2.71828)}
], LARGE_BINARY_RECORDDEF);

ParquetIO.Write(dataset_large_binaryOut, '/var/lib/HPCCSystems/mydropzone/LargeBinaryTest.parquet', TRUE);

largeBinaryDatasetIn := ParquetIO.Read(LARGE_BINARY_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/LargeBinaryTest.parquet');

largeBinaryResult := IF(
    dataset_large_binaryOut[1] = largeBinaryDatasetIn[1] AND
    dataset_large_binaryOut[2] = largeBinaryDatasetIn[2],
    'Pass', 'Fail: Large Binary data mismatch'
);

dataset_large_listOut := DATASET([
    {1, 'lst1', 'apple,banana,cherry'},
    {2, 'lst2', 'dog,cat,bird,fish'},
    {3, 'lst3', 'red,green,blue,yellow,purple'}
], LIST_RECORDDEF);

ParquetIO.Write(dataset_large_listOut, '/var/lib/HPCCSystems/mydropzone/LargeListTest.parquet', TRUE);

largeListDatasetIn := ParquetIO.Read(LIST_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/LargeListTest.parquet');

largeListResult := IF(
    dataset_large_listOut[1] = largeListDatasetIn[1] AND
    dataset_large_listOut[2] = largeListDatasetIn[2] AND
    dataset_large_listOut[3] = largeListDatasetIn[3],
    'Pass', 'Fail: Large List data mismatch'
);

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
