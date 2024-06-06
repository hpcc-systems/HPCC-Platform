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
//nothor,noroxie
//Cover's data type's supported by ECL and arrow

IMPORT Parquet;

//All covered ECL data type's tested below

// BOOLEAN type
ParquetIO.write(DATASET([
    {000, 'aaa', TRUE},
    {001, 'aab', FALSE},
    {002, 'aac', TRUE},
    {003, 'aad', FALSE},
    {004, 'aae', TRUE}
], {UNSIGNED testid, STRING3 testname, BOOLEAN value}), '/var/lib/HPCCSystems/mydropzone/BooleanTest.parquet', TRUE);

booleanDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; BOOLEAN value}, '/var/lib/HPCCSystems/mydropzone/BooleanTest.parquet');
booleanResult := IF(COUNT(booleanDataset) = 5, 'Pass', 'Fail: Boolean data count mismatch');

// INTEGER type
ParquetIO.write(DATASET([
    {010, 'aai', 123},
    {011, 'aaj', -987},
    {012, 'aak', 456},
    {013, 'aal', 789},
    {014, 'aam', -321}
], {UNSIGNED testid, STRING3 testname, INTEGER value}), '/var/lib/HPCCSystems/mydropzone/IntegerTest.parquet', TRUE);

integerDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; INTEGER value}, '/var/lib/HPCCSystems/mydropzone/IntegerTest.parquet');
integerResult := IF(COUNT(integerDataset) = 5, 'Pass', 'Fail: Integer data count mismatch');

// UNSIGNED type
ParquetIO.write(DATASET([
    {020, 'aan', 12345},
    {021, 'aao', 67890},
    {022, 'aap', 1234},
    {023, 'aaq', 5678},
    {024, 'aar', 91011}
], {UNSIGNED testid, STRING3 testname, UNSIGNED value}), '/var/lib/HPCCSystems/mydropzone/UnsignedTest.parquet', TRUE);

unsignedDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; UNSIGNED value}, '/var/lib/HPCCSystems/mydropzone/UnsignedTest.parquet');
unsignedResult := IF(COUNT(unsignedDataset) = 5, 'Pass', 'Fail: Unsigned data count mismatch');

// REAL type
ParquetIO.write(DATASET([
    {030, 'aas', 1.23},
    {031, 'aat', -9.87},
    {032, 'aau', 45.67},
    {033, 'aav', 78.90},
    {034, 'aaw', -32.1}
], {UNSIGNED testid, STRING3 testname, REAL value}), '/var/lib/HPCCSystems/mydropzone/RealTest.parquet', TRUE);

realDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; REAL value}, '/var/lib/HPCCSystems/mydropzone/RealTest.parquet');
realResult := IF(COUNT(realDataset) = 5, 'Pass', 'Fail: Real data count mismatch');

// DECIMAL type
ParquetIO.write(DATASET([
    {040, 'aax', 12.34D},
    {041, 'aay', -56.78D},
    {042, 'aaz', 90.12D},
    {043, 'aba', 34.56D},
    {044, 'abb', -78.90D}
], {UNSIGNED testid, STRING3 testname, DECIMAL10_2 value}), '/var/lib/HPCCSystems/mydropzone/DecimalTest.parquet', TRUE);

decimalDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; DECIMAL10_2 value}, '/var/lib/HPCCSystems/mydropzone/DecimalTest.parquet');
decimalResult := IF(COUNT(decimalDataset) = 5, 'Pass', 'Fail: Decimal data count mismatch');

// STRING type
ParquetIO.write(DATASET([
    {050, 'abc', 'Hello'},
    {051, 'abd', 'World'},
    {052, 'abe', 'Test'},
    {053, 'abf', 'String'},
    {054, 'abg', 'Types'}
], {UNSIGNED testid, STRING3 testname, STRING value}), '/var/lib/HPCCSystems/mydropzone/StringTest.parquet', TRUE);

stringDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; STRING value}, '/var/lib/HPCCSystems/mydropzone/StringTest.parquet');
stringResult := IF(COUNT(stringDataset) = 5, 'Pass', 'Fail: String data count mismatch');

// DATA type (converted to STRING)
ParquetIO.write(DATASET([
    {060, 'abh', (STRING)X'0123456789ABCDEF'},
    {061, 'abi', (STRING)X'FEDCBA9876543210'},
    {062, 'abj', (STRING)X'00FF00FF00FF00FF'},
    {063, 'abk', (STRING)X'FF00FF00FF00FF00'},
    {064, 'abl', (STRING)X'0A0B0C0D0E0F1011'}
], {UNSIGNED testid, STRING3 testname, STRING value}), '/var/lib/HPCCSystems/mydropzone/DataAsStringTest.parquet', TRUE);

dataAsStringDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; STRING value}, '/var/lib/HPCCSystems/mydropzone/DataAsStringTest.parquet');
dataAsStringResult := IF(COUNT(dataAsStringDataset) = 5, 'Pass', 'Fail: Data as String data count mismatch');

// VARSTRING type
ParquetIO.write(DATASET([
    {070, 'abm', 'VarString1'},
    {071, 'abn', 'VarString2'},
    {072, 'abo', 'VarString3'},
    {073, 'abp', 'VarString4'},
    {074, 'abq', 'VarString5'}
], {UNSIGNED testid, STRING3 testname, VARSTRING value}), '/var/lib/HPCCSystems/mydropzone/VarStringTest.parquet', TRUE);

varStringDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; VARSTRING value}, '/var/lib/HPCCSystems/mydropzone/VarStringTest.parquet');
varStringResult := IF(COUNT(varStringDataset) = 5, 'Pass', 'Fail: VarString data count mismatch');

// QSTRING type
ParquetIO.write(DATASET([
    {080, 'abr', 'QStr1'},
    {081, 'abs', 'QStr2'},
    {082, 'abt', 'QStr3'},
    {083, 'abu', 'QStr4'},
    {084, 'abv', 'QStr5'}
], {UNSIGNED testid, STRING3 testname, QSTRING value}), '/var/lib/HPCCSystems/mydropzone/QStringTest.parquet', TRUE);

qStringDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; QSTRING value}, '/var/lib/HPCCSystems/mydropzone/QStringTest.parquet');
qStringResult := IF(COUNT(qStringDataset) = 5, 'Pass', 'Fail: QString data count mismatch');

// UTF8 type
ParquetIO.write(DATASET([
    {090, 'abw', U'UTF8_1'},
    {091, 'abx', U'UTF8_2'},
    {092, 'aby', U'UTF8_3'},
    {093, 'abz', U'UTF8_4'},
    {094, 'aca', U'UTF8_5'}
], {UNSIGNED testid, STRING3 testname, UTF8 value}), '/var/lib/HPCCSystems/mydropzone/UTF8Test.parquet', TRUE);

utf8Dataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; UTF8 value}, '/var/lib/HPCCSystems/mydropzone/UTF8Test.parquet');
utf8Result := IF(COUNT(utf8Dataset) = 5, 'Pass', 'Fail: UTF8 data count mismatch');

// UNICODE type
ParquetIO.write(DATASET([
    {100, 'acb', U'Unicode1'},
    {101, 'acc', U'Unicode2'},
    {102, 'acd', U'Unicode3'},
    {103, 'ace', U'Unicode4'},
    {104, 'acf', U'Unicode5'}
], {UNSIGNED testid, STRING3 testname, UNICODE value}), '/var/lib/HPCCSystems/mydropzone/UnicodeTest.parquet', TRUE);

unicodeDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; UNICODE value}, '/var/lib/HPCCSystems/mydropzone/UnicodeTest.parquet');
unicodeResult := IF(COUNT(unicodeDataset) = 5, 'Pass', 'Fail: Unicode data count mismatch');

// SET OF INTEGER type
ParquetIO.write(DATASET([
    {110, 'acg', [1,2,3]},
    {111, 'ach', [4,5,6]},
    {112, 'aci', [7,8,9]},
    {113, 'acj', [10,11,12]},
    {114, 'ack', [13,14,15]}
], {UNSIGNED testid, STRING3 testname, SET OF INTEGER value}), '/var/lib/HPCCSystems/mydropzone/SetOfIntegerTest.parquet', TRUE);

setOfIntegerDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; SET OF INTEGER value}, '/var/lib/HPCCSystems/mydropzone/SetOfIntegerTest.parquet');
setOfIntegerResult := IF(COUNT(setOfIntegerDataset) = 5, 'Pass', 'Fail: Set of Integer data count mismatch');

// VARUNICODE type
ParquetIO.write(DATASET([
    {120, 'acl', U'VarUnicode1'},
    {121, 'acm', U'VarUnicode2'},
    {122, 'acn', U'VarUnicode3'},
    {123, 'aco', U'VarUnicode4'},
    {124, 'acp', U'VarUnicode5'}
], {UNSIGNED testid, STRING3 testname, VARUNICODE value}), '/var/lib/HPCCSystems/mydropzone/VarUnicodeTest.parquet', TRUE);

varUnicodeDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; SET OF INTEGER value}, '/var/lib/HPCCSystems/mydropzone/VarUnicodeTest.parquet');
varUnicodeResult := IF(COUNT(varUnicodeDataset) = 5, 'Pass', 'Fail: Set of Integer data count mismatch');

// REAL8 (FLOAT8) type
ParquetIO.write(DATASET([
    {170, 'adk', 1.23D},
    {171, 'adl', -9.87D},
    {172, 'adm', 3.14159265358979D},
    {173, 'adn', 2.71828182845904D},
    {174, 'ado', -1.41421356237309D}
], {UNSIGNED testid, STRING3 testname, REAL8 value}), '/var/lib/HPCCSystems/mydropzone/Real8Test.parquet', TRUE);

real8Dataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; REAL8 value}, '/var/lib/HPCCSystems/mydropzone/Real8Test.parquet');
real8Result := IF(COUNT(real8Dataset) = 5, 'Pass', 'Fail: Real8 data count mismatch');

// SET OF STRING type
ParquetIO.write(DATASET([
    {180, 'adp', ['Set', 'Of', 'String', 'Test']},
    {181, 'adq', ['ECL', 'Data', 'Types']},
    {182, 'adr', ['Hello', 'World']},
    {183, 'ads', ['One', 'Two', 'Three', 'Four', 'Five']},
    {184, 'adt', ['A', 'B', 'C', 'D', 'E']}
], {UNSIGNED testid, STRING3 testname, SET OF STRING value}), '/var/lib/HPCCSystems/mydropzone/SetOfStringTest.parquet', TRUE);

setOfStringDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; SET OF STRING value}, '/var/lib/HPCCSystems/mydropzone/SetOfStringTest.parquet');
setOfStringResult := IF(COUNT(setOfStringDataset) = 5, 'Pass', 'Fail: Set of String data count mismatch');

// SET OF UNICODE type
ParquetIO.write(DATASET([
    {190, 'adu', [U'Unicode', U'Set', U'Test']},
    {191, 'adv', [U'こんにちは', U'世界']},
    {192, 'adw', [U'Á', U'É', U'Í', U'Ó', U'Ú']},
    {193, 'adx', [U'α', U'β', U'γ', U'δ', U'ε']},
    {194, 'ady', [U'☀', U'☁', U'☂', U'☃', U'☄']}
], {UNSIGNED testid, STRING3 testname, SET OF UNICODE value}), '/var/lib/HPCCSystems/mydropzone/SetOfUnicodeTest.parquet', TRUE);

setOfUnicodeDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; SET OF UNICODE value}, '/var/lib/HPCCSystems/mydropzone/SetOfUnicodeTest.parquet');
setOfUnicodeResult := IF(COUNT(setOfUnicodeDataset) = 5, 'Pass', 'Fail: Set of Unicode data count mismatch');

// INTEGER8 type
ParquetIO.write(DATASET([
    {300, 'afa', (INTEGER2)32767},
    {301, 'afb', (INTEGER4)2147483647},
    {302, 'afc', (INTEGER8)9223372036854775807}
], {UNSIGNED testid, STRING3 testname, INTEGER8 value}), '/var/lib/HPCCSystems/mydropzone/IntegerSizesTest.parquet', TRUE);

integer8Dataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; INTEGER8 value}, '/var/lib/HPCCSystems/mydropzone/IntegerSizesTest.parquet');
integer8Result := IF(COUNT(integer8Dataset) = 3, 'Pass', 'Fail: Integer8 data count mismatch');

// UNSIGNED8 type
ParquetIO.write(DATASET([
    {310, 'afd', (UNSIGNED2)65535},
    {311, 'afe', (UNSIGNED4)4294967295},
    {312, 'aff', (UNSIGNED8)18446744073709551615}
], {UNSIGNED testid, STRING3 testname, UNSIGNED8 value}), '/var/lib/HPCCSystems/mydropzone/UnsignedSizesTest.parquet', TRUE);

unsigned8Dataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; UNSIGNED8 value}, '/var/lib/HPCCSystems/mydropzone/UnsignedSizesTest.parquet');
unsigned8Result := IF(COUNT(unsigned8Dataset) = 3, 'Pass', 'Fail: Unsigned8 data count mismatch');

// REAL4 (FLOAT4) type
ParquetIO.write(DATASET([
    {320, 'afg', (REAL4)1.23},
    {321, 'afh', (REAL4)-9.87},
    {322, 'afi', (REAL4)3.14159}
], {UNSIGNED testid, STRING3 testname, REAL4 value}), '/var/lib/HPCCSystems/mydropzone/Real4Test.parquet', TRUE);

real4Dataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; REAL4 value}, '/var/lib/HPCCSystems/mydropzone/Real4Test.parquet');
real4Result := IF(COUNT(real4Dataset) = 3, 'Pass', 'Fail: Real4 data count mismatch');

// INTEGER1 (BYTE) type
ParquetIO.write(DATASET([
    {340, 'afp', 127},
    {341, 'afq', -128},
    {342, 'afr', 0}
], {UNSIGNED testid, STRING3 testname, INTEGER1 value}), '/var/lib/HPCCSystems/mydropzone/Integer1Test.parquet', TRUE);

integer1Dataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; INTEGER1 value}, '/var/lib/HPCCSystems/mydropzone/Integer1Test.parquet');
integer1Result := IF(COUNT(integer1Dataset) = 3, 'Pass', 'Fail: Integer1 data count mismatch');

DATA10 REALToBinary(REAL val) := (DATA10)val;

dataset_fixed_size_binary := DATASET([
    {1, 'pos', REALToBinary(3.14159)},
    {2, 'neg', REALToBinary(-2.71828)},
    {3, 'zer', REALToBinary(0.0)},
    {4, 'big', REALToBinary(1.23E+38)},
    {5, 'sml', REALToBinary(1.23E-38)}
], {UNSIGNED1 id, STRING3 name, DATA10 value});

ParquetIO.write(dataset_fixed_size_binary, '/var/lib/HPCCSystems/mydropzone/FixedSizeBinaryTest.parquet', TRUE);

fixedSizeBinaryDataset := ParquetIO.Read({UNSIGNED1 id; STRING3 name; DATA10 value}, '/var/lib/HPCCSystems/mydropzone/FixedSizeBinaryTest.parquet');
fixedSizeBinaryResult := IF(COUNT(fixedSizeBinaryDataset) = 5, 'Pass', 'Fail: Fixed Size Binary data count mismatch');

DATA REALToLargeBinary(REAL val) := (DATA)val;

dataset_large_binary := DATASET([
    {1, 'pos', REALToLargeBinary(3.14159)},
    {2, 'neg', REALToLargeBinary(-2.71828)},
    {3, 'zer', REALToLargeBinary(0.0)},
    {4, 'big', REALToLargeBinary(1.23E+38)},
    {5, 'sml', REALToLargeBinary(1.23E-38)}
], {UNSIGNED1 id, STRING3 name, DATA value});

ParquetIO.write(dataset_large_binary, '/var/lib/HPCCSystems/mydropzone/LargeBinaryTest.parquet', TRUE);

largeBinaryDataset := ParquetIO.Read({UNSIGNED1 id; STRING3 name; DATA value}, '/var/lib/HPCCSystems/mydropzone/LargeBinaryTest.parquet');
largeBinaryResult := IF(COUNT(largeBinaryDataset) = 5, 'Pass', 'Fail: Large Binary data count mismatch');

ListRecord := RECORD
    UNSIGNED1 id;
    STRING4 name;
    STRING value;
END;

dataset_large_list := DATASET([
    {1, 'lst1', 'apple,banana,cherry'},
    {2, 'lst2', 'dog,cat,bird,fish'},
    {3, 'lst3', 'red,green,blue,yellow,purple'},
    {4, 'lst4', 'one,two,three,four,five,six,seven'},
    {5, 'lst5', 'Doctor,Teacher,Engineer,Nurse'},
    {6, 'num1', '1,2,3,4,5'},
    {7, 'num2', '10,20,30,40,50,60,70'},
    {8, 'mix1', 'a,1,b,2,c,3'},
    {9, 'mix2', '100,apple,200,banana,300,cherry'},
    {10, 'lst0', 'Make, peace, truth, pictionary, Light, broom, Door, Seige, Fruit'}
], ListRecord);

ParquetIO.write(dataset_large_list, '/var/lib/HPCCSystems/mydropzone/LargeListTest.parquet', TRUE);

largeListDataset := ParquetIO.Read({UNSIGNED1 id; STRING4 name; STRING value}, '/var/lib/HPCCSystems/mydropzone/LargeListTest.parquet');
largeListResult := IF(COUNT(largeListDataset) = 10, 'Pass', 'Fail: Large List data count mismatch');


//All covered arrow data type's tested below

IntegersRec := RECORD
    BOOLEAN null_value;
    UNSIGNED1 uint8_value;
    INTEGER1 int8_value;
    UNSIGNED2 uint16_value;
    INTEGER2 int16_value;
    UNSIGNED4 uint32_value;
    INTEGER4 int32_value;
END;

integers_ds := ParquetIO.read(IntegersRec, '/var/lib/HPCCSystems/mydropzone/integertest.parquet');

integersResult := IF(COUNT(integers_ds) = 559240, 'Pass', 'Fail: Integers data count mismatch');

DiverseRec := RECORD
    UNSIGNED8 uint64_value;
    INTEGER8 int64_value;
    REAL4 half_float_value;
    REAL4 float_value;
    REAL8 double_value;
    STRING string_value;
    DATA binary_value;
END;

diverse_ds := ParquetIO.read(DiverseRec, '/var/lib/HPCCSystems/mydropzone/diverse.parquet');

diverseResult := IF(COUNT(diverse_ds) = 4483853, 'Pass', 'Fail: Diverse data count mismatch');

TimeRec := RECORD
    UNSIGNED date32_value;
    UNSIGNED date64_value;
    UNSIGNED timestamp_value;
    UNSIGNED time32_value;
    UNSIGNED time64_value;
    INTEGER interval_months;
    DECIMAL decimal_value;
    SET OF INTEGER list_value;
END;

times_ds := PARQUETIO.Read(TimeRec,'/var/lib/HPCCSystems/mydropzone/time3.parquet');

timeResult := IF(COUNT(times_ds) = 80659, 'Pass', 'Fail: Time data count mismatch');

INTERVAL_DAY_TIME := RECORD
    INTEGER days;
    INTEGER milliseconds;
END;

EdgeRec := RECORD
    INTERVAL_DAY_TIME interval_day_time_value;
    STRING large_string_value;
    DATA large_binary_value;
    SET OF INTEGER large_list_value;
END;

edges_ds := PARQUETIO.Read(EdgeRec, '/var/lib/HPCCSystems/mydropzone/edgecase1.parquet');

edgeResult := IF(COUNT(edges_ds) = 15768, 'Pass', 'Fail: Edge data count mismatch');

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
    OUTPUT(largeListResult, NAMED('LargeListTest'), OVERWRITE),
    OUTPUT(integersResult, NAMED('IntegersTest'), OVERWRITE),
    OUTPUT(diverseResult, NAMED('DiverseTest'), OVERWRITE),
    OUTPUT(timeResult, NAMED('TimeTest'), OVERWRITE),
    OUTPUT(edgeResult, NAMED('EdgeTest'), OVERWRITE)
);