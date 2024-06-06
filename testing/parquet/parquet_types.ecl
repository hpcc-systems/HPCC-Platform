//class=ParquetRegression
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
], {UNSIGNED testid, STRING3 testname, BOOLEAN value}), '/tmp/BooleanTest.parquet', TRUE);

// INTEGER type
ParquetIO.write(DATASET([
    {010, 'aai', 123},
    {011, 'aaj', -987},
    {012, 'aak', 456},
    {013, 'aal', 789},
    {014, 'aam', -321}
], {UNSIGNED testid, STRING3 testname, INTEGER value}), '/tmp/IntegerTest.parquet', TRUE);

// UNSIGNED type
ParquetIO.write(DATASET([
    {020, 'aan', 12345},
    {021, 'aao', 67890},
    {022, 'aap', 1234},
    {023, 'aaq', 5678},
    {024, 'aar', 91011}
], {UNSIGNED testid, STRING3 testname, UNSIGNED value}), '/tmp/UnsignedTest.parquet', TRUE);

// REAL type
ParquetIO.write(DATASET([
    {030, 'aas', 1.23},
    {031, 'aat', -9.87},
    {032, 'aau', 45.67},
    {033, 'aav', 78.90},
    {034, 'aaw', -32.1}
], {UNSIGNED testid, STRING3 testname, REAL value}), '/tmp/RealTest.parquet', TRUE);

// DECIMAL type
ParquetIO.write(DATASET([
    {040, 'aax', 12.34D},
    {041, 'aay', -56.78D},
    {042, 'aaz', 90.12D},
    {043, 'aba', 34.56D},
    {044, 'abb', -78.90D}
], {UNSIGNED testid, STRING3 testname, DECIMAL10_2 value}), '/tmp/DecimalTest.parquet', TRUE);

// STRING type
ParquetIO.write(DATASET([
    {050, 'abc', 'Hello'},
    {051, 'abd', 'World'},
    {052, 'abe', 'Test'},
    {053, 'abf', 'String'},
    {054, 'abg', 'Types'}
], {UNSIGNED testid, STRING3 testname, STRING value}), '/tmp/StringTest.parquet', TRUE);

// DATA type (converted to STRING)
ParquetIO.write(DATASET([
    {060, 'abh', (STRING)X'0123456789ABCDEF'},
    {061, 'abi', (STRING)X'FEDCBA9876543210'},
    {062, 'abj', (STRING)X'00FF00FF00FF00FF'},
    {063, 'abk', (STRING)X'FF00FF00FF00FF00'},
    {064, 'abl', (STRING)X'0A0B0C0D0E0F1011'}
], {UNSIGNED testid, STRING3 testname, STRING value}), '/tmp/DataAsStringTest.parquet', TRUE);

// VARSTRING type
ParquetIO.write(DATASET([
    {070, 'abm', 'VarString1'},
    {071, 'abn', 'VarString2'},
    {072, 'abo', 'VarString3'},
    {073, 'abp', 'VarString4'},
    {074, 'abq', 'VarString5'}
], {UNSIGNED testid, STRING3 testname, VARSTRING value}), '/tmp/VarStringTest.parquet', TRUE);

// QSTRING type
ParquetIO.write(DATASET([
    {080, 'abr', 'QStr1'},
    {081, 'abs', 'QStr2'},
    {082, 'abt', 'QStr3'},
    {083, 'abu', 'QStr4'},
    {084, 'abv', 'QStr5'}
], {UNSIGNED testid, STRING3 testname, QSTRING value}), '/tmp/QStringTest.parquet', TRUE);

// UTF8 type
ParquetIO.write(DATASET([
    {090, 'abw', U'UTF8_1'},
    {091, 'abx', U'UTF8_2'},
    {092, 'aby', U'UTF8_3'},
    {093, 'abz', U'UTF8_4'},
    {094, 'aca', U'UTF8_5'}
], {UNSIGNED testid, STRING3 testname, UTF8 value}), '/tmp/UTF8Test.parquet', TRUE);

// UNICODE type
ParquetIO.write(DATASET([
    {100, 'acb', U'Unicode1'},
    {101, 'acc', U'Unicode2'},
    {102, 'acd', U'Unicode3'},
    {103, 'ace', U'Unicode4'},
    {104, 'acf', U'Unicode5'}
], {UNSIGNED testid, STRING3 testname, UNICODE value}), '/tmp/UnicodeTest.parquet', TRUE);

// SET OF INTEGER type
ParquetIO.write(DATASET([
    {110, 'acg', [1,2,3]},
    {111, 'ach', [4,5,6]},
    {112, 'aci', [7,8,9]},
    {113, 'acj', [10,11,12]},
    {114, 'ack', [13,14,15]}
], {UNSIGNED testid, STRING3 testname, SET OF INTEGER value}), '/tmp/SetOfIntegerTest.parquet', TRUE);

// VARUNICODE type
ParquetIO.write(DATASET([
    {120, 'acl', U'VarUnicode1'},
    {121, 'acm', U'VarUnicode2'},
    {122, 'acn', U'VarUnicode3'},
    {123, 'aco', U'VarUnicode4'},
    {124, 'acp', U'VarUnicode5'}
], {UNSIGNED testid, STRING3 testname, VARUNICODE value}), '/tmp/VarUnicodeTest.parquet', TRUE);

// REAL8 (FLOAT8) type
ParquetIO.write(DATASET([
    {170, 'adk', 1.23D},
    {171, 'adl', -9.87D},
    {172, 'adm', 3.14159265358979D},
    {173, 'adn', 2.71828182845904D},
    {174, 'ado', -1.41421356237309D}
], {UNSIGNED testid, STRING3 testname, REAL8 value}), '/tmp/Real8Test.parquet', TRUE);

// SET OF STRING type
ParquetIO.write(DATASET([
    {180, 'adp', ['Set', 'Of', 'String', 'Test']},
    {181, 'adq', ['ECL', 'Data', 'Types']},
    {182, 'adr', ['Hello', 'World']},
    {183, 'ads', ['One', 'Two', 'Three', 'Four', 'Five']},
    {184, 'adt', ['A', 'B', 'C', 'D', 'E']}
], {UNSIGNED testid, STRING3 testname, SET OF STRING value}), '/tmp/SetOfStringTest.parquet', TRUE);

// SET OF UNICODE type
ParquetIO.write(DATASET([
    {190, 'adu', [U'Unicode', U'Set', U'Test']},
    {191, 'adv', [U'こんにちは', U'世界']},
    {192, 'adw', [U'Á', U'É', U'Í', U'Ó', U'Ú']},
    {193, 'adx', [U'α', U'β', U'γ', U'δ', U'ε']},
    {194, 'ady', [U'☀', U'☁', U'☂', U'☃', U'☄']}
], {UNSIGNED testid, STRING3 testname, SET OF UNICODE value}), '/tmp/SetOfUnicodeTest.parquet', TRUE);


ParquetIO.write(DATASET([
    {300, 'afa', (INTEGER2)32767},
    {301, 'afb', (INTEGER4)2147483647},
    {302, 'afc', (INTEGER8)9223372036854775807}
], {UNSIGNED testid, STRING3 testname, INTEGER8 value}), '/tmp/IntegerSizesTest.parquet', TRUE);

// UNSIGNED2, UNSIGNED4, UNSIGNED8
ParquetIO.write(DATASET([
    {310, 'afd', (UNSIGNED2)65535},
    {311, 'afe', (UNSIGNED4)4294967295},
    {312, 'aff', (UNSIGNED8)18446744073709551615}
], {UNSIGNED testid, STRING3 testname, UNSIGNED8 value}), '/tmp/UnsignedSizesTest.parquet', TRUE);

// REAL4 (FLOAT4)
ParquetIO.write(DATASET([
    {320, 'afg', (REAL4)1.23},
    {321, 'afh', (REAL4)-9.87},
    {322, 'afi', (REAL4)3.14159}
], {UNSIGNED testid, STRING3 testname, REAL4 value}), '/tmp/Real4Test.parquet', TRUE);

// INTEGER1 (BYTE) type
ParquetIO.write(DATASET([
    {340, 'afp', 127},
    {341, 'afq', -128},
    {342, 'afr', 0}
], {UNSIGNED testid, STRING3 testname, INTEGER1 value}), '/tmp/Integer1Test.parquet', TRUE);

PARALLEL(
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; BOOLEAN value}, '/tmp/BooleanTest.parquet'), NAMED('BooleanTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; INTEGER value}, '/tmp/IntegerTest.parquet'), NAMED('IntegerTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; UNSIGNED value}, '/tmp/UnsignedTest.parquet'), NAMED('UnsignedTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; REAL value}, '/tmp/RealTest.parquet'), NAMED('RealTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; DECIMAL10_2 value}, '/tmp/DecimalTest.parquet'), NAMED('DecimalTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; STRING value}, '/tmp/StringTest.parquet'), NAMED('StringTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; STRING value}, '/tmp/DataAsStringTest.parquet'), NAMED('DataAsStringTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; VARSTRING value}, '/tmp/VarStringTest.parquet'), NAMED('VarStringTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; QSTRING value}, '/tmp/QStringTest.parquet'), NAMED('QStringTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; UTF8 value}, '/tmp/UTF8Test.parquet'), NAMED('UTF8Test')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; UNICODE value}, '/tmp/UnicodeTest.parquet'), NAMED('UnicodeTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; SET OF INTEGER value}, '/tmp/SetOfIntegerTest.parquet'), NAMED('SetOfIntegerTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; VARUNICODE value}, '/tmp/VarUnicodeTest.parquet'), NAMED('VarUnicodeTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; REAL8 value}, '/tmp/Real8Test.parquet'), NAMED('Real8Test')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; SET OF STRING value}, '/tmp/SetOfStringTest.parquet'), NAMED('SetOfStringTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; SET OF UNICODE value}, '/tmp/SetOfUnicodeTest.parquet'), NAMED('SetOfUnicodeTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; INTEGER8 value}, '/tmp/IntegerSizesTest.parquet'), NAMED('IntegerSizesTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; UNSIGNED8 value}, '/tmp/UnsignedSizesTest.parquet'), NAMED('UnsignedSizesTest')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; REAL4 value}, '/tmp/Real4Test.parquet'), NAMED('Real4Test')),
    OUTPUT(ParquetIO.read({UNSIGNED testid; STRING3 testname; INTEGER1 value}, '/tmp/Integer1Test.parquet'), NAMED('Integer1Test'))

);

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

OUTPUT(integers_ds, NAMED('IntegersTest'));


DiverseRec := RECORD
    UNSIGNED8 uint64_value;
    INTEGER8 int64_value;
    REAL4 half_float_value;
    REAL4 float_value;
    REAL8 double_value;
    STRING string_value;
    DATA binary_value;
    DATA fixed_size_binary_value;
END;

diverse_ds := ParquetIO.read(DiverseRec, '/var/lib/HPCCSystems/mydropzone/diverse.parquet');

OUTPUT(diverse_ds, NAMED('DiverseTest'));

TimeRec := RECORD
    UNSIGNED date32_value;
    UNSIGNED date64_value;
    UNSIGNED timestamp_value;
    UNSIGNED time32_value;
    UNSIGNED time64_value;
    INTEGER interval_months;
    DECIMAL decimal_value;
    INTEGER list_value;
END;

times_ds := PARQUETIO.Read(TimeRec,'/var/lib/HPCCSystems/mydropzone/time2.parquet');

OUTPUT(times_ds, NAMED('TimeTest'));


INTERVAL_DAY_TIME := RECORD
    INTEGER4 days;
    INTEGER8 milliseconds;
END;

MAP_RECORD := RECORD
    STRING key;
    INTEGER4 value;
END;

EdgeRec := RECORD
   INTERVAL_DAY_TIME interval_day_time_value;
   DATASET(MAP_RECORD) map_value;
    STRING large_string_value;
    DATA large_binary_value;
    SET OF INTEGER4 large_list_value;
   
END;

edges_ds := PARQUETIO.Read(EdgeRec, '/var/lib/HPCCSystems/mydropzone/edgecase.parquet');
OUTPUT(edges_ds, NAMED('EdgeTest'));

