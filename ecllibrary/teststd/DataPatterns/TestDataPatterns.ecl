IMPORT Std;

EXPORT TestDataPatterns := MODULE

    //--------------------------------------------------------------------------
    // Useful functions
    //--------------------------------------------------------------------------

    SHARED ValueForAttr(ds, attrNameStr, f) := FUNCTIONMACRO
        RETURN ds(attribute = attrNameStr)[1].f;
    ENDMACRO;

    //--------------------------------------------------------------------------
    // Baseline string test
    //--------------------------------------------------------------------------

    SHARED Basic_String := DATASET
        (
            [
                'Dan', 'Steve', '', 'Mike', 'Dan', 'Sebastian', 'Dan'
            ],
            {STRING s}
        );

    SHARED Basic_String_Profile := Std.DataPatterns.Profile(NOFOLD(Basic_String));

    EXPORT Test_Basic_String_Profile :=
        [
            ASSERT(Basic_String_Profile[1].attribute = 's'),
            ASSERT(Basic_String_Profile[1].rec_count = 7),
            ASSERT(Basic_String_Profile[1].given_attribute_type = 'string'),
            ASSERT((DECIMAL9_6)Basic_String_Profile[1].fill_rate = (DECIMAL9_6)85.714286),
            ASSERT(Basic_String_Profile[1].fill_count = 6),
            ASSERT(Basic_String_Profile[1].cardinality = 4),
            ASSERT(Basic_String_Profile[1].best_attribute_type = 'string9'),
            ASSERT(COUNT(Basic_String_Profile[1].modes) = 1),
            ASSERT(Basic_String_Profile[1].modes[1].value = 'Dan'),
            ASSERT(Basic_String_Profile[1].modes[1].rec_count = 3),
            ASSERT(Basic_String_Profile[1].min_length = 3),
            ASSERT(Basic_String_Profile[1].max_length = 9),
            ASSERT(Basic_String_Profile[1].ave_length = 4),
            ASSERT(COUNT(Basic_String_Profile[1].popular_patterns) = 4),
            ASSERT(Basic_String_Profile[1].popular_patterns[1].data_pattern = 'Aaa'),
            ASSERT(Basic_String_Profile[1].popular_patterns[1].rec_count = 3),
            ASSERT(Basic_String_Profile[1].popular_patterns[2].data_pattern = 'Aaaa'),
            ASSERT(Basic_String_Profile[1].popular_patterns[2].rec_count = 1),
            ASSERT(Basic_String_Profile[1].popular_patterns[3].data_pattern = 'Aaaaa'),
            ASSERT(Basic_String_Profile[1].popular_patterns[3].rec_count = 1),
            ASSERT(Basic_String_Profile[1].popular_patterns[4].data_pattern = 'Aaaaaaaaa'),
            ASSERT(Basic_String_Profile[1].popular_patterns[4].rec_count = 1),
            ASSERT(COUNT(Basic_String_Profile[1].rare_patterns) = 0),
            ASSERT(Basic_String_Profile[1].is_numeric = FALSE),
            ASSERT(Basic_String_Profile[1].numeric_min = 0),
            ASSERT(Basic_String_Profile[1].numeric_max = 0),
            ASSERT(Basic_String_Profile[1].numeric_mean = 0),
            ASSERT(Basic_String_Profile[1].numeric_std_dev = 0),
            ASSERT(Basic_String_Profile[1].numeric_lower_quartile = 0),
            ASSERT(Basic_String_Profile[1].numeric_median = 0),
            ASSERT(Basic_String_Profile[1].numeric_upper_quartile = 0),
            ASSERT(COUNT(Basic_String_Profile[1].correlations) = 0),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Baseline numeric test
    //--------------------------------------------------------------------------

    SHARED Basic_Numeric := DATASET
        (
            [
                -1000, 500, -250, 2000, 1500, -2000, 2000
            ],
            {INTEGER n}
        );

    SHARED Basic_Numeric_Profile := Std.DataPatterns.Profile(NOFOLD(Basic_Numeric));

    EXPORT Test_Basic_Numeric_Profile :=
        [
            ASSERT(Basic_Numeric_Profile[1].attribute = 'n'),
            ASSERT(Basic_Numeric_Profile[1].rec_count = 7),
            ASSERT(Basic_Numeric_Profile[1].given_attribute_type = 'integer8'),
            ASSERT(Basic_Numeric_Profile[1].fill_rate = 100),
            ASSERT(Basic_Numeric_Profile[1].fill_count = 7),
            ASSERT(Basic_Numeric_Profile[1].cardinality = 6),
            ASSERT(Basic_Numeric_Profile[1].best_attribute_type = 'integer8'),
            ASSERT(COUNT(Basic_Numeric_Profile[1].modes) = 1),
            ASSERT(Basic_Numeric_Profile[1].modes[1].value = '2000'),
            ASSERT(Basic_Numeric_Profile[1].modes[1].rec_count = 2),
            ASSERT(Basic_Numeric_Profile[1].min_length = 3),
            ASSERT(Basic_Numeric_Profile[1].max_length = 5),
            ASSERT(Basic_Numeric_Profile[1].ave_length = 4),
            ASSERT(COUNT(Basic_Numeric_Profile[1].popular_patterns) = 4),
            ASSERT(Basic_Numeric_Profile[1].popular_patterns[1].data_pattern = '9999'),
            ASSERT(Basic_Numeric_Profile[1].popular_patterns[1].rec_count = 3),
            ASSERT(Basic_Numeric_Profile[1].popular_patterns[2].data_pattern = '-9999'),
            ASSERT(Basic_Numeric_Profile[1].popular_patterns[2].rec_count = 2),
            ASSERT(Basic_Numeric_Profile[1].popular_patterns[3].data_pattern = '-999'),
            ASSERT(Basic_Numeric_Profile[1].popular_patterns[3].rec_count = 1),
            ASSERT(Basic_Numeric_Profile[1].popular_patterns[4].data_pattern = '999'),
            ASSERT(Basic_Numeric_Profile[1].popular_patterns[4].rec_count = 1),
            ASSERT(COUNT(Basic_Numeric_Profile[1].rare_patterns) = 0),
            ASSERT(Basic_Numeric_Profile[1].is_numeric = TRUE),
            ASSERT(Basic_Numeric_Profile[1].numeric_min = -2000),
            ASSERT(Basic_Numeric_Profile[1].numeric_max = 2000),
            ASSERT(Basic_Numeric_Profile[1].numeric_mean = 392.8571),
            ASSERT(Basic_Numeric_Profile[1].numeric_std_dev = 1438.3593),
            ASSERT(Basic_Numeric_Profile[1].numeric_lower_quartile = -1000),
            ASSERT(Basic_Numeric_Profile[1].numeric_median = 500),
            ASSERT(Basic_Numeric_Profile[1].numeric_upper_quartile = 2000),
            ASSERT(COUNT(Basic_Numeric_Profile[1].correlations) = 0),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Empty data detection
    //--------------------------------------------------------------------------

    // Layout contains every ECL data type that Profile can process (except for
    // SET OF datatypes, child records, and child datasets)
    SHARED EmptyDataLayout := RECORD
        BOOLEAN f_boolean;
        INTEGER f_integer;
        UNSIGNED f_unsigned;
        UNSIGNED INTEGER f_unsigned_integer;
        BIG_ENDIAN INTEGER f_big_endian_integer;
        BIG_ENDIAN UNSIGNED f_big_endian_unsigned;
        BIG_ENDIAN UNSIGNED INTEGER f_big_endian_unsigned_integer;
        LITTLE_ENDIAN INTEGER f_little_endian_integer;
        LITTLE_ENDIAN UNSIGNED f_little_endian_unsigned;
        LITTLE_ENDIAN UNSIGNED INTEGER f_little_endian_unsigned_integer;
        REAL f_real;
        DECIMAL32 f_decimal32;
        DECIMAL32_6 f_decimal32_6;
        UDECIMAL32 f_udecimal32;
        UDECIMAL32_6 f_udecimal32_6;
        UNSIGNED DECIMAL32 f_unsigned_decimal32;
        UNSIGNED DECIMAL32_6 f_unsigned_decimal32_6;
        STRING f_string;
        STRING256 f_string256;
        ASCII STRING f_ascii_string;
        ASCII STRING256 f_ascii_string256;
        EBCDIC STRING f_ebcdic_string;
        EBCDIC STRING256 f_ebcdic_string256;
        QSTRING f_qstring;
        QSTRING256 f_qstring256;
        UNICODE f_unicode;
        UNICODE_de f_unicode_de;
        UNICODE256 f_unicode256;
        UNICODE_de256 f_unicode_de256;
        UTF8 f_utf8;
        UTF8_de f_utf8_de;
        DATA f_data;
        DATA16 f_data16;
        VARSTRING f_varstring;
        VARSTRING256 f_varstring256;
        VARUNICODE f_varunicode;
        VARUNICODE_de f_varunicode_de;
        VARUNICODE256 f_varunicode256;
        VARUNICODE_de256 f_varunicode_de256;
    END;

    SHARED Empty_Data := DATASET
        (
            1,
            TRANSFORM
                (
                    EmptyDataLayout,
                    SELF := []
                )
        );

    SHARED Empty_Data_Profile := Std.DataPatterns.Profile(NOFOLD(Empty_Data), features := 'cardinality,best_ecl_types,lengths,modes,patterns');

    // Convenience function for testing the same thing for several attributes
    SHARED TestEmptyAttr(STRING attributeName) :=
        [
            ASSERT(ValueForAttr(Empty_Data_Profile, attributeName, cardinality) = 0),
            ASSERT(ValueForAttr(Empty_Data_Profile, attributeName, best_attribute_type) = ValueForAttr(Empty_Data_Profile, attributeName, given_attribute_type)),
            ASSERT(ValueForAttr(Empty_Data_Profile, attributeName, min_length) = 0),
            ASSERT(ValueForAttr(Empty_Data_Profile, attributeName, max_length) = 0),
            ASSERT(ValueForAttr(Empty_Data_Profile, attributeName, ave_length) = 0),
            ASSERT(COUNT(ValueForAttr(Empty_Data_Profile, attributeName, popular_patterns)) = 0),
            ASSERT(COUNT(ValueForAttr(Empty_Data_Profile, attributeName, rare_patterns)) = 0),
            ASSERT(TRUE)
        ];

    EXPORT Test_Empty_Data_Profile :=
        [
            TestEmptyAttr('f_ascii_string'),
            TestEmptyAttr('f_ascii_string16'),
            TestEmptyAttr('f_big_endian_integer'),
            TestEmptyAttr('f_big_endian_unsigned'),
            TestEmptyAttr('f_big_endian_unsigned_integer'),
            TestEmptyAttr('f_data'),
            TestEmptyAttr('f_decimal32'),
            TestEmptyAttr('f_decimal32_6'),
            TestEmptyAttr('f_ebcdic_string'),
            TestEmptyAttr('f_ebcdic_string16'),
            TestEmptyAttr('f_integer'),
            TestEmptyAttr('f_little_endian_integer'),
            TestEmptyAttr('f_little_endian_unsigned'),
            TestEmptyAttr('f_little_endian_unsigned_integer'),
            TestEmptyAttr('f_qstring'),
            TestEmptyAttr('f_qstring16'),
            TestEmptyAttr('f_real'),
            TestEmptyAttr('f_string'),
            TestEmptyAttr('f_string16'),
            TestEmptyAttr('f_udecimal32'),
            TestEmptyAttr('f_udecimal32_6'),
            TestEmptyAttr('f_unicode'),
            TestEmptyAttr('f_unicode16'),
            TestEmptyAttr('f_unicode_de'),
            TestEmptyAttr('f_unicode_de16'),
            TestEmptyAttr('f_unsigned'),
            TestEmptyAttr('f_unsigned_decimal32'),
            TestEmptyAttr('f_unsigned_decimal32_6'),
            TestEmptyAttr('f_unsigned_integer'),
            TestEmptyAttr('f_utf8'),
            TestEmptyAttr('f_utf8_de'),
            TestEmptyAttr('f_varstring'),
            TestEmptyAttr('f_varstring16'),
            TestEmptyAttr('f_varunicode'),
            TestEmptyAttr('f_varunicode16'),
            TestEmptyAttr('f_varunicode_de'),
            TestEmptyAttr('f_varunicode_de16'),

            // Handle BOOLEAN special because it is not truly empty
            ASSERT(ValueForAttr(Empty_Data_Profile, 'f_boolean', cardinality) = 1),
            ASSERT(ValueForAttr(Empty_Data_Profile, 'f_boolean', best_attribute_type) = ValueForAttr(Empty_Data_Profile, 'f_boolean', given_attribute_type)),
            ASSERT(ValueForAttr(Empty_Data_Profile, 'f_boolean', min_length) = 1),
            ASSERT(ValueForAttr(Empty_Data_Profile, 'f_boolean', max_length) = 1),
            ASSERT(ValueForAttr(Empty_Data_Profile, 'f_boolean', ave_length) = 1),
            ASSERT(COUNT(ValueForAttr(Empty_Data_Profile, 'f_boolean', popular_patterns)) = 1),
            ASSERT(COUNT(ValueForAttr(Empty_Data_Profile, 'f_boolean', rare_patterns)) = 0),

            // Handle fixed-length DATA special because it is not truly empty
            ASSERT(ValueForAttr(Empty_Data_Profile, 'f_data16', cardinality) = 1),
            ASSERT(ValueForAttr(Empty_Data_Profile, 'f_data16', best_attribute_type) = ValueForAttr(Empty_Data_Profile, 'f_data16', given_attribute_type)),
            ASSERT(ValueForAttr(Empty_Data_Profile, 'f_data16', min_length) = 16),
            ASSERT(ValueForAttr(Empty_Data_Profile, 'f_data16', max_length) = 16),
            ASSERT(ValueForAttr(Empty_Data_Profile, 'f_data16', ave_length) = 16),
            ASSERT(COUNT(ValueForAttr(Empty_Data_Profile, 'f_data16', popular_patterns)) = 1),
            ASSERT(COUNT(ValueForAttr(Empty_Data_Profile, 'f_data16', rare_patterns)) = 0),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Unicode pattern detection
    //--------------------------------------------------------------------------

    SHARED Pattern_Unicode := DATASET
        (
            [
                U'abcd\353', U'ABCDË'
            ],
            {UNICODE_de5 s}
        );

    SHARED Pattern_Unicode_Profile := Std.DataPatterns.Profile(NOFOLD(Pattern_Unicode), features := 'patterns');

    EXPORT Test_Pattern_Unicode_Profile :=
        [
            ASSERT(Pattern_Unicode_Profile[1].given_attribute_type = 'unicode_de5'),
            ASSERT(COUNT(Pattern_Unicode_Profile[1].popular_patterns) = 2),
            ASSERT(Pattern_Unicode_Profile[1].popular_patterns[1].data_pattern = 'AAAAA'),
            ASSERT(Pattern_Unicode_Profile[1].popular_patterns[1].rec_count = 1),
            ASSERT(Pattern_Unicode_Profile[1].popular_patterns[2].data_pattern = 'aaaaa'),
            ASSERT(Pattern_Unicode_Profile[1].popular_patterns[2].rec_count = 1),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Punctuation pattern test
    //--------------------------------------------------------------------------

    SHARED Pattern_Punctuation := DATASET
        (
            [
                'This! Is- Not. Helpful?'
            ],
            {STRING s}
        );

    SHARED Pattern_Punctuation_Profile := Std.DataPatterns.Profile(NOFOLD(Pattern_Punctuation), features := 'patterns');

    EXPORT Test_Pattern_Punctuation_Profile :=
        [
            ASSERT(Pattern_Punctuation_Profile[1].attribute = 's'),
            ASSERT(COUNT(Pattern_Punctuation_Profile[1].popular_patterns) = 1),
            ASSERT(Pattern_Punctuation_Profile[1].popular_patterns[1].data_pattern = 'Aaaa! Aa- Aaa. Aaaaaaa?'),
            ASSERT(Pattern_Punctuation_Profile[1].popular_patterns[1].rec_count = 1),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Finding integers within strings test
    //--------------------------------------------------------------------------

    SHARED Best_Integer := DATASET
        (
            [
                {'-100', '-100', '-1000', '-10000', '-100000'},
                {'100', '100', '1000', '10000', '100000'}
            ],
            {STRING s1, STRING s2, STRING s3, STRING s4, STRING s5}
        );

    SHARED Best_Integer_Profile := Std.DataPatterns.Profile(NOFOLD(Best_Integer), features := 'best_ecl_types');

    EXPORT Test_Best_Integer_Profile :=
        [
            ASSERT(ValueForAttr(Best_Integer_Profile, 's1', best_attribute_type) = 'integer2'),
            ASSERT(ValueForAttr(Best_Integer_Profile, 's2', best_attribute_type) = 'integer2'),
            ASSERT(ValueForAttr(Best_Integer_Profile, 's3', best_attribute_type) = 'integer3'),
            ASSERT(ValueForAttr(Best_Integer_Profile, 's4', best_attribute_type) = 'integer3'),
            ASSERT(ValueForAttr(Best_Integer_Profile, 's5', best_attribute_type) = 'integer4'),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Finding unsigned integers within strings test
    //--------------------------------------------------------------------------

    SHARED Best_Unsigned := DATASET
        (
            [
                {'100', '100', '1000', '10000', '100000'}
            ],
            {STRING s1, STRING s2, STRING s3, STRING s4, STRING s5}
        );

    SHARED Best_Unsigned_Profile := Std.DataPatterns.Profile(NOFOLD(Best_Unsigned), features := 'best_ecl_types');

    EXPORT Test_Best_Unsigned_Profile :=
        [
            ASSERT(ValueForAttr(Best_Unsigned_Profile, 's1', best_attribute_type) = 'unsigned2'),
            ASSERT(ValueForAttr(Best_Unsigned_Profile, 's2', best_attribute_type) = 'unsigned2'),
            ASSERT(ValueForAttr(Best_Unsigned_Profile, 's3', best_attribute_type) = 'unsigned2'),
            ASSERT(ValueForAttr(Best_Unsigned_Profile, 's4', best_attribute_type) = 'unsigned3'),
            ASSERT(ValueForAttr(Best_Unsigned_Profile, 's5', best_attribute_type) = 'unsigned3'),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Finding reals within strings test
    //--------------------------------------------------------------------------

    SHARED Best_Real := DATASET
        (
            [
                {'99.99', '-99.99', '9.1234e-10', '.123', '99.0'}
            ],
            {STRING s1, STRING s2, STRING s3, STRING s4, STRING s5}
        );

    SHARED Best_Real_Profile := Std.DataPatterns.Profile(NOFOLD(Best_Real), features := 'best_ecl_types');

    EXPORT Test_Best_Real_Profile :=
        [
            ASSERT(ValueForAttr(Best_Real_Profile, 's1', best_attribute_type) = 'real4'),
            ASSERT(ValueForAttr(Best_Real_Profile, 's2', best_attribute_type) = 'real4'),
            ASSERT(ValueForAttr(Best_Real_Profile, 's3', best_attribute_type) = 'real8'),
            ASSERT(ValueForAttr(Best_Real_Profile, 's4', best_attribute_type) = 'real4'),
            ASSERT(ValueForAttr(Best_Real_Profile, 's5', best_attribute_type) = 'real4'),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Not actually numbers test
    //--------------------------------------------------------------------------

    SHARED Best_NaN := DATASET
        (
            [
                {'123456789012345678901', '-12345678901234567890', '9.1234e-1000', '99.1234567890123456', '123456789012345678901.0'}
            ],
            {STRING s1, STRING s2, STRING s3, STRING s4, STRING s5}
        );

    SHARED Best_NaN_Profile := Std.DataPatterns.Profile(NOFOLD(Best_NaN), features := 'best_ecl_types');

    EXPORT Test_Best_NaN_Profile :=
        [
            ASSERT(ValueForAttr(Best_NaN_Profile, 's1', best_attribute_type) = 'string21'),
            ASSERT(ValueForAttr(Best_NaN_Profile, 's2', best_attribute_type) = 'string21'),
            ASSERT(ValueForAttr(Best_NaN_Profile, 's3', best_attribute_type) = 'string12'),
            ASSERT(ValueForAttr(Best_NaN_Profile, 's4', best_attribute_type) = 'string19'),
            ASSERT(ValueForAttr(Best_NaN_Profile, 's5', best_attribute_type) = 'string23'),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Embedded child record test
    //--------------------------------------------------------------------------

    SHARED Embedded_Child1 := DATASET
        (
            [
                {'Dan', {123, 345, 567}},
                {'Mike', {987, 765, 543}}
            ],
            {STRING s, {UNSIGNED4 x, UNSIGNED4 y, UNSIGNED4 z} foo}
        );

    SHARED Embedded_Child1_Profile := Std.DataPatterns.Profile(NOFOLD(Embedded_Child1));

    EXPORT Test_Embedded_Child1_Profile :=
        [
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].attribute = 'foo.x'),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].rec_count = 2),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].given_attribute_type = 'unsigned4'),
            ASSERT((DECIMAL9_6)Embedded_Child1_Profile(attribute = 'foo.x')[1].fill_rate = (DECIMAL9_6)100),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].fill_count = 2),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].cardinality = 2),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].best_attribute_type = 'unsigned4'),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].min_length = 3),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].max_length = 3),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].ave_length = 3),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = 'foo.x')[1].popular_patterns) = 1),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = 'foo.x')[1].rare_patterns) = 0),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].is_numeric = TRUE),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].numeric_min = 123),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].numeric_max = 987),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].numeric_mean = 555),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].numeric_std_dev = 432),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].numeric_lower_quartile = 123),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].numeric_median = 555),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.x')[1].numeric_upper_quartile = 0),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = 'foo.x')[1].correlations) = 2),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].attribute = 'foo.y'),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].rec_count = 2),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].given_attribute_type = 'unsigned4'),
            ASSERT((DECIMAL9_6)Embedded_Child1_Profile(attribute = 'foo.y')[1].fill_rate = (DECIMAL9_6)100),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].fill_count = 2),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].cardinality = 2),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].best_attribute_type = 'unsigned4'),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].min_length = 3),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].max_length = 3),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].ave_length = 3),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = 'foo.y')[1].popular_patterns) = 1),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = 'foo.y')[1].rare_patterns) = 0),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].is_numeric = TRUE),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].numeric_min = 345),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].numeric_max = 765),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].numeric_mean = 555),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].numeric_std_dev = 210),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].numeric_lower_quartile = 345),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].numeric_median = 555),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.y')[1].numeric_upper_quartile = 0),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = 'foo.y')[1].correlations) = 2),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].attribute = 'foo.z'),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].rec_count = 2),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].given_attribute_type = 'unsigned4'),
            ASSERT((DECIMAL9_6)Embedded_Child1_Profile(attribute = 'foo.z')[1].fill_rate = (DECIMAL9_6)100),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].fill_count = 2),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].cardinality = 2),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].best_attribute_type = 'unsigned4'),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].min_length = 3),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].max_length = 3),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].ave_length = 3),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = 'foo.z')[1].popular_patterns) = 1),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = 'foo.z')[1].rare_patterns) = 0),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].is_numeric = TRUE),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].numeric_min = 543),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].numeric_max = 567),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].numeric_mean = 555),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].numeric_std_dev = 12),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].numeric_lower_quartile = 543),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].numeric_median = 555),
            ASSERT(Embedded_Child1_Profile(attribute = 'foo.z')[1].numeric_upper_quartile = 0),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = 'foo.z')[1].correlations) = 2),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Test strings fields containing numerics with leading zeros (issue 42)
    //--------------------------------------------------------------------------

    SHARED Leading_Zeros := DATASET
        (
            [
                {'0100', '1234', '0001', '7809', '-0600'},
                {'0020', '0001', '0023', '0001', '600'}
            ],
            {STRING s1, STRING s2, STRING s3, STRING s4, STRING s5}
        );

    SHARED Leading_Zeros_Profile := Std.DataPatterns.Profile(NOFOLD(Leading_Zeros), features := 'best_ecl_types');

    EXPORT Test_Leading_Zeros_Profile :=
        [
            ASSERT(ValueForAttr(Leading_Zeros_Profile, 's1', best_attribute_type) = 'string4'),
            ASSERT(ValueForAttr(Leading_Zeros_Profile, 's2', best_attribute_type) = 'string4'),
            ASSERT(ValueForAttr(Leading_Zeros_Profile, 's3', best_attribute_type) = 'string4'),
            ASSERT(ValueForAttr(Leading_Zeros_Profile, 's4', best_attribute_type) = 'string4'),
            ASSERT(ValueForAttr(Leading_Zeros_Profile, 's5', best_attribute_type) = 'integer3'),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // String fields with wildly varying lengths (three orders of magnitude
    // difference) should become variable-length 'string' datatypes
    //--------------------------------------------------------------------------

    SHARED STRING MLS(UNSIGNED4 len) := EMBED(C++)
        const char  letters[] = "abcdefghijklmnopqrstuvwxyz0123456789";

        __lenResult = len;
        __result = static_cast<char*>(rtlMalloc(__lenResult));

        for (uint32_t x = 0; x < len; x++)
            __result[x] = letters[rand() % 36];
    ENDEMBED;

    SHARED Large_Strings := DATASET
        (
            [
                {'abcd', '1234', '0001', '7', '-0600'},
                {'0020', MLS(5000), MLS(500), MLS(1050), '600'}
            ],
            {STRING s1, STRING s2, STRING s3, STRING s4, STRING s5}
        );

    SHARED Large_Strings_Profile := Std.DataPatterns.Profile(NOFOLD(Large_Strings), features := 'best_ecl_types');

    EXPORT Test_Large_Strings_Profile :=
        [
            ASSERT(ValueForAttr(Large_Strings_Profile, 's1', best_attribute_type) = 'string4'),
            ASSERT(ValueForAttr(Large_Strings_Profile, 's2', best_attribute_type) = 'string'),
            ASSERT(ValueForAttr(Large_Strings_Profile, 's3', best_attribute_type) = 'string500'),
            ASSERT(ValueForAttr(Large_Strings_Profile, 's4', best_attribute_type) = 'string'),
            ASSERT(ValueForAttr(Large_Strings_Profile, 's5', best_attribute_type) = 'integer3'),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Test strings fields containing numerics with leading zeros (issue 42)
    //--------------------------------------------------------------------------

    SHARED SetOf_Types := DATASET
        (
            [
                {1, [1,2,3,4]},
                {100, [9,8]},
                {200, [4,4,4,4,4,4,4,4,4,4,4]},
                {300, []},
                {150, [5,6]}
            ],
            {
                UNSIGNED2           n,
                SET OF UNSIGNED2    my_set
            }
        );

    SHARED SetOf_Types_Profile := Std.DataPatterns.Profile(NOFOLD(SetOf_Types));

    EXPORT Test_SetOf_Types_Profile :=
        [
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].attribute = 'my_set'),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].rec_count = 5),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].given_attribute_type = 'set of unsigned2'),
            ASSERT((DECIMAL9_6)SetOf_Types_Profile(attribute = 'my_set')[1].fill_rate = (DECIMAL9_6)80),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].fill_count = 4),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].cardinality = 4),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].best_attribute_type = 'set of unsigned2'),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].min_length = 2),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].max_length = 11),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].ave_length = 4),
            ASSERT(COUNT(SetOf_Types_Profile(attribute = 'my_set')[1].popular_patterns) = 3),
            ASSERT(COUNT(SetOf_Types_Profile(attribute = 'my_set')[1].rare_patterns) = 0),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].is_numeric = FALSE),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].numeric_min = 0),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].numeric_max = 0),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].numeric_mean = 0),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].numeric_std_dev = 0),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].numeric_lower_quartile = 0),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].numeric_median = 0),
            ASSERT(SetOf_Types_Profile(attribute = 'my_set')[1].numeric_upper_quartile = 0),
            ASSERT(COUNT(SetOf_Types_Profile(attribute = 'my_set')[1].correlations) = 0),
            ASSERT(TRUE)
        ];

    EXPORT Main := [
        EVALUATE(Test_Basic_String_Profile), 
        EVALUATE(Test_Basic_Numeric_Profile),
        EVALUATE(Test_Empty_Data_Profile),
        EVALUATE(Test_Pattern_Unicode_Profile),
        EVALUATE(Test_Pattern_Punctuation_Profile),
        EVALUATE(Test_Best_Integer_Profile),
        EVALUATE(Test_Best_Unsigned_Profile),
        EVALUATE(Test_Best_Real_Profile),
        EVALUATE(Test_Best_NaN_Profile),
        EVALUATE(Test_Embedded_Child1_Profile),
        EVALUATE(Test_Leading_Zeros_Profile),
        EVALUATE(Test_Large_Strings_Profile),
        EVALUATE(Test_SetOf_Types_Profile)
    ];
END;
