<Archive build="internal_7.6.19-closedown0"
         eclVersion="7.6.19"
         legacyImport="0"
         legacyWhen="0">
 <Query attributePath="_local_directory_.teststdhthor"/>
 <Module key="_local_directory_" name="_local_directory_">
  <Attribute key="teststdhthor"
             name="teststdhthor"
             sourcePath="/home/gavin/dev/hpcc/ecl/regress/teststdhthor.ecl"
             ts="1584549018000000">
   /*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#option (&apos;targetClusterType&apos;, &apos;hthor&apos;);

import teststd;

runTests := teststd.DataPatterns.TestDataPatterns.Main;
runTests;
//export teststd := runTests;&#10;
  </Attribute>
 </Module>
 <Module key="teststd" name="teststd"/>
 <Module key="teststd.datapatterns" name="teststd.DataPatterns">
  <Attribute key="testdatapatterns"
             name="TestDataPatterns"
             sourcePath="/home/gavin/dev/hpcc/ecllibrary/teststd/DataPatterns/TestDataPatterns.ecl"
             ts="1584547639000000">
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
                &apos;Dan&apos;, &apos;Steve&apos;, &apos;&apos;, &apos;Mike&apos;, &apos;Dan&apos;, &apos;Sebastian&apos;, &apos;Dan&apos;
            ],
            {STRING s}
        );

    SHARED Basic_String_Profile := Std.DataPatterns.Profile(NOFOLD(Basic_String));

    EXPORT Test_Basic_String_Profile :=
        [
            ASSERT(Basic_String_Profile[1].attribute = &apos;s&apos;),
            ASSERT(Basic_String_Profile[1].rec_count = 7),
            ASSERT(Basic_String_Profile[1].given_attribute_type = &apos;string&apos;),
            ASSERT((DECIMAL9_6)Basic_String_Profile[1].fill_rate = (DECIMAL9_6)85.714286),
            ASSERT(Basic_String_Profile[1].fill_count = 6),
            ASSERT(Basic_String_Profile[1].cardinality = 4),
            ASSERT(Basic_String_Profile[1].best_attribute_type = &apos;string9&apos;),
            ASSERT(COUNT(Basic_String_Profile[1].modes) = 1),
            ASSERT(Basic_String_Profile[1].modes[1].value = &apos;Dan&apos;),
            ASSERT(Basic_String_Profile[1].modes[1].rec_count = 3),
            ASSERT(Basic_String_Profile[1].min_length = 3),
            ASSERT(Basic_String_Profile[1].max_length = 9),
            ASSERT(Basic_String_Profile[1].ave_length = 4),
            ASSERT(COUNT(Basic_String_Profile[1].popular_patterns) = 4),
            ASSERT(Basic_String_Profile[1].popular_patterns[1].data_pattern = &apos;Aaa&apos;),
            ASSERT(Basic_String_Profile[1].popular_patterns[1].rec_count = 3),
            ASSERT(Basic_String_Profile[1].popular_patterns[2].data_pattern = &apos;Aaaa&apos;),
            ASSERT(Basic_String_Profile[1].popular_patterns[2].rec_count = 1),
            ASSERT(Basic_String_Profile[1].popular_patterns[3].data_pattern = &apos;Aaaaa&apos;),
            ASSERT(Basic_String_Profile[1].popular_patterns[3].rec_count = 1),
            ASSERT(Basic_String_Profile[1].popular_patterns[4].data_pattern = &apos;Aaaaaaaaa&apos;),
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
            ASSERT(Basic_Numeric_Profile[1].attribute = &apos;n&apos;),
            ASSERT(Basic_Numeric_Profile[1].rec_count = 7),
            ASSERT(Basic_Numeric_Profile[1].given_attribute_type = &apos;integer8&apos;),
            ASSERT(Basic_Numeric_Profile[1].fill_rate = 100),
            ASSERT(Basic_Numeric_Profile[1].fill_count = 7),
            ASSERT(Basic_Numeric_Profile[1].cardinality = 6),
            ASSERT(Basic_Numeric_Profile[1].best_attribute_type = &apos;integer8&apos;),
            ASSERT(COUNT(Basic_Numeric_Profile[1].modes) = 1),
            ASSERT(Basic_Numeric_Profile[1].modes[1].value = &apos;2000&apos;),
            ASSERT(Basic_Numeric_Profile[1].modes[1].rec_count = 2),
            ASSERT(Basic_Numeric_Profile[1].min_length = 3),
            ASSERT(Basic_Numeric_Profile[1].max_length = 5),
            ASSERT(Basic_Numeric_Profile[1].ave_length = 4),
            ASSERT(COUNT(Basic_Numeric_Profile[1].popular_patterns) = 4),
            ASSERT(Basic_Numeric_Profile[1].popular_patterns[1].data_pattern = &apos;9999&apos;),
            ASSERT(Basic_Numeric_Profile[1].popular_patterns[1].rec_count = 3),
            ASSERT(Basic_Numeric_Profile[1].popular_patterns[2].data_pattern = &apos;-9999&apos;),
            ASSERT(Basic_Numeric_Profile[1].popular_patterns[2].rec_count = 2),
            ASSERT(Basic_Numeric_Profile[1].popular_patterns[3].data_pattern = &apos;-999&apos;),
            ASSERT(Basic_Numeric_Profile[1].popular_patterns[3].rec_count = 1),
            ASSERT(Basic_Numeric_Profile[1].popular_patterns[4].data_pattern = &apos;999&apos;),
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

    SHARED Empty_Data_Profile := Std.DataPatterns.Profile(NOFOLD(Empty_Data), features := &apos;cardinality,best_ecl_types,lengths,modes,patterns&apos;);

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
            TestEmptyAttr(&apos;f_ascii_string&apos;),
            TestEmptyAttr(&apos;f_ascii_string16&apos;),
            TestEmptyAttr(&apos;f_big_endian_integer&apos;),
            TestEmptyAttr(&apos;f_big_endian_unsigned&apos;),
            TestEmptyAttr(&apos;f_big_endian_unsigned_integer&apos;),
            TestEmptyAttr(&apos;f_data&apos;),
            TestEmptyAttr(&apos;f_decimal32&apos;),
            TestEmptyAttr(&apos;f_decimal32_6&apos;),
            TestEmptyAttr(&apos;f_ebcdic_string&apos;),
            TestEmptyAttr(&apos;f_ebcdic_string16&apos;),
            TestEmptyAttr(&apos;f_integer&apos;),
            TestEmptyAttr(&apos;f_little_endian_integer&apos;),
            TestEmptyAttr(&apos;f_little_endian_unsigned&apos;),
            TestEmptyAttr(&apos;f_little_endian_unsigned_integer&apos;),
            TestEmptyAttr(&apos;f_qstring&apos;),
            TestEmptyAttr(&apos;f_qstring16&apos;),
            TestEmptyAttr(&apos;f_real&apos;),
            TestEmptyAttr(&apos;f_string&apos;),
            TestEmptyAttr(&apos;f_string16&apos;),
            TestEmptyAttr(&apos;f_udecimal32&apos;),
            TestEmptyAttr(&apos;f_udecimal32_6&apos;),
            TestEmptyAttr(&apos;f_unicode&apos;),
            TestEmptyAttr(&apos;f_unicode16&apos;),
            TestEmptyAttr(&apos;f_unicode_de&apos;),
            TestEmptyAttr(&apos;f_unicode_de16&apos;),
            TestEmptyAttr(&apos;f_unsigned&apos;),
            TestEmptyAttr(&apos;f_unsigned_decimal32&apos;),
            TestEmptyAttr(&apos;f_unsigned_decimal32_6&apos;),
            TestEmptyAttr(&apos;f_unsigned_integer&apos;),
            TestEmptyAttr(&apos;f_utf8&apos;),
            TestEmptyAttr(&apos;f_utf8_de&apos;),
            TestEmptyAttr(&apos;f_varstring&apos;),
            TestEmptyAttr(&apos;f_varstring16&apos;),
            TestEmptyAttr(&apos;f_varunicode&apos;),
            TestEmptyAttr(&apos;f_varunicode16&apos;),
            TestEmptyAttr(&apos;f_varunicode_de&apos;),
            TestEmptyAttr(&apos;f_varunicode_de16&apos;),

            // Handle BOOLEAN special because it is not truly empty
            ASSERT(ValueForAttr(Empty_Data_Profile, &apos;f_boolean&apos;, cardinality) = 1),
            ASSERT(ValueForAttr(Empty_Data_Profile, &apos;f_boolean&apos;, best_attribute_type) = ValueForAttr(Empty_Data_Profile, &apos;f_boolean&apos;, given_attribute_type)),
            ASSERT(ValueForAttr(Empty_Data_Profile, &apos;f_boolean&apos;, min_length) = 1),
            ASSERT(ValueForAttr(Empty_Data_Profile, &apos;f_boolean&apos;, max_length) = 1),
            ASSERT(ValueForAttr(Empty_Data_Profile, &apos;f_boolean&apos;, ave_length) = 1),
            ASSERT(COUNT(ValueForAttr(Empty_Data_Profile, &apos;f_boolean&apos;, popular_patterns)) = 1),
            ASSERT(COUNT(ValueForAttr(Empty_Data_Profile, &apos;f_boolean&apos;, rare_patterns)) = 0),

            // Handle fixed-length DATA special because it is not truly empty
            ASSERT(ValueForAttr(Empty_Data_Profile, &apos;f_data16&apos;, cardinality) = 1),
            ASSERT(ValueForAttr(Empty_Data_Profile, &apos;f_data16&apos;, best_attribute_type) = ValueForAttr(Empty_Data_Profile, &apos;f_data16&apos;, given_attribute_type)),
            ASSERT(ValueForAttr(Empty_Data_Profile, &apos;f_data16&apos;, min_length) = 16),
            ASSERT(ValueForAttr(Empty_Data_Profile, &apos;f_data16&apos;, max_length) = 16),
            ASSERT(ValueForAttr(Empty_Data_Profile, &apos;f_data16&apos;, ave_length) = 16),
            ASSERT(COUNT(ValueForAttr(Empty_Data_Profile, &apos;f_data16&apos;, popular_patterns)) = 1),
            ASSERT(COUNT(ValueForAttr(Empty_Data_Profile, &apos;f_data16&apos;, rare_patterns)) = 0),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Unicode pattern detection
    //--------------------------------------------------------------------------

    SHARED Pattern_Unicode := DATASET
        (
            [
                U&apos;abcd\353&apos;, U&apos;ABCDË&apos;
            ],
            {UNICODE_de5 s}
        );

    SHARED Pattern_Unicode_Profile := Std.DataPatterns.Profile(NOFOLD(Pattern_Unicode), features := &apos;patterns&apos;);

    EXPORT Test_Pattern_Unicode_Profile :=
        [
            ASSERT(Pattern_Unicode_Profile[1].given_attribute_type = &apos;unicode_de5&apos;),
            ASSERT(COUNT(Pattern_Unicode_Profile[1].popular_patterns) = 2),
            ASSERT(Pattern_Unicode_Profile[1].popular_patterns[1].data_pattern = &apos;AAAAA&apos;),
            ASSERT(Pattern_Unicode_Profile[1].popular_patterns[1].rec_count = 1),
            ASSERT(Pattern_Unicode_Profile[1].popular_patterns[2].data_pattern = &apos;aaaaa&apos;),
            ASSERT(Pattern_Unicode_Profile[1].popular_patterns[2].rec_count = 1),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Punctuation pattern test
    //--------------------------------------------------------------------------

    SHARED Pattern_Punctuation := DATASET
        (
            [
                &apos;This! Is- Not. Helpful?&apos;
            ],
            {STRING s}
        );

    SHARED Pattern_Punctuation_Profile := Std.DataPatterns.Profile(NOFOLD(Pattern_Punctuation), features := &apos;patterns&apos;);

    EXPORT Test_Pattern_Punctuation_Profile :=
        [
            ASSERT(Pattern_Punctuation_Profile[1].attribute = &apos;s&apos;),
            ASSERT(COUNT(Pattern_Punctuation_Profile[1].popular_patterns) = 1),
            ASSERT(Pattern_Punctuation_Profile[1].popular_patterns[1].data_pattern = &apos;Aaaa! Aa- Aaa. Aaaaaaa?&apos;),
            ASSERT(Pattern_Punctuation_Profile[1].popular_patterns[1].rec_count = 1),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Finding integers within strings test
    //--------------------------------------------------------------------------

    SHARED Best_Integer := DATASET
        (
            [
                {&apos;-100&apos;, &apos;-100&apos;, &apos;-1000&apos;, &apos;-10000&apos;, &apos;-100000&apos;},
                {&apos;100&apos;, &apos;100&apos;, &apos;1000&apos;, &apos;10000&apos;, &apos;100000&apos;}
            ],
            {STRING s1, STRING s2, STRING s3, STRING s4, STRING s5}
        );

    SHARED Best_Integer_Profile := Std.DataPatterns.Profile(NOFOLD(Best_Integer), features := &apos;best_ecl_types&apos;);

    EXPORT Test_Best_Integer_Profile :=
        [
            ASSERT(ValueForAttr(Best_Integer_Profile, &apos;s1&apos;, best_attribute_type) = &apos;integer2&apos;),
            ASSERT(ValueForAttr(Best_Integer_Profile, &apos;s2&apos;, best_attribute_type) = &apos;integer2&apos;),
            ASSERT(ValueForAttr(Best_Integer_Profile, &apos;s3&apos;, best_attribute_type) = &apos;integer3&apos;),
            ASSERT(ValueForAttr(Best_Integer_Profile, &apos;s4&apos;, best_attribute_type) = &apos;integer3&apos;),
            ASSERT(ValueForAttr(Best_Integer_Profile, &apos;s5&apos;, best_attribute_type) = &apos;integer4&apos;),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Finding unsigned integers within strings test
    //--------------------------------------------------------------------------

    SHARED Best_Unsigned := DATASET
        (
            [
                {&apos;100&apos;, &apos;100&apos;, &apos;1000&apos;, &apos;10000&apos;, &apos;100000&apos;}
            ],
            {STRING s1, STRING s2, STRING s3, STRING s4, STRING s5}
        );

    SHARED Best_Unsigned_Profile := Std.DataPatterns.Profile(NOFOLD(Best_Unsigned), features := &apos;best_ecl_types&apos;);

    EXPORT Test_Best_Unsigned_Profile :=
        [
            ASSERT(ValueForAttr(Best_Unsigned_Profile, &apos;s1&apos;, best_attribute_type) = &apos;unsigned2&apos;),
            ASSERT(ValueForAttr(Best_Unsigned_Profile, &apos;s2&apos;, best_attribute_type) = &apos;unsigned2&apos;),
            ASSERT(ValueForAttr(Best_Unsigned_Profile, &apos;s3&apos;, best_attribute_type) = &apos;unsigned2&apos;),
            ASSERT(ValueForAttr(Best_Unsigned_Profile, &apos;s4&apos;, best_attribute_type) = &apos;unsigned3&apos;),
            ASSERT(ValueForAttr(Best_Unsigned_Profile, &apos;s5&apos;, best_attribute_type) = &apos;unsigned3&apos;),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Finding reals within strings test
    //--------------------------------------------------------------------------

    SHARED Best_Real := DATASET
        (
            [
                {&apos;99.99&apos;, &apos;-99.99&apos;, &apos;9.1234e-10&apos;, &apos;.123&apos;, &apos;99.0&apos;}
            ],
            {STRING s1, STRING s2, STRING s3, STRING s4, STRING s5}
        );

    SHARED Best_Real_Profile := Std.DataPatterns.Profile(NOFOLD(Best_Real), features := &apos;best_ecl_types&apos;);

    EXPORT Test_Best_Real_Profile :=
        [
            ASSERT(ValueForAttr(Best_Real_Profile, &apos;s1&apos;, best_attribute_type) = &apos;real4&apos;),
            ASSERT(ValueForAttr(Best_Real_Profile, &apos;s2&apos;, best_attribute_type) = &apos;real4&apos;),
            ASSERT(ValueForAttr(Best_Real_Profile, &apos;s3&apos;, best_attribute_type) = &apos;real8&apos;),
            ASSERT(ValueForAttr(Best_Real_Profile, &apos;s4&apos;, best_attribute_type) = &apos;real4&apos;),
            ASSERT(ValueForAttr(Best_Real_Profile, &apos;s5&apos;, best_attribute_type) = &apos;real4&apos;),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Not actually numbers test
    //--------------------------------------------------------------------------

    SHARED Best_NaN := DATASET
        (
            [
                {&apos;123456789012345678901&apos;, &apos;-12345678901234567890&apos;, &apos;9.1234e-1000&apos;, &apos;99.1234567890123456&apos;, &apos;123456789012345678901.0&apos;}
            ],
            {STRING s1, STRING s2, STRING s3, STRING s4, STRING s5}
        );

    SHARED Best_NaN_Profile := Std.DataPatterns.Profile(NOFOLD(Best_NaN), features := &apos;best_ecl_types&apos;);

    EXPORT Test_Best_NaN_Profile :=
        [
            ASSERT(ValueForAttr(Best_NaN_Profile, &apos;s1&apos;, best_attribute_type) = &apos;string21&apos;),
            ASSERT(ValueForAttr(Best_NaN_Profile, &apos;s2&apos;, best_attribute_type) = &apos;string21&apos;),
            ASSERT(ValueForAttr(Best_NaN_Profile, &apos;s3&apos;, best_attribute_type) = &apos;string12&apos;),
            ASSERT(ValueForAttr(Best_NaN_Profile, &apos;s4&apos;, best_attribute_type) = &apos;string19&apos;),
            ASSERT(ValueForAttr(Best_NaN_Profile, &apos;s5&apos;, best_attribute_type) = &apos;string23&apos;),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Embedded child record test
    //--------------------------------------------------------------------------

    SHARED Embedded_Child1 := DATASET
        (
            [
                {&apos;Dan&apos;, {123, 345, 567}},
                {&apos;Mike&apos;, {987, 765, 543}}
            ],
            {STRING s, {UNSIGNED4 x, UNSIGNED4 y, UNSIGNED4 z} foo}
        );

    SHARED Embedded_Child1_Profile := Std.DataPatterns.Profile(NOFOLD(Embedded_Child1));

    EXPORT Test_Embedded_Child1_Profile :=
        [
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].attribute = &apos;foo.x&apos;),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].rec_count = 2),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].given_attribute_type = &apos;unsigned4&apos;),
            ASSERT((DECIMAL9_6)Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].fill_rate = (DECIMAL9_6)100),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].fill_count = 2),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].cardinality = 2),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].best_attribute_type = &apos;unsigned4&apos;),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].min_length = 3),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].max_length = 3),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].ave_length = 3),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].popular_patterns) = 1),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].rare_patterns) = 0),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].is_numeric = TRUE),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].numeric_min = 123),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].numeric_max = 987),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].numeric_mean = 555),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].numeric_std_dev = 432),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].numeric_lower_quartile = 123),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].numeric_median = 555),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].numeric_upper_quartile = 0),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = &apos;foo.x&apos;)[1].correlations) = 2),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].attribute = &apos;foo.y&apos;),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].rec_count = 2),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].given_attribute_type = &apos;unsigned4&apos;),
            ASSERT((DECIMAL9_6)Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].fill_rate = (DECIMAL9_6)100),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].fill_count = 2),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].cardinality = 2),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].best_attribute_type = &apos;unsigned4&apos;),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].min_length = 3),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].max_length = 3),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].ave_length = 3),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].popular_patterns) = 1),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].rare_patterns) = 0),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].is_numeric = TRUE),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].numeric_min = 345),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].numeric_max = 765),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].numeric_mean = 555),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].numeric_std_dev = 210),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].numeric_lower_quartile = 345),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].numeric_median = 555),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].numeric_upper_quartile = 0),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = &apos;foo.y&apos;)[1].correlations) = 2),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].attribute = &apos;foo.z&apos;),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].rec_count = 2),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].given_attribute_type = &apos;unsigned4&apos;),
            ASSERT((DECIMAL9_6)Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].fill_rate = (DECIMAL9_6)100),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].fill_count = 2),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].cardinality = 2),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].best_attribute_type = &apos;unsigned4&apos;),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].min_length = 3),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].max_length = 3),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].ave_length = 3),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].popular_patterns) = 1),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].rare_patterns) = 0),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].is_numeric = TRUE),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].numeric_min = 543),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].numeric_max = 567),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].numeric_mean = 555),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].numeric_std_dev = 12),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].numeric_lower_quartile = 543),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].numeric_median = 555),
            ASSERT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].numeric_upper_quartile = 0),
            ASSERT(COUNT(Embedded_Child1_Profile(attribute = &apos;foo.z&apos;)[1].correlations) = 2),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // Test strings fields containing numerics with leading zeros (issue 42)
    //--------------------------------------------------------------------------

    SHARED Leading_Zeros := DATASET
        (
            [
                {&apos;0100&apos;, &apos;1234&apos;, &apos;0001&apos;, &apos;7809&apos;, &apos;-0600&apos;},
                {&apos;0020&apos;, &apos;0001&apos;, &apos;0023&apos;, &apos;0001&apos;, &apos;600&apos;}
            ],
            {STRING s1, STRING s2, STRING s3, STRING s4, STRING s5}
        );

    SHARED Leading_Zeros_Profile := Std.DataPatterns.Profile(NOFOLD(Leading_Zeros), features := &apos;best_ecl_types&apos;);

    EXPORT Test_Leading_Zeros_Profile :=
        [
            ASSERT(ValueForAttr(Leading_Zeros_Profile, &apos;s1&apos;, best_attribute_type) = &apos;string4&apos;),
            ASSERT(ValueForAttr(Leading_Zeros_Profile, &apos;s2&apos;, best_attribute_type) = &apos;string4&apos;),
            ASSERT(ValueForAttr(Leading_Zeros_Profile, &apos;s3&apos;, best_attribute_type) = &apos;string4&apos;),
            ASSERT(ValueForAttr(Leading_Zeros_Profile, &apos;s4&apos;, best_attribute_type) = &apos;string4&apos;),
            ASSERT(ValueForAttr(Leading_Zeros_Profile, &apos;s5&apos;, best_attribute_type) = &apos;integer3&apos;),
            ASSERT(TRUE)
        ];

    //--------------------------------------------------------------------------
    // String fields with wildly varying lengths (three orders of magnitude
    // difference) should become variable-length &apos;string&apos; datatypes
    //--------------------------------------------------------------------------

    SHARED STRING MLS(UNSIGNED4 len) := EMBED(C++)
        const char  letters[] = &quot;abcdefghijklmnopqrstuvwxyz0123456789&quot;;

        __lenResult = len;
        __result = static_cast&lt;char*&gt;(rtlMalloc(__lenResult));

        for (uint32_t x = 0; x &lt; len; x++)
            __result[x] = letters[rand() % 36];
    ENDEMBED;

    SHARED Large_Strings := DATASET
        (
            [
                {&apos;abcd&apos;, &apos;1234&apos;, &apos;0001&apos;, &apos;7&apos;, &apos;-0600&apos;},
                {&apos;0020&apos;, MLS(5000), MLS(500), MLS(1050), &apos;600&apos;}
            ],
            {STRING s1, STRING s2, STRING s3, STRING s4, STRING s5}
        );

    SHARED Large_Strings_Profile := Std.DataPatterns.Profile(NOFOLD(Large_Strings), features := &apos;best_ecl_types&apos;);

    EXPORT Test_Large_Strings_Profile :=
        [
            ASSERT(ValueForAttr(Large_Strings_Profile, &apos;s1&apos;, best_attribute_type) = &apos;string4&apos;),
            ASSERT(ValueForAttr(Large_Strings_Profile, &apos;s2&apos;, best_attribute_type) = &apos;string&apos;),
            ASSERT(ValueForAttr(Large_Strings_Profile, &apos;s3&apos;, best_attribute_type) = &apos;string500&apos;),
            ASSERT(ValueForAttr(Large_Strings_Profile, &apos;s4&apos;, best_attribute_type) = &apos;string&apos;),
            ASSERT(ValueForAttr(Large_Strings_Profile, &apos;s5&apos;, best_attribute_type) = &apos;integer3&apos;),
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
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].attribute = &apos;my_set&apos;),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].rec_count = 5),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].given_attribute_type = &apos;set of unsigned2&apos;),
            ASSERT((DECIMAL9_6)SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].fill_rate = (DECIMAL9_6)80),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].fill_count = 4),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].cardinality = 4),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].best_attribute_type = &apos;set of unsigned2&apos;),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].min_length = 2),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].max_length = 11),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].ave_length = 4),
            ASSERT(COUNT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].popular_patterns) = 3),
            ASSERT(COUNT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].rare_patterns) = 0),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].is_numeric = FALSE),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].numeric_min = 0),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].numeric_max = 0),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].numeric_mean = 0),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].numeric_std_dev = 0),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].numeric_lower_quartile = 0),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].numeric_median = 0),
            ASSERT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].numeric_upper_quartile = 0),
            ASSERT(COUNT(SetOf_Types_Profile(attribute = &apos;my_set&apos;)[1].correlations) = 0),
            ASSERT(TRUE)
        ];

    EXPORT Main := [
//        EVALUATE(Test_Basic_String_Profile),
//        EVALUATE(Test_Basic_Numeric_Profile),
//        EVALUATE(Test_Empty_Data_Profile),
        EVALUATE(Test_Pattern_Unicode_Profile)
//        EVALUATE(Test_Pattern_Punctuation_Profile),
//        EVALUATE(Test_Best_Integer_Profile),
//        EVALUATE(Test_Best_Unsigned_Profile),
//        EVALUATE(Test_Best_Real_Profile),
//        EVALUATE(Test_Best_NaN_Profile),
//        EVALUATE(Test_Embedded_Child1_Profile),
//        EVALUATE(Test_Leading_Zeros_Profile),
//        EVALUATE(Test_Large_Strings_Profile),
//        EVALUATE(Test_SetOf_Types_Profile)
    ];
END;&#10;
  </Attribute>
 </Module>
 <Module key="std" name="std">
  <Attribute key="str"
             name="Str"
             sourcePath="/home/gavin/dev/hpcc/ecllibrary/std/Str.ecl"
             ts="1576576211000000">
   /*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */


externals :=
    SERVICE : fold
STRING EncodeBase64(const data src) :   eclrtl,pure,include,library=&apos;eclrtl&apos;,entrypoint=&apos;rtlBase64Encode&apos;;
DATA DecodeBase64(const string src) :   eclrtl,pure,include,library=&apos;eclrtl&apos;,entrypoint=&apos;rtlBase64Decode&apos;;
    END;

EXPORT Str := MODULE


/*
  Since this is primarily a wrapper for a plugin, all the definitions for this standard library
  module are included in a single file.  Generally I would expect them in individual files.
  */

IMPORT lib_stringlib;

/**
 * Compares the two strings case insensitively.  Returns a negative integer, zero, or a positive integer according to
 * whether the first string is less than, equal to, or greater than the second.
 *
 * @param src1          The first string to be compared.
 * @param src2          The second string to be compared.
 * @see                 Str.EqualIgnoreCase
 */

EXPORT INTEGER4 CompareIgnoreCase(STRING src1, STRING src2) :=
  lib_stringlib.StringLib.StringCompareIgnoreCase(src1, src2);

/**
 * Tests whether the two strings are identical ignoring differences in case.
 *
 * @param src1          The first string to be compared.
 * @param src2          The second string to be compared.
 * @see                 Str.CompareIgnoreCase
 */

EXPORT BOOLEAN EqualIgnoreCase(STRING src1, STRING src2) := CompareIgnoreCase(src1, src2) = 0;

/**
 * Returns the character position of the nth match of the search string with the first string.
 * If no match is found the attribute returns 0.
 * If an instance is omitted the position of the first instance is returned.
 *
 * @param src           The string that is searched
 * @param sought        The string being sought.
 * @param instance      Which match instance are we interested in?
 */

EXPORT UNSIGNED4 Find(STRING src, STRING sought, UNSIGNED4 instance = 1) :=
  lib_stringlib.StringLib.StringFind(src, sought, instance);

/**
 * Returns the number of occurences of the second string within the first string.
 *
 * @param src           The string that is searched
 * @param sought        The string being sought.
 */

EXPORT UNSIGNED4 FindCount(STRING src, STRING sought) := lib_stringlib.StringLib.StringFindCount(src, sought);

/**
 * Tests if the search string matches the pattern.
 * The pattern can contain wildcards &apos;?&apos; (single character) and &apos;*&apos; (multiple character).
 *
 * @param src           The string that is being tested.
 * @param pattern       The pattern to match against.
 * @param ignore_case   Whether to ignore differences in case between characters
 */

EXPORT BOOLEAN WildMatch(STRING src, STRING _pattern, BOOLEAN ignore_case) :=
  lib_stringlib.StringLib.StringWildExactMatch(src, _pattern, ignore_case);

/**
 * Tests if the search string contains each of the characters in the pattern.
 * If the pattern contains duplicate characters those characters will match once for each occurence in the pattern.
 *
 * @param src           The string that is being tested.
 * @param pattern       The pattern to match against.
 * @param ignore_case   Whether to ignore differences in case between characters
 */

EXPORT BOOLEAN Contains(STRING src, STRING _pattern, BOOLEAN ignore_case) :=
  lib_stringlib.StringLib.StringContains(src, _pattern, ignore_case);

/**
 * Returns the first string with all characters within the second string removed.
 *
 * @param src           The string that is being tested.
 * @param filter        The string containing the set of characters to be excluded.
 * @see                 Str.Filter
 */

EXPORT STRING FilterOut(STRING src, STRING filter) := lib_stringlib.StringLib.StringFilterOut(src, filter);

/**
 * Returns the first string with all characters not within the second string removed.
 *
 * @param src           The string that is being tested.
 * @param filter        The string containing the set of characters to be included.
 * @see                 Str.FilterOut
 */

EXPORT STRING Filter(STRING src, STRING filter) := lib_stringlib.StringLib.StringFilter(src, filter);

/**
 * Returns the source string with the replacement character substituted for all characters included in the
 * filter string.
 * MORE: Should this be a general string substitution?
 *
 * @param src           The string that is being tested.
 * @param filter        The string containing the set of characters to be included.
 * @param replace_char  The character to be substituted into the result.
 * @see                 Std.Str.Translate, Std.Str.SubstituteExcluded
 */

EXPORT STRING SubstituteIncluded(STRING src, STRING filter, STRING1 replace_char) :=
  lib_stringlib.StringLib.StringSubstituteOut(src, filter, replace_char);

/**
 * Returns the source string with the replacement character substituted for all characters not included in the
 * filter string.
 * MORE: Should this be a general string substitution?
 *
 * @param src           The string that is being tested.
 * @param filter        The string containing the set of characters to be included.
 * @param replace_char  The character to be substituted into the result.
 * @see                 Std.Str.SubstituteIncluded
 */

EXPORT STRING SubstituteExcluded(STRING src, STRING filter, STRING1 replace_char) :=
  lib_stringlib.StringLib.StringSubstitute(src, filter, replace_char);

/**
 * Returns the source string with the all characters that match characters in the search string replaced
 * with the character at the corresponding position in the replacement string.
 *
 * @param src           The string that is being tested.
 * @param search        The string containing the set of characters to be included.
 * @param replacement   The string containing the characters to act as replacements.
 * @see                 Std.Str.SubstituteIncluded
 */

//MORE: Would be more efficient to create a mapping object, and pass that to the replacement function.
EXPORT STRING Translate(STRING src, STRING search, STRING replacement) :=
  lib_stringlib.StringLib.StringTranslate(src, search, replacement);

/**
 * Returns the argument string with all upper case characters converted to lower case.
 *
 * @param src           The string that is being converted.
 */

EXPORT STRING ToLowerCase(STRING src) := lib_stringlib.StringLib.StringToLowerCase(src);

/**
 * Return the argument string with all lower case characters converted to upper case.
 *
 * @param src           The string that is being converted.
 */

EXPORT STRING ToUpperCase(STRING src) := lib_stringlib.StringLib.StringToUpperCase(src);

/**
 * Returns the argument string with the first letter of each word in upper case and all other
 * letters left as-is.
 * A contiguous sequence of alphanumeric characters is treated as a word.
 *
 * @param src           The string that is being converted.
 */

EXPORT STRING ToCapitalCase(STRING src) := lib_stringlib.StringLib.StringToCapitalCase(src);

/**
 * Returns the argument string with the first letter of each word in upper case and all other
 * letters lower case.
 * A contiguous sequence of alphanumeric characters is treated as a word.
 *
 * @param src           The string that is being converted.
 */

EXPORT STRING ToTitleCase(STRING src) := lib_stringlib.StringLib.StringToTitleCase(src);

/**
 * Returns the argument string with all characters in reverse order.
 * Note the argument is not TRIMMED before it is reversed.
 *
 * @param src           The string that is being reversed.
 */

EXPORT STRING Reverse(STRING src) := lib_stringlib.StringLib.StringReverse(src);

/**
 * Returns the source string with the replacement string substituted for all instances of the search string.
 *
 * @param src           The string that is being transformed.
 * @param sought        The string to be replaced.
 * @param replacement   The string to be substituted into the result.
 */

EXPORT STRING FindReplace(STRING src, STRING sought, STRING replacement) :=
  lib_stringlib.StringLib.StringFindReplace(src, sought, replacement);

/**
 * Returns the nth element from a comma separated string.
 *
 * @param src           The string containing the comma separated list.
 * @param instance      Which item to select from the list.
 */

EXPORT STRING Extract(STRING src, UNSIGNED4 instance) := lib_stringlib.StringLib.StringExtract(src, instance);

/**
 * Returns the source string with all instances of multiple adjacent space characters (2 or more spaces together)
 * reduced to a single space character.  Leading and trailing spaces are removed, and tab characters are converted
 * to spaces.
 *
 * @param src           The string to be cleaned.
 */

EXPORT STRING CleanSpaces(STRING src) := lib_stringlib.StringLib.StringCleanSpaces(src);

/**
 * Returns true if the prefix string matches the leading characters in the source string.  Trailing spaces are
 * stripped from the prefix before matching.
 * // x.myString.StartsWith(&apos;x&apos;) as an alternative syntax would be even better
 *
 * @param src           The string being searched in.
 * @param prefix        The prefix to search for.
 */

EXPORT BOOLEAN StartsWith(STRING src, STRING prefix) := src[1..LENGTH(TRIM(prefix))]=prefix;

/**
 * Returns true if the suffix string matches the trailing characters in the source string.  Trailing spaces are
 * stripped from both strings before matching.
 *
 * @param src           The string being searched in.
 * @param suffix        The prefix to search for.
 */
EXPORT BOOLEAN EndsWith(STRING src, STRING suffix) := src[LENGTH(TRIM(src))-LENGTH(TRIM(suffix))+1..]=suffix;


/**
 * Removes the suffix from the search string, if present, and returns the result.  Trailing spaces are
 * stripped from both strings before matching.
 *
 * @param src           The string being searched in.
 * @param suffix        The prefix to search for.
 */
EXPORT STRING RemoveSuffix(STRING src, STRING suffix) :=
            IF(EndsWith(src, suffix), src[1..length(trim(src))-length(trim(suffix))], src);


/**
 * Returns a string containing a list of elements from a comma separated string.
 *
 * @param src           The string containing the comma separated list.
 * @param mask          A bitmask of which elements should be included.  Bit 0 is item1, bit1 item 2 etc.
 */

EXPORT STRING ExtractMultiple(STRING src, UNSIGNED8 mask) := lib_stringlib.StringLib.StringExtractMultiple(src, mask);

/**
 * Returns the number of words that the string contains.  Words are separated by one or more separator strings. No
 * spaces are stripped from either string before matching.
 *
 * @param src           The string being searched in.
 * @param separator     The string used to separate words
 * @param allow_blank   Indicates if empty/blank string items are included in the results.
 */

EXPORT UNSIGNED4 CountWords(STRING src, STRING separator, BOOLEAN allow_blank = FALSE) := lib_stringlib.StringLib.CountWords(src, separator, allow_blank);

/**
 * Returns the list of words extracted from the string.  Words are separated by one or more separator strings. No
 * spaces are stripped from either string before matching.
 *
 * @param src           The string being searched in.
 * @param separator     The string used to separate words
 * @param allow_blank   Indicates if empty/blank string items are included in the results.
 */

EXPORT SET OF STRING SplitWords(STRING src, STRING separator, BOOLEAN allow_blank = FALSE) := lib_stringlib.StringLib.SplitWords(src, separator, allow_blank);


/**
 * Returns the list of words extracted from the string.  Words are separated by one or more separator strings. No
 * spaces are stripped from either string before matching.
 *
 * @param words         The set of strings to be combined.
 * @param separator     The string used to separate words.
 */

EXPORT STRING CombineWords(SET OF STRING words, STRING separator) := lib_stringlib.StringLib.CombineWords(words, separator);


/**
 * Returns the minimum edit distance between the two strings.  An insert change or delete counts as a single edit.
 * The two strings are trimmed before comparing.
 *
 * @param _left         The first string to be compared.
 * @param _right        The second string to be compared.
 * @param radius        The maximum edit distance that is acceptable, or 0 for no limit.  Defaults to 0.
 * @return              The minimum edit distance between the two strings.  Edit distances above radius will
                        return an arbitrary value larger than radius.
 */

EXPORT UNSIGNED4 EditDistance(STRING _left, STRING _right, UNSIGNED4 radius = 0) :=
    lib_stringlib.StringLib.EditDistanceV3(_left, _right, radius);

/**
 * Returns true if the minimum edit distance between the two strings is with a specific range.
 * The two strings are trimmed before comparing.
 *
 * @param _left         The first string to be compared.
 * @param _right        The second string to be compared.
 * @param radius        The maximum edit distance that is acceptable.
 * @return              Whether or not the two strings are within the given specified edit distance.
 */

EXPORT BOOLEAN EditDistanceWithinRadius(STRING _left, STRING _right, UNSIGNED4 radius) :=
    lib_stringlib.StringLib.EditDistanceWithinRadiusV2(_left, _right, radius);


/**
 * Returns the number of words in the string.  Words are separated by one or more spaces.
 *
 * @param text          The string to be broken into words.
 * @return              The number of words in the string.
 */

EXPORT UNSIGNED4 WordCount(STRING text) :=
    lib_stringlib.StringLib.StringWordCount(text);

/**
 * Returns the n-th word from the string.  Words are separated by one or more spaces.
 *
 * @param text          The string to be broken into words.
 * @param n             Which word should be returned from the function.
 * @return              The number of words in the string.
 */

EXPORT STRING GetNthWord(STRING text, UNSIGNED4 n) :=
    lib_stringlib.StringLib.StringGetNthWord(text, n);

/**
 * Returns everything except the first word from the string.  Words are separated by one or more whitespace characters.
 * Whitespace before and after the first word is also removed.
 *
 * @param text          The string to be broken into words.
 * @return              The string excluding the first word.
 */

EXPORT ExcludeFirstWord(STRING text) := lib_stringlib.Stringlib.StringExcludeNthWord(text, 1);

/**
 * Returns everything except the last word from the string.  Words are separated by one or more whitespace characters.
 * Whitespace after a word is removed with the word and leading whitespace is removed with the first word.
 *
 * @param text          The string to be broken into words.
 * @return              The string excluding the last word.
 */

EXPORT ExcludeLastWord(STRING text) := lib_stringlib.Stringlib.StringExcludeLastWord(text);

/**
 * Returns everything except the nth word from the string.  Words are separated by one or more whitespace characters.
 * Whitespace after a word is removed with the word and leading whitespace is removed with the first word.
 *
 * @param text          The string to be broken into words.
 * @param n             Which word should be returned from the function.
 * @return              The string excluding the nth word.
 */

EXPORT ExcludeNthWord(STRING text, UNSIGNED2 n) := lib_stringlib.Stringlib.StringExcludeNthWord(text, n);

/**
 * Tests if the search string contains the supplied word as a whole word.
 *
 * @param src           The string that is being tested.
 * @param word          The word to be searched for.
 * @param ignore_case   Whether to ignore differences in case between characters.
 */

EXPORT BOOLEAN FindWord(STRING src, STRING word, BOOLEAN ignore_case=FALSE) := FUNCTION
   return IF (ignore_case,
              REGEXFIND(&apos;\\b&apos;+word+&apos;\\b&apos;, src, NOCASE),
              REGEXFIND(&apos;\\b&apos;+word+&apos;\\b&apos;, src));
END;

/*
 * Returns a string containing text repeated n times.
 *
 * @param text          The string to be repeated.
 * @param n             Number of repetitions.
 * @return              A string containing n concatenations of the string text.
 */

EXPORT STRING Repeat(STRING text, UNSIGNED4 n) := lib_stringlib.Stringlib.StringRepeat(text, n);

/*
 * Converts the data value to a sequence of hex pairs.
 *
 * @param value         The data value that should be expanded as a sequence of hex pairs.
 * @return              A string containing a sequence of hex pairs.
 */

EXPORT STRING ToHexPairs(DATA value) := lib_stringlib.StringLib.Data2String(value);

/*
 * Converts a string containing sequences of hex pairs to a data value.
 *
 * Embedded spaces are ignored, out of range characters are treated as &apos;0&apos;, a trailing nibble
 * at the end of the string is ignored.
 *
 *
 * @param hex_pairs     The string containing the hex pairs to process.
 * @return              A data value with each byte created from a pair of hex digits.
 */

EXPORT DATA FromHexPairs(STRING hex_pairs) := lib_stringlib.StringLib.String2Data(hex_pairs);

/*
 * Encode binary data to base64 string.
 *
 * Every 3 data bytes are encoded to 4 base64 characters. If the length of the input is not divisible
 * by 3, up to 2 &apos;=&apos; characters are appended to the output.
 *
 *
 * @param value         The binary data array to process.
 * @return              Base 64 encoded string.
 */

EXPORT STRING EncodeBase64(DATA value) := externals.EncodeBase64(value);

/*
 * Decode base64 encoded string to binary data.
 *
 * If the input is not valid base64 encoding (invalid characters, or ends mid-quartet), an empty
 * result is returned. Whitespace in the input is skipped.
 *
 *
 * @param value        The base 64 encoded string.
 * @return             Decoded binary data if the input is valid else zero length data.
 */

EXPORT DATA DecodeBase64(STRING value) := externals.DecodeBase64(value);

END;&#10;
  </Attribute>
 </Module>
 <Module key="std.datapatterns" name="std.DataPatterns">
  <Attribute key="profile"
             name="Profile"
             sourcePath="/home/gavin/dev/hpcc/ecllibrary/std/DataPatterns/Profile.ecl"
             ts="1584541394000000">
   /***
 * Function macro for profiling all or part of a dataset.  The output is a
 * dataset containing the following information for each profiled attribute:
 *
 *      attribute               The name of the attribute
 *      given_attribute_type    The ECL type of the attribute as it was defined
 *                              in the input dataset
 *      best_attribute_type     An ECL data type that both allows all values
 *                              in the input dataset and consumes the least
 *                              amount of memory
 *      rec_count               The number of records analyzed in the dataset;
 *                              this may be fewer than the total number of
 *                              records, if the optional sampleSize argument
 *                              was provided with a value less than 100
 *      fill_count              The number of rec_count records containing
 *                              non-nil values; a &apos;nil value&apos; is an empty
 *                              string, a numeric zero, or an empty SET; note
 *                              that BOOLEAN attributes are always counted as
 *                              filled, regardless of their value; also,
 *                              fixed-length DATA attributes (e.g. DATA10) are
 *                              also counted as filled, given their typical
 *                              function of holding data blobs
 *      fill_rate               The percentage of rec_count records containing
 *                              non-nil values; this is basically
 *                              fill_count / rec_count * 100
 *      cardinality             The number of unique, non-nil values within
 *                              the attribute
 *      cardinality_breakdown   For those attributes with a low number of
 *                              unique, non-nil values, show each value and the
 *                              number of records containing that value; the
 *                              lcbLimit parameter governs what &quot;low number&quot;
 *                              means
 *      modes                   The most common values in the attribute, after
 *                              coercing all values to STRING, along with the
 *                              number of records in which the values were
 *                              found; if no value is repeated more than once
 *                              then no mode will be shown; up to five (5)
 *                              modes will be shown; note that string values
 *                              longer than the maxPatternLen argument will
 *                              be truncated
 *      min_length              For SET datatypes, the fewest number of elements
 *                              found in the set; for other data types, the
 *                              shortest length of a value when expressed
 *                              as a string; null values are ignored
 *      max_length              For SET datatypes, the largest number of elements
 *                              found in the set; for other data types, the
 *                              longest length of a value when expressed
 *                              as a string; null values are ignored
 *      ave_length              For SET datatypes, the average number of elements
 *                              found in the set; for other data types, the
 *                              average length of a value when expressed
 *                              as a string; null values are ignored
 *      popular_patterns        The most common patterns of values; see below
 *      rare_patterns           The least common patterns of values; see below
 *      is_numeric              Boolean indicating if the original attribute
 *                              was a numeric scalar or if the best_attribute_type
 *                              value was a numeric scaler; if TRUE then the
 *                              numeric_xxxx output fields will be
 *                              populated with actual values; if this value
 *                              is FALSE then all numeric_xxxx output values
 *                              should be ignored
 *      numeric_min             The smallest non-nil value found within the
 *                              attribute as a DECIMAL; this value is valid only
 *                              if is_numeric is TRUE; if is_numeric is FALSE
 *                              then zero will show here
 *      numeric_max             The largest non-nil value found within the
 *                              attribute as a DECIMAL;this value is valid only
 *                              if is_numeric is TRUE; if is_numeric is FALSE
 *                              then zero will show here
 *      numeric_mean            The mean (average) non-nil value found within
 *                              the attribute as a DECIMAL; this value is valid
 *                              only if is_numeric is TRUE; if is_numeric is FALSE
 *                              then zero will show here
 *      numeric_std_dev         The standard deviation of the non-nil values
 *                              in the attribute as a DECIMAL; this value is valid
 *                              only if is_numeric is TRUE; if is_numeric is FALSE
 *                              then zero will show here
 *      numeric_lower_quartile  The value separating the first (bottom) and
 *                              second quarters of non-nil values within
 *                              the attribute as a DECIMAL; this value is valid only
 *                              if is_numeric is TRUE; if is_numeric is FALSE
 *                              then zero will show here
 *      numeric_median          The median non-nil value within the attribute
 *                              as a DECIMAL; this value is valid only
 *                              if is_numeric is TRUE; if is_numeric is FALSE
 *                              then zero will show here
 *      numeric_upper_quartile  The value separating the third and fourth
 *                              (top) quarters of non-nil values within
 *                              the attribute as a DECIMAL; this value is valid only
 *                              if is_numeric is TRUE; if is_numeric is FALSE
 *                              then zero will show here
 *      correlations            A child dataset containing correlation values
 *                              comparing the current numeric attribute with all
 *                              other numeric attributes, listed in descending
 *                              correlation value order; the attribute must be
 *                              a numeric ECL datatype; non-numeric attributes
 *                              will return an empty child dataset; note that
 *                              this can be a time-consuming operation,
 *                              depending on the number of numeric attributes
 *                              in your dataset and the number of rows (if you
 *                              have N numeric attributes, then
 *                              N * (N - 1) / 2 calculations are performed,
 *                              each scanning all data rows)
 *
 * Most profile outputs can be disabled.  See the &apos;features&apos; argument, below.
 *
 * Data patterns can give you an idea of what your data looks like when it is
 * expressed as a (human-readable) string.  The function converts each
 * character of the string into a fixed character palette to producing a &quot;data
 * pattern&quot; and then counts the number of unique patterns for that attribute.
 * The most- and least-popular patterns from the data will be shown in the
 * output, along with the number of times that pattern appears and an example
 * (randomly chosen from the actual data).  The character palette used is:
 *
 *      A   Any uppercase letter
 *      a   Any lowercase letter
 *      9   Any numeric digit
 *      B   A boolean value (true or false)
 *
 * All other characters are left as-is in the pattern.
 *
 * Function parameters:
 *
 * @param   inFile          The dataset to process; this could be a child
 *                          dataset (e.g. inFile.childDS); REQUIRED
 * @param   fieldListStr    A string containing a comma-delimited list of
 *                          attribute names to process; note that attributes
 *                          listed here must be scalar datatypes (not child
 *                          records or child datasets); use an empty string to
 *                          process all attributes in inFile; OPTIONAL,
 *                          defaults to an empty string
 * @param   maxPatterns     The maximum number of patterns (both popular and
 *                          rare) to return for each attribute; OPTIONAL,
 *                          defaults to 100
 * @param   maxPatternLen   The maximum length of a pattern; longer patterns
 *                          are truncated in the output; this value is also
 *                          used to set the maximum length of the data to
 *                          consider when finding cardinality and mode values;
 *                          must be 33 or larger; OPTIONAL, defaults to 100
 * @param   features        A comma-delimited string listing the profiling
 *                          elements to be included in the output; OPTIONAL,
 *                          defaults to a comma-delimited string containing all
 *                          of the available keywords:
 *                              KEYWORD                 AFFECTED OUTPUT
 *                              fill_rate               fill_rate
 *                                                      fill_count
 *                              cardinality             cardinality
 *                              cardinality_breakdown   cardinality_breakdown
 *                              best_ecl_types          best_attribute_type
 *                              modes                   modes
 *                              lengths                 min_length
 *                                                      max_length
 *                                                      ave_length
 *                              patterns                popular_patterns
 *                                                      rare_patterns
 *                              min_max                 numeric_min
 *                                                      numeric_max
 *                              mean                    numeric_mean
 *                              std_dev                 numeric_std_dev
 *                              quartiles               numeric_lower_quartile
 *                                                      numeric_median
 *                                                      numeric_upper_quartile
 *                              correlations            correlations
 *                          To omit the output associated with a single keyword,
 *                          set this argument to a comma-delimited string
 *                          containing all other keywords; note that the
 *                          is_numeric output will appear only if min_max,
 *                          mean, std_dev, quartiles, or correlations features
 *                          are active; also note that enabling the
 *                          cardinality_breakdown feature will also enable
 *                          the cardinality feature, even if it is not
 *                          explicitly enabled
 * @param   sampleSize      A positive integer representing a percentage of
 *                          inFile to examine, which is useful when analyzing a
 *                          very large dataset and only an estimated data
 *                          profile is sufficient; valid range for this
 *                          argument is 1-100; values outside of this range
 *                          will be clamped; OPTIONAL, defaults to 100 (which
 *                          indicates that the entire dataset will be analyzed)
 * @param   lcbLimit        A positive integer (&lt;= 500) indicating the maximum
 *                          cardinality allowed for an attribute in order to
 *                          emit a breakdown of the attribute&apos;s values; this
 *                          parameter will be ignored if cardinality_breakdown
 *                          is not included in the features argument; OPTIONAL,
 *                          defaults to 64
 */
EXPORT Profile(inFile,
               fieldListStr = &apos;\&apos;\&apos;&apos;,
               maxPatterns = 100,
               maxPatternLen = 100,
               features = &apos;\&apos;fill_rate,best_ecl_types,cardinality,cardinality_breakdown,modes,lengths,patterns,min_max,mean,std_dev,quartiles,correlations\&apos;&apos;,
               sampleSize = 100,
               lcbLimit = 64) := FUNCTIONMACRO
    LOADXML(&apos;&lt;xml/&gt;&apos;);

    #UNIQUENAME(temp);                      // Ubiquitous &quot;contains random things&quot;
    #UNIQUENAME(scalarFields);              // Contains a delimited list of scalar attributes (full names) along with their indexed positions
    #UNIQUENAME(explicitScalarFields);      // Contains a delimited list of scalar attributes (full names) without indexed positions
    #UNIQUENAME(childDSFields);             // Contains a delimited list of child dataset attributes (full names) along with their indexed positions
    #UNIQUENAME(fieldCount);                // Contains the number of fields we&apos;ve seen while processing record layouts
    #UNIQUENAME(recLevel);                  // Will be used to determine at which level we are processing
    #UNIQUENAME(fieldStack);                // String-based stack telling us whether we&apos;re within an embedded dataset or record
    #UNIQUENAME(namePrefix);                // When processing child records and datasets, contains the leading portion of the attribute&apos;s full name
    #UNIQUENAME(fullName);                  // The full name of an attribute
    #UNIQUENAME(needsDelim);                // Boolean indicating whether we need to insert a delimiter somewhere
    #UNIQUENAME(namePos);                   // Contains character offset information, for parsing delimited strings
    #UNIQUENAME(numValue);                  // Extracted numeric value from a string
    #UNIQUENAME(nameValue);                 // Extracted string value from a string

    IMPORT Std;

    //--------------------------------------------------------------------------

    // Remove all spaces from features list so we can parse it more easily
    #UNIQUENAME(trimmedFeatures);
    LOCAL %trimmedFeatures% := TRIM(features, ALL);

    // Remove all spaces from field list so we can parse it more easily
    #UNIQUENAME(trimmedFieldList);
    LOCAL %trimmedFieldList% := TRIM(fieldListStr, ALL);

    // Clamp lcbLimit to 0..500
    #UNIQUENAME(lowCardinalityThreshold);
    LOCAL %lowCardinalityThreshold% := MIN(MAX(lcbLimit, 0), 500);

    // The maximum number of mode values to return
    #UNIQUENAME(MAX_MODES);
    LOCAL %MAX_MODES% := 5;

    // Typedefs
    #UNIQUENAME(Attribute_t);
    LOCAL %Attribute_t% := STRING;
    #UNIQUENAME(AttributeType_t);
    LOCAL %AttributeType_t% := STRING36;
    #UNIQUENAME(NumericStat_t);
    LOCAL %NumericStat_t% := DECIMAL32_4;

    // Tests for enabled features
    #UNIQUENAME(FeatureEnabledFillRate);
    LOCAL %FeatureEnabledFillRate%() := REGEXFIND(&apos;\\bfill_rate\\b&apos;, %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledBestECLTypes);
    LOCAL %FeatureEnabledBestECLTypes%() := REGEXFIND(&apos;\\bbest_ecl_types\\b&apos;, %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledLowCardinalityBreakdown);
    LOCAL %FeatureEnabledLowCardinalityBreakdown%() := %lowCardinalityThreshold% &gt; 0 AND REGEXFIND(&apos;\\bcardinality_breakdown\\b&apos;, %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledCardinality);
    LOCAL %FeatureEnabledCardinality%() := %FeatureEnabledLowCardinalityBreakdown%() OR REGEXFIND(&apos;\\bcardinality\\b&apos;, %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledModes);
    LOCAL %FeatureEnabledModes%() := REGEXFIND(&apos;\\bmodes\\b&apos;, %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledLengths);
    LOCAL %FeatureEnabledLengths%() := REGEXFIND(&apos;\\blengths\\b&apos;, %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledPatterns);
    LOCAL %FeatureEnabledPatterns%() := (UNSIGNED)maxPatterns &gt; 0 AND REGEXFIND(&apos;\\bpatterns\\b&apos;, %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledMinMax);
    LOCAL %FeatureEnabledMinMax%() := REGEXFIND(&apos;\\bmin_max\\b&apos;, %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledMean);
    LOCAL %FeatureEnabledMean%() := REGEXFIND(&apos;\\bmean\\b&apos;, %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledStdDev);
    LOCAL %FeatureEnabledStdDev%() := REGEXFIND(&apos;\\bstd_dev\\b&apos;, %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledQuartiles);
    LOCAL %FeatureEnabledQuartiles%() := REGEXFIND(&apos;\\bquartiles\\b&apos;, %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledCorrelations);
    LOCAL %FeatureEnabledCorrelations%() := REGEXFIND(&apos;\\bcorrelations\\b&apos;, %trimmedFeatures%, NOCASE);

    //--------------------------------------------------------------------------

    // Ungroup the given dataset, in case it was grouped
    #UNIQUENAME(ungroupedInFile);
    LOCAL %ungroupedInFile% := UNGROUP(inFile);

    // Clamp the sample size to something reasonable
    #UNIQUENAME(clampedSampleSize);
    LOCAL %clampedSampleSize% := MAX(1, MIN(100, (INTEGER)sampleSize));

    // Create a sample dataset if needed
    #UNIQUENAME(sampledData);
    LOCAL %sampledData% := IF
        (
            %clampedSampleSize% &lt; 100,
            ENTH(%ungroupedInFile%, %clampedSampleSize%, 100, 1, LOCAL),
            %ungroupedInFile%
        );

    // Slim the dataset if the caller provided an explicit set of attributes;
    // note that the TABLE function will fail if %trimmedFieldList% cites an
    // attribute that is a child dataset (this is an ECL limitation)
    #UNIQUENAME(workingInFile);
    LOCAL %workingInFile% :=
        #IF(%trimmedFieldList% = &apos;&apos;)
            %sampledData%
        #ELSE
            TABLE(%sampledData%, {#EXPAND(%trimmedFieldList%)})
        #END;

    // Distribute the inbound dataset across all our nodes for faster processing
    #UNIQUENAME(distributedInFile);
    LOCAL %distributedInFile% := DISTRIBUTE(%workingInFile%, SKEW(0.05));

    #EXPORTXML(inFileFields, RECORDOF(%distributedInFile%));

    // Walk the slimmed dataset, pulling out top-level scalars and noting
    // child datasets
    #SET(scalarFields, &apos;&apos;);
    #SET(childDSFields, &apos;&apos;);
    #SET(fieldCount, 0);
    #SET(recLevel, 0);
    #SET(fieldStack, &apos;&apos;);
    #SET(namePrefix, &apos;&apos;);
    #SET(fullName, &apos;&apos;);
    #FOR(inFileFields)
        #FOR(Field)
            #SET(fieldCount, %fieldCount% + 1)
            #IF(%{@isEnd}% != 1)
                // Adjust full name
                #SET(fullName, %&apos;namePrefix&apos;% + %&apos;@name&apos;%)
            #END
            #IF(%{@isRecord}% = 1)
                // Push record onto stack so we know what we&apos;re popping when we see @isEnd
                #SET(fieldStack, &apos;r&apos; + %&apos;fieldStack&apos;%)
                #APPEND(namePrefix, %&apos;@name&apos;% + &apos;.&apos;)
            #ELSEIF(%{@isDataset}% = 1)
                // Push dataset onto stack so we know what we&apos;re popping when we see @isEnd
                #SET(fieldStack, &apos;d&apos; + %&apos;fieldStack&apos;%)
                #APPEND(namePrefix, %&apos;@name&apos;% + &apos;.&apos;)
                #SET(recLevel, %recLevel% + 1)
                // Note the field index and field name so we can process it separately
                #IF(%&apos;childDSFields&apos;% != &apos;&apos;)
                    #APPEND(childDSFields, &apos;,&apos;)
                #END
                #APPEND(childDSFields, %&apos;fieldCount&apos;% + &apos;:&apos; + %&apos;fullName&apos;%)
                // Extract the child dataset into its own attribute so we can more easily
                // process it later
                #SET(temp, #MANGLE(%&apos;fullName&apos;%));
                LOCAL %temp% := NORMALIZE
                    (
                        %distributedInFile%,
                        LEFT.%fullName%,
                        TRANSFORM
                            (
                                RECORDOF(%distributedInFile%.%fullName%),
                                SELF := RIGHT
                            )
                    );
            #ELSEIF(%{@isEnd}% = 1)
                #SET(namePrefix, REGEXREPLACE(&apos;\\w+\\.$&apos;, %&apos;namePrefix&apos;%, &apos;&apos;))
                #IF(%&apos;fieldStack&apos;%[1] = &apos;d&apos;)
                    #SET(recLevel, %recLevel% - 1)
                #END
                #SET(fieldStack, %&apos;fieldStack&apos;%[2..])
            #ELSEIF(%recLevel% = 0)
                // Note the field index and full name of the attribute so we can process it
                #IF(%&apos;scalarFields&apos;% != &apos;&apos;)
                    #APPEND(scalarFields, &apos;,&apos;)
                #END
                #APPEND(scalarFields, %&apos;fieldCount&apos;% + &apos;:&apos; + %&apos;fullName&apos;%)
            #END
        #END
    #END

    // Collect the gathered full attribute names so we can walk them later
    #SET(explicitScalarFields, REGEXREPLACE(&apos;\\d+:&apos;, %&apos;scalarFields&apos;%, &apos;&apos;));

    // Define the record layout that will be used by the inner _Inner_Profile() call
    LOCAL ModeRec := RECORD
        STRING                          value;
        UNSIGNED4                       rec_count;
    END;

    LOCAL PatternCountRec := RECORD
        STRING                          data_pattern;
        UNSIGNED4                       rec_count;
        STRING                          example;
    END;

    LOCAL CorrelationRec := RECORD
        STRING                          attribute;
        DECIMAL7_6                      corr;
    END;

    LOCAL OutputLayout := RECORD
        STRING                          sortValue;
        STRING                          attribute;
        UNSIGNED4                       rec_count;
        STRING                          given_attribute_type;
        DECIMAL9_6                      fill_rate;
        UNSIGNED4                       fill_count;
        UNSIGNED4                       cardinality;
        DATASET(ModeRec)                cardinality_breakdown {MAXCOUNT(%lowCardinalityThreshold%)};
        STRING                          best_attribute_type;
        DATASET(ModeRec)                modes {MAXCOUNT(%MAX_MODES%)};
        UNSIGNED4                       min_length;
        UNSIGNED4                       max_length;
        UNSIGNED4                       ave_length;
        DATASET(PatternCountRec)        popular_patterns {MAXCOUNT((UNSIGNED)maxPatterns)};
        DATASET(PatternCountRec)        rare_patterns {MAXCOUNT((UNSIGNED)maxPatterns)};
        BOOLEAN                         is_numeric;
        %NumericStat_t%                 numeric_min;
        %NumericStat_t%                 numeric_max;
        %NumericStat_t%                 numeric_mean;
        %NumericStat_t%                 numeric_std_dev;
        %NumericStat_t%                 numeric_lower_quartile;
        %NumericStat_t%                 numeric_median;
        %NumericStat_t%                 numeric_upper_quartile;
        DATASET(CorrelationRec)         correlations {MAXCOUNT(%fieldCount%)};
    END;

    // Define the record layout that will be returned to the caller; note
    // that the structure is variable, depending on the features passed
    // to Profile()
    #UNIQUENAME(FinalOutputLayout);
    LOCAL %FinalOutputLayout% := RECORD
        STRING                          attribute;
        STRING                          given_attribute_type;
        #IF(%FeatureEnabledBestECLTypes%())
            STRING                      best_attribute_type;
        #END
        UNSIGNED4                       rec_count;
        #IF(%FeatureEnabledFillRate%())
            UNSIGNED4                   fill_count;
            DECIMAL9_6                  fill_rate;
        #END
        #IF(%FeatureEnabledCardinality%())
            UNSIGNED4                   cardinality;
        #END
        #IF(%FeatureEnabledLowCardinalityBreakdown%())
            DATASET(ModeRec)            cardinality_breakdown;
        #END
        #IF(%FeatureEnabledModes%())
            DATASET(ModeRec)            modes;
        #END
        #IF(%FeatureEnabledLengths%())
            UNSIGNED4                   min_length;
            UNSIGNED4                   max_length;
            UNSIGNED4                   ave_length;
        #END
        #IF(%FeatureEnabledPatterns%())
            DATASET(PatternCountRec)    popular_patterns;
            DATASET(PatternCountRec)    rare_patterns;
        #END
        #IF(%FeatureEnabledMinMax%() OR %FeatureEnabledMean%() OR %FeatureEnabledStdDev%() OR %FeatureEnabledQuartiles%() OR %FeatureEnabledCorrelations%())
            BOOLEAN                     is_numeric;
        #END
        #IF(%FeatureEnabledMinMax%())
            %NumericStat_t%             numeric_min;
            %NumericStat_t%             numeric_max;
        #END
        #IF(%FeatureEnabledMean%())
            %NumericStat_t%             numeric_mean;
        #END
        #IF(%FeatureEnabledStdDev%())
            %NumericStat_t%             numeric_std_dev;
        #END
        #IF(%FeatureEnabledQuartiles%())
            %NumericStat_t%             numeric_lower_quartile;
            %NumericStat_t%             numeric_median;
            %NumericStat_t%             numeric_upper_quartile;
        #END
        #IF(%FeatureEnabledCorrelations%())
            DATASET(CorrelationRec)     correlations;
        #END
    END;

    //==========================================================================

    // This is the meat of the function macro that actually does the profiling;
    // it is called with various datasets and (possibly) explicit attributes
    // to process and the results will eventually be combined to form the
    // final result; the parameters largely match the Profile() call, with the
    // addition of a few parameters that help place the results into the
    // correct format; note that the name of this function macro is not wrapped
    // in a UNIQUENAME -- that is due to an apparent limitation in the ECL
    // compiler
    LOCAL _Inner_Profile(_inFile,
                         _fieldListStr,
                         _maxPatterns,
                         _maxPatternLen,
                         _lcbLimit,
                         _maxModes,
                         _resultLayout,
                         _attrNamePrefix,
                         _sortPrefix) := FUNCTIONMACRO
        #EXPORTXML(inFileFields, RECORDOF(_inFile));
        #UNIQUENAME(foundMaxPatternLen);                // Will become the length of the longest pattern we will be processing
        #SET(foundMaxPatternLen, 33);                   // Preset to minimum length for an attribute pattern
        #UNIQUENAME(explicitFields);                    // Attributes from _fieldListStr that are found in the top level of the dataset
        #UNIQUENAME(numericFields);                     // Numeric attributes from _fieldListStr that are found in the top level of the dataset

        // Validate that attribute is okay for us to process (there is no explicit
        // attribute list or the name is in the list)
        #UNIQUENAME(_CanProcessAttribute);
        LOCAL %_CanProcessAttribute%(STRING attrName) := (_fieldListStr = &apos;&apos; OR REGEXFIND(&apos;(^|,)&apos; + attrName + &apos;(,|$)&apos;, _fieldListStr, NOCASE));

        // Test an attribute type to see if is a SET OF &lt;something&gt;
        #UNIQUENAME(_IsSetType);
        LOCAL %_IsSetType%(STRING attrType) := (attrType[..7] = &apos;set of &apos;);

        // Helper function to convert a full field name into something we
        // can reference as an ECL attribute
        #UNIQUENAME(_MakeAttr);
        LOCAL %_MakeAttr%(STRING attr) := REGEXREPLACE(&apos;\\.&apos;, attr, &apos;_&apos;);

        // Pattern mapping a STRING datatype
        #UNIQUENAME(_MapAllStr);
        LOCAL STRING %_MapAllStr%(STRING s) := EMBED(C++)
            __lenResult = lenS;
            __result = static_cast&lt;char*&gt;(rtlMalloc(__lenResult));

            for (uint32_t x = 0; x &lt; lenS; x++)
            {
                unsigned char   ch = s[x];

                if (ch &gt;= &apos;A&apos; &amp;&amp; ch &lt;= &apos;Z&apos;)
                    __result[x] = &apos;A&apos;;
                else if (ch &gt;= &apos;a&apos; &amp;&amp; ch &lt;= &apos;z&apos;)
                    __result[x] = &apos;a&apos;;
                else if (ch &gt;= &apos;1&apos; &amp;&amp; ch &lt;= &apos;9&apos;) // Leave &apos;0&apos; as-is and replace with &apos;9&apos; later
                    __result[x] = &apos;9&apos;;
                else
                    __result[x] = ch;
            }
        ENDEMBED;

        // Pattern mapping a UNICODE datatype; using regex due to the complexity
        // of the character set
        #UNIQUENAME(_MapUpperCharUni);
        LOCAL %_MapUpperCharUni%(UNICODE s) := REGEXREPLACE(u&apos;[[:upper:]]&apos;, s, u&apos;A&apos;);
        #UNIQUENAME(_MapLowerCharUni);
        LOCAL %_MapLowerCharUni%(UNICODE s) := REGEXREPLACE(u&apos;[[:lower:]]&apos;, s, u&apos;a&apos;);
        #UNIQUENAME(_MapDigitUni);
        LOCAL %_MapDigitUni%(UNICODE s) := REGEXREPLACE(u&apos;[1-9]&apos;, s, u&apos;9&apos;); // Leave &apos;0&apos; as-is and replace with &apos;9&apos; later
        #UNIQUENAME(_MapAllUni);
        LOCAL %_MapAllUni%(UNICODE s) := (STRING)%_MapDigitUni%(%_MapLowerCharUni%(%_MapUpperCharUni%(s)));

        // Trimming strings
        #UNIQUENAME(_TrimmedStr);
        LOCAL %_TrimmedStr%(STRING s) := TRIM(s, LEFT, RIGHT);
        #UNIQUENAME(_TrimmedUni);
        LOCAL %_TrimmedUni%(UNICODE s) := TRIM(s, LEFT, RIGHT);

        // Collect a list of the top-level attributes that we can process,
        // determine the actual maximum length of a data pattern (if we can
        // reduce that length then we can save on memory allocation), and
        // collect the numeric fields for correlation
        #SET(needsDelim, 0);
        #SET(recLevel, 0);
        #SET(fieldStack, &apos;&apos;);
        #SET(namePrefix, &apos;&apos;);
        #SET(explicitFields, &apos;&apos;);
        #SET(numericFields, &apos;&apos;);
        #FOR(inFileFields)
            #FOR(Field)
                #IF(%{@isRecord}% = 1)
                    #SET(fieldStack, &apos;r&apos; + %&apos;fieldStack&apos;%)
                    #APPEND(namePrefix, %&apos;@name&apos;% + &apos;.&apos;)
                #ELSEIF(%{@isDataset}% = 1)
                    #SET(fieldStack, &apos;d&apos; + %&apos;fieldStack&apos;%)
                    #SET(recLevel, %recLevel% + 1)
                #ELSEIF(%{@isEnd}% = 1)
                    #IF(%&apos;fieldStack&apos;%[1] = &apos;d&apos;)
                        #SET(recLevel, %recLevel% - 1)
                    #ELSE
                        #SET(namePrefix, REGEXREPLACE(&apos;\\w+\\.$&apos;, %&apos;namePrefix&apos;%, &apos;&apos;))
                    #END
                    #SET(fieldStack, %&apos;fieldStack&apos;%[2..])
                #ELSEIF(%recLevel% = 0)
                    #IF(%_CanProcessAttribute%(%&apos;namePrefix&apos;% + %&apos;@name&apos;%))
                        #IF(%needsDelim% = 1)
                            #APPEND(explicitFields, &apos;,&apos;)
                        #END
                        #APPEND(explicitFields, %&apos;namePrefix&apos;% + %&apos;@name&apos;%)
                        #SET(needsDelim, 1)

                        #IF(NOT %_IsSetType%(%&apos;@type&apos;%))
                            #IF(REGEXFIND(&apos;(string)|(data)|(utf)&apos;, %&apos;@type&apos;%))
                                #IF(%@size% &lt; 0)
                                    #SET(foundMaxPatternLen, MAX(_maxPatternLen, %foundMaxPatternLen%))
                                #ELSE
                                    #SET(foundMaxPatternLen, MIN(MAX(%@size%, %foundMaxPatternLen%), _maxPatternLen))
                                #END
                            #ELSEIF(REGEXFIND(&apos;unicode&apos;, %&apos;@type&apos;%))
                                // UNICODE is UCS-2 so the size reflects two bytes per character
                                #IF(%@size% &lt; 0)
                                    #SET(foundMaxPatternLen, MAX(_maxPatternLen, %foundMaxPatternLen%))
                                #ELSE
                                    #SET(foundMaxPatternLen, MIN(MAX(%@size% DIV 2 + 1, %foundMaxPatternLen%), _maxPatternLen))
                                #END
                            #ELSEIF(REGEXFIND(&apos;(integer)|(unsigned)|(decimal)|(real)&apos;, %&apos;@type&apos;%))
                                #IF(%&apos;numericFields&apos;% != &apos;&apos;)
                                    #APPEND(numericFields, &apos;,&apos;)
                                #END
                                #APPEND(numericFields, %&apos;namePrefix&apos;% + %&apos;@name&apos;%)
                            #END
                        #END
                    #END
                #END
            #END
        #END

        // Typedefs
        #UNIQUENAME(DataPattern_t);
        LOCAL %DataPattern_t% := #EXPAND(&apos;STRING&apos; + %&apos;foundMaxPatternLen&apos;%);
        #UNIQUENAME(StringValue_t);
        LOCAL %StringValue_t% := #EXPAND(&apos;STRING&apos; + %&apos;foundMaxPatternLen&apos;%);

        // Create a dataset containing pattern information, string length, and
        // booleans indicating filled and numeric datatypes for each processed
        // attribute; note that this is created by appending a series of PROJECT
        // results; to protect against skew problems when dealing with attributes
        // with low cardinality, and to attempt to reduce our temporary storage
        // footprint, create a reduced dataset that contains unique values for
        // our attributes and the number of times the values appear, as well as
        // some of the other interesting bits we can collect at the same time; note
        // that we try to explicitly target the original attribute&apos;s data type and
        // perform the minimal amount of work necessary on the value to transform
        // it to our common structure

        #UNIQUENAME(DataInfoRec);
        LOCAL %DataInfoRec% := RECORD
            %Attribute_t%       attribute;
            %AttributeType_t%   given_attribute_type;
            %StringValue_t%     string_value;
            UNSIGNED4           value_count;
            %DataPattern_t%     data_pattern;
            UNSIGNED4           data_length;
            BOOLEAN             is_filled;
            BOOLEAN             is_number;
        END;

        #UNIQUENAME(dataInfo);
        LOCAL %dataInfo% :=
            #SET(recLevel, 0)
            #SET(fieldStack, &apos;&apos;)
            #SET(namePrefix, &apos;&apos;)
            #SET(needsDelim, 0)
            #SET(fieldCount, 0)
            #FOR(inFileFields)
                #FOR(Field)
                    #IF(%{@isRecord}% = 1)
                        #SET(fieldStack, &apos;r&apos; + %&apos;fieldStack&apos;%)
                        #APPEND(namePrefix, %&apos;@name&apos;% + &apos;.&apos;)
                    #ELSEIF(%{@isDataset}% = 1)
                        #SET(fieldStack, &apos;d&apos; + %&apos;fieldStack&apos;%)
                        #SET(recLevel, %recLevel% + 1)
                    #ELSEIF(%{@isEnd}% = 1)
                        #IF(%&apos;fieldStack&apos;%[1] = &apos;d&apos;)
                            #SET(recLevel, %recLevel% - 1)
                        #ELSE
                            #SET(namePrefix, REGEXREPLACE(&apos;\\w+\\.$&apos;, %&apos;namePrefix&apos;%, &apos;&apos;))
                        #END
                        #SET(fieldStack, %&apos;fieldStack&apos;%[2..])
                    #ELSEIF(%recLevel% = 0)
                        #IF(%_CanProcessAttribute%(%&apos;namePrefix&apos;% + %&apos;@name&apos;%))
                            #SET(fieldCount, %fieldCount% + 1)
                            #IF(%needsDelim% = 1) + #END

                            IF(EXISTS(_inFile),
                                PROJECT
                                    (
                                        TABLE
                                            (
                                                _inFile,
                                                {
                                                    %Attribute_t%       attribute := %&apos;namePrefix&apos;% + %&apos;@name&apos;%,
                                                    %AttributeType_t%   given_attribute_type := %&apos;@ecltype&apos;%,
                                                    %StringValue_t%     string_value :=
                                                                            #IF(%_IsSetType%(%&apos;@type&apos;%))
                                                                                (%StringValue_t%)Std.Str.CombineWords((SET OF STRING)_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%), &apos;, &apos;)
                                                                            #ELSEIF(REGEXFIND(&apos;(integer)|(unsigned)|(decimal)|(real)|(boolean)&apos;, %&apos;@type&apos;%))
                                                                                (%StringValue_t%)_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%)
                                                                            #ELSEIF(REGEXFIND(&apos;string&apos;, %&apos;@type&apos;%))
                                                                                %_TrimmedStr%(_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%))
                                                                            #ELSE
                                                                                %_TrimmedStr%((%StringValue_t%)_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%))
                                                                            #END,
                                                    UNSIGNED4           value_count := COUNT(GROUP),
                                                    %DataPattern_t%     data_pattern :=
                                                                            #IF(%_IsSetType%(%&apos;@type&apos;%))
                                                                                %_MapAllStr%(%_TrimmedStr%(Std.Str.CombineWords((SET OF STRING)_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%), &apos;, &apos;))[..%foundMaxPatternLen%])
                                                                            #ELSEIF(REGEXFIND(&apos;(integer)|(unsigned)|(decimal)|(real)&apos;, %&apos;@type&apos;%))
                                                                                %_MapAllStr%((STRING)_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%))
                                                                            #ELSEIF(REGEXFIND(&apos;(unicode)|(utf)&apos;, %&apos;@type&apos;%))
                                                                                #IF(%@size% &lt; 0 OR (%@size% DIV 2 + 1) &gt; %foundMaxPatternLen%)
                                                                                    %_MapAllUni%(%_TrimmedUni%((UNICODE)_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%))[..%foundMaxPatternLen%])
                                                                                #ELSE
                                                                                    %_MapAllUni%(%_TrimmedUni%((UNICODE)_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%)))
                                                                                #END
                                                                            #ELSEIF(REGEXFIND(&apos;string&apos;, %&apos;@type&apos;%))
                                                                                #IF(%@size% &lt; 0 OR %@size% &gt; %foundMaxPatternLen%)
                                                                                    %_MapAllStr%(%_TrimmedStr%(_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%))[..%foundMaxPatternLen%])
                                                                                #ELSE
                                                                                    %_MapAllStr%(%_TrimmedStr%(_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%)))
                                                                                #END
                                                                            #ELSEIF(%&apos;@type&apos;% = &apos;boolean&apos;)
                                                                                &apos;B&apos;
                                                                            #ELSE
                                                                                %_MapAllStr%(%_TrimmedStr%((STRING)_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%))[..%foundMaxPatternLen%])
                                                                            #END,
                                                    UNSIGNED4           data_length :=
                                                                            #IF(%_IsSetType%(%&apos;@type&apos;%))
                                                                                COUNT(_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%))
                                                                            #ELSEIF(REGEXFIND(&apos;(unicode)|(utf)&apos;, %&apos;@type&apos;%))
                                                                                LENGTH(%_TrimmedUni%((UNICODE)_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%)))
                                                                            #ELSEIF(REGEXFIND(&apos;string&apos;, %&apos;@type&apos;%))
                                                                                LENGTH(%_TrimmedStr%(_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%)))
                                                                            #ELSEIF(%&apos;@type&apos;% = &apos;boolean&apos;)
                                                                                1
                                                                            #ELSE
                                                                                LENGTH((STRING)_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%))
                                                                            #END,
                                                    BOOLEAN             is_filled :=
                                                                            #IF(%_IsSetType%(%&apos;@type&apos;%))
                                                                                COUNT(_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%)) &gt; 0
                                                                            #ELSEIF(REGEXFIND(&apos;(unicode)|(utf)&apos;, %&apos;@type&apos;%))
                                                                                LENGTH(%_TrimmedUni%(_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%))) &gt; 0
                                                                            #ELSEIF(REGEXFIND(&apos;string&apos;, %&apos;@type&apos;%))
                                                                                LENGTH(%_TrimmedStr%(_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%))) &gt; 0
                                                                            #ELSEIF(REGEXFIND(&apos;data&apos;, %&apos;@type&apos;%))
                                                                                LENGTH(_inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%)) &gt; 0
                                                                            #ELSEIF(%&apos;@type&apos;% = &apos;boolean&apos;)
                                                                                TRUE
                                                                            #ELSE
                                                                                _inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%) != 0
                                                                            #END,
                                                    BOOLEAN             is_number :=
                                                                            #IF(%_IsSetType%(%&apos;@type&apos;%))
                                                                                FALSE
                                                                            #ELSEIF(REGEXFIND(&apos;(integer)|(unsigned)|(decimal)|(real)&apos;, %&apos;@type&apos;%))
                                                                                TRUE
                                                                            #ELSE
                                                                                FALSE
                                                                            #END
                                                },
                                                _inFile.#EXPAND(%&apos;namePrefix&apos;% + %&apos;@name&apos;%),
                                                LOCAL
                                            ),
                                            TRANSFORM(%DataInfoRec%, SELF := LEFT)
                                    ),
                                DATASET
                                    (
                                        1,
                                        TRANSFORM
                                            (
                                                %DataInfoRec%,
                                                SELF.attribute := %&apos;namePrefix&apos;% + %&apos;@name&apos;%,
                                                SELF.given_attribute_type := %&apos;@ecltype&apos;%,
                                                SELF := []
                                            )
                                    )
                                )

                            #SET(needsDelim, 1)
                        #END
                    #END
                #END
            #END

            // Insert empty value for syntax checking
            #IF(%fieldCount% = 0)
                DATASET([], %DataInfoRec%)
            #END;

        // Get only those attributes that are filled
        #UNIQUENAME(filledDataInfo);
        LOCAL %filledDataInfo% := %dataInfo%(is_filled);

        // Determine the best ECL data type for each attribute
        #UNIQUENAME(DataTypeEnum);
        LOCAL %DataTypeEnum% := ENUM
            (
                UNSIGNED4,
                    AsIs = 0,
                    SignedInteger = 1,
                    UnsignedInteger = 2,
                    FloatingPoint = 4,
                    ExpNotation = 8
            );

        #UNIQUENAME(BestTypeFlag);
        LOCAL %DataTypeEnum% %BestTypeFlag%(STRING dataPattern, %AttributeType_t% attributeType) := FUNCTION
            isLeadingZeroInteger := REGEXFIND(&apos;^0[09]{1,18}$&apos;, dataPattern);
            isSignedInteger := REGEXFIND(&apos;^\\-[09]{1,19}$&apos;, dataPattern);
            isShortUnsignedInteger := REGEXFIND(&apos;^[09]{1,19}$&apos;, dataPattern);
            isUnsignedInteger := REGEXFIND(&apos;^\\+?[09]{1,20}$&apos;, dataPattern);
            isFloatingPoint := REGEXFIND(&apos;^(\\-|\\+)?[09]{0,15}\\.[09]{1,15}$&apos;, dataPattern);
            isExpNotation := REGEXFIND(&apos;^(\\-|\\+)?[09]\\.[09]{1,6}[aA]\\-[09]{1,3}$&apos;, dataPattern);

            stringWithNumbersType := MAP
                (
                    isSignedInteger         =&gt;  %DataTypeEnum%.SignedInteger | %DataTypeEnum%.FloatingPoint | %DataTypeEnum%.ExpNotation,
                    isShortUnsignedInteger  =&gt;  %DataTypeEnum%.SignedInteger | %DataTypeEnum%.UnsignedInteger | %DataTypeEnum%.FloatingPoint | %DataTypeEnum%.ExpNotation,
                    isUnsignedInteger       =&gt;  %DataTypeEnum%.UnsignedInteger | %DataTypeEnum%.FloatingPoint | %DataTypeEnum%.ExpNotation,
                    isFloatingPoint         =&gt;  %DataTypeEnum%.FloatingPoint | %DataTypeEnum%.ExpNotation,
                    isExpNotation           =&gt;  %DataTypeEnum%.ExpNotation,
                    %DataTypeEnum%.AsIs
                );

            bestType := MAP
                (
                    %_IsSetType%(attributeType)                                                 =&gt;  %DataTypeEnum%.AsIs,
                    REGEXFIND(&apos;(integer)|(unsigned)|(decimal)|(real)|(boolean)&apos;, attributeType) =&gt;  %DataTypeEnum%.AsIs,
                    isLeadingZeroInteger                                                        =&gt;  %DataTypeEnum%.AsIs,
                    stringWithNumbersType
                );

            RETURN bestType;
        END;

        // Estimate integer size from readable data length
        #UNIQUENAME(Len2Size);
        LOCAL %Len2Size%(UNSIGNED2 c) := MAP ( c &lt; 3 =&gt; 1, c &lt; 5 =&gt; 2, c &lt; 7 =&gt; 3, c &lt; 9 =&gt; 4, c &lt; 11 =&gt; 5, c &lt; 14 =&gt; 6, c &lt; 16 =&gt; 7, 8 );

        #UNIQUENAME(attributeTypePatterns);
        LOCAL %attributeTypePatterns% := TABLE
            (
                %filledDataInfo%,
                {
                    attribute,
                    given_attribute_type,
                    data_pattern,
                    data_length,
                    %DataTypeEnum%      type_flag := %BestTypeFlag%(TRIM(data_pattern), given_attribute_type),
                    UNSIGNED4           min_data_length := 0 // will be populated within %attributesWithTypeFlagsSummary%

                },
                attribute, given_attribute_type, data_pattern, data_length,
                MERGE
            );

        #UNIQUENAME(MinNotZero);
        LOCAL %MinNotZero%(UNSIGNED4 n1, UNSIGNED4 n2) := MAP
            (
                n1 = 0  =&gt;  n2,
                n2 = 0  =&gt;  n1,
                MIN(n1, n2)
            );

        #UNIQUENAME(attributesWithTypeFlagsSummary);
        LOCAL %attributesWithTypeFlagsSummary% := AGGREGATE
            (
                %attributeTypePatterns%,
                RECORDOF(%attributeTypePatterns%),
                TRANSFORM
                    (
                        RECORDOF(%attributeTypePatterns%),
                        SELF.data_length := MAX(LEFT.data_length, RIGHT.data_length),
                        SELF.min_data_length := %MinNotZero%(LEFT.data_length, RIGHT.data_length),
                        SELF.type_flag := IF(TRIM(RIGHT.attribute) != &apos;&apos;, LEFT.type_flag &amp; RIGHT.type_flag, LEFT.type_flag),
                        SELF := LEFT
                    ),
                TRANSFORM
                    (
                        RECORDOF(%attributeTypePatterns%),
                        SELF.data_length := MAX(RIGHT1.data_length, RIGHT2.data_length),
                        SELF.min_data_length := %MinNotZero%(RIGHT1.data_length, RIGHT2.data_length),
                        SELF.type_flag := RIGHT1.type_flag &amp; RIGHT2.type_flag,
                        SELF := RIGHT1
                    ),
                LEFT.attribute,
                FEW
            );

        #UNIQUENAME(AttributeTypeRec);
        LOCAL %AttributeTypeRec% := RECORD
            %Attribute_t%       attribute;
            %AttributeType_t%   given_attribute_type;
            %AttributeType_t%   best_attribute_type;
        END;

        #UNIQUENAME(attributeBestTypeInfo);
        LOCAL %attributeBestTypeInfo% := PROJECT
            (
                %attributesWithTypeFlagsSummary%,
                TRANSFORM
                    (
                        %AttributeTypeRec%,
                        SELF.best_attribute_type := MAP
                            (
                                %_IsSetType%(LEFT.given_attribute_type)                                                 =&gt;  LEFT.given_attribute_type,
                                REGEXFIND(&apos;(integer)|(unsigned)|(decimal)|(real)|(boolean)&apos;, LEFT.given_attribute_type) =&gt;  LEFT.given_attribute_type,
                                REGEXFIND(&apos;data&apos;, LEFT.given_attribute_type)                                            =&gt;  &apos;data&apos; + IF(LEFT.data_length &gt; 0 AND (LEFT.data_length &lt; (LEFT.min_data_length * 1000)), (STRING)LEFT.data_length, &apos;&apos;),
                                (LEFT.type_flag &amp; %DataTypeEnum%.UnsignedInteger) != 0                                  =&gt;  &apos;unsigned&apos; + %Len2Size%(LEFT.data_length),
                                (LEFT.type_flag &amp; %DataTypeEnum%.SignedInteger) != 0                                    =&gt;  &apos;integer&apos; + %Len2Size%(LEFT.data_length),
                                (LEFT.type_flag &amp; %DataTypeEnum%.FloatingPoint) != 0                                    =&gt;  &apos;real&apos; + IF(LEFT.data_length &lt; 8, &apos;4&apos;, &apos;8&apos;),
                                (LEFT.type_flag &amp; %DataTypeEnum%.ExpNotation) != 0                                      =&gt;  &apos;real8&apos;,
                                REGEXFIND(&apos;utf&apos;, LEFT.given_attribute_type)                                             =&gt;  LEFT.given_attribute_type,
                                REGEXREPLACE(&apos;\\d+$&apos;, TRIM(LEFT.given_attribute_type), &apos;&apos;) + IF(LEFT.data_length &gt; 0 AND (LEFT.data_length &lt; (LEFT.min_data_length * 1000)), (STRING)LEFT.data_length, &apos;&apos;)
                            ),
                        SELF := LEFT
                    )
            );

        #UNIQUENAME(filledDataInfoNumeric);
        LOCAL %filledDataInfoNumeric% := JOIN
            (
                %filledDataInfo%,
                %attributeBestTypeInfo%,
                LEFT.attribute = RIGHT.attribute,
                TRANSFORM
                    (
                        RECORDOF(LEFT),
                        SELF.is_number := LEFT.is_number OR (REGEXFIND(&apos;(integer)|(unsigned)|(decimal)|(real)&apos;, RIGHT.best_attribute_type) AND NOT REGEXFIND(&apos;set of &apos;, RIGHT.best_attribute_type)),
                        SELF := LEFT
                    ),
                LEFT OUTER, KEEP(1), SMART
            ) : ONWARNING(4531, IGNORE);

        // Build a set of attributes for quartiles, unique values, and modes for
        // each processed attribute
        #SET(recLevel, 0);
        #SET(fieldStack, &apos;&apos;);
        #SET(namePrefix, &apos;&apos;);
        #FOR(inFileFields)
            #FOR(Field)
                #IF(%{@isRecord}% = 1)
                    #SET(fieldStack, &apos;r&apos; + %&apos;fieldStack&apos;%)
                    #APPEND(namePrefix, %&apos;@name&apos;% + &apos;.&apos;)
                #ELSEIF(%{@isDataset}% = 1)
                    #SET(fieldStack, &apos;d&apos; + %&apos;fieldStack&apos;%)
                    #SET(recLevel, %recLevel% + 1)
                #ELSEIF(%{@isEnd}% = 1)
                    #IF(%&apos;fieldStack&apos;%[1] = &apos;d&apos;)
                        #SET(recLevel, %recLevel% - 1)
                    #ELSE
                        #SET(namePrefix, REGEXREPLACE(&apos;\\w+\\.$&apos;, %&apos;namePrefix&apos;%, &apos;&apos;))
                    #END
                    #SET(fieldStack, %&apos;fieldStack&apos;%[2..]);
                #ELSEIF(%recLevel% = 0)
                    #IF(%_CanProcessAttribute%(%&apos;namePrefix&apos;% + %&apos;@name&apos;%))
                        // Note that we create explicit attributes here for all
                        // top-level attributes in the dataset that we&apos;re
                        // processing, even if they are not numeric datatypes
                        #UNIQUENAME(uniqueNumericValueCounts)
                        %uniqueNumericValueCounts% := PROJECT
                            (
                                %filledDataInfoNumeric%(attribute = %&apos;namePrefix&apos;% + %&apos;@name&apos;% AND is_number),
                                TRANSFORM
                                    (
                                        {
                                            REAL        value,
                                            UNSIGNED6   cnt,
                                            UNSIGNED6   valueEndPos
                                        },
                                        SELF.value := (REAL)LEFT.string_value,
                                        SELF.cnt := LEFT.value_count,
                                        SELF.valueEndPos := 0
                                    )
                            );

                        // Explicit attributes containing scalars
                        LOCAL #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_min&apos;)) := MIN(%uniqueNumericValueCounts%, value);
                        LOCAL #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_max&apos;)) := MAX(%uniqueNumericValueCounts%, value);
                        LOCAL #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_ave&apos;)) := SUM(%uniqueNumericValueCounts%, value * cnt) / SUM(%uniqueNumericValueCounts%, cnt);
                        LOCAL #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_std_dev&apos;)) := SQRT(SUM(%uniqueNumericValueCounts%, (value - #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_ave&apos;))) * (value - #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_ave&apos;))) * cnt) / SUM(%uniqueNumericValueCounts%, cnt));

                        // Determine the position of the last record in the original
                        // dataset that contains a particular value
                        #UNIQUENAME(uniqueNumericValuePos)
                        %uniqueNumericValuePos% := ITERATE
                            (
                                SORT(%uniqueNumericValueCounts%, value, SKEW(1)),
                                TRANSFORM
                                    (
                                        RECORDOF(LEFT),
                                        SELF.valueEndPos := LEFT.valueEndPos + RIGHT.cnt,
                                        SELF := RIGHT
                                    )
                            );

                        // The total number of records in this subset
                        #UNIQUENAME(wholeNumRecs)
                        LOCAL %wholeNumRecs% := MAX(%uniqueNumericValuePos%, valueEndPos);
                        #UNIQUENAME(halfNumRecs);
                        LOCAL %halfNumRecs% := %wholeNumRecs% DIV 2;

                        // Find the median
                        #UNIQUENAME(q2Pos1);
                        LOCAL %q2Pos1% := %halfNumRecs% + (%wholeNumRecs% % 2);
                        #UNIQUENAME(q2Value1);
                        LOCAL %q2Value1% := MIN(%uniqueNumericValuePos%(valueEndPos &gt;= %q2Pos1%), value);
                        #UNIQUENAME(q2Pos2);
                        LOCAL %q2Pos2% := %q2Pos1% + ((%wholeNumRecs% + 1) % 2);
                        #UNIQUENAME(q2Value2);
                        LOCAL %q2Value2% := MIN(%uniqueNumericValuePos%(valueEndPos &gt;= %q2Pos2%), value);
                        LOCAL #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_q2_value&apos;)) := AVE(%q2Value1%, %q2Value2%);

                        // Find the lower quartile
                        #UNIQUENAME(q1Pos1);
                        LOCAL %q1Pos1% := (%halfNumRecs% DIV 2) + (%halfNumRecs% % 2);
                        #UNIQUENAME(q1Value1);
                        LOCAL %q1Value1% := MIN(%uniqueNumericValuePos%(valueEndPos &gt;= %q1Pos1%), value);
                        #UNIQUENAME(q1Pos2);
                        LOCAL %q1Pos2% := %q1Pos1% + ((%halfNumRecs% + 1) % 2);
                        #UNIQUENAME(q1Value2);
                        LOCAL %q1Value2% := MIN(%uniqueNumericValuePos%(valueEndPos &gt;= %q1Pos2%), value);
                        LOCAL #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_q1_value&apos;)) := IF(%halfNumRecs% &gt; 0, AVE(%q1Value1%, %q1Value2%), 0);

                        // Find the upper quartile
                        #UNIQUENAME(q3Pos1);
                        LOCAL %q3Pos1% := MAX(%q2Pos1%, %q2Pos2%) + (%halfNumRecs% DIV 2) + (%halfNumRecs% % 2);
                        #UNIQUENAME(q3Value1);
                        LOCAL %q3Value1% := MIN(%uniqueNumericValuePos%(valueEndPos &gt;= %q3Pos1%), value);
                        #UNIQUENAME(q3Pos2);
                        LOCAL %q3Pos2% := %q3Pos1% - ((%halfNumRecs% + 1) % 2);
                        #UNIQUENAME(q3Value2);
                        LOCAL %q3Value2% := MIN(%uniqueNumericValuePos%(valueEndPos &gt;= %q3Pos2%), value);
                        LOCAL #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_q3_value&apos;)) := IF(%halfNumRecs% &gt; 0, AVE(%q3Value1%, %q3Value2%), 0);

                        // Derive all unique data values and the number of times
                        // each occurs in the data
                        LOCAL #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_uniq_value_recs&apos;)) := TABLE
                            (
                                %filledDataInfoNumeric%(attribute = %&apos;namePrefix&apos;% + %&apos;@name&apos;%),
                                {
                                    string_value,
                                    UNSIGNED4 rec_count := SUM(GROUP, value_count)
                                },
                                string_value,
                                MERGE
                            );

                        // Find the mode of the (string) data; using a JOIN here
                        // to avoid the 10MB limit error that sometimes occurs
                        // when you use filters to find a single value; also note
                        // the TOPN calls to reduce the search space, which also
                        // effectively limit the final result to _maxModes records
                        #UNIQUENAME(topRecords);
                        %topRecords% := TOPN(#EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_uniq_value_recs&apos;)), _maxModes, -rec_count);
                        #UNIQUENAME(topRecord)
                        %topRecord% := TOPN(%topRecords%, 1, -rec_count);
                        LOCAL #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_mode_values&apos;)) := JOIN
                            (
                                %topRecords%,
                                %topRecord%,
                                LEFT.rec_count = RIGHT.rec_count,
                                TRANSFORM
                                    (
                                        ModeRec,
                                        SELF.value := LEFT.string_value,
                                        SELF.rec_count := LEFT.rec_count
                                    ),
                                SMART
                            ) : ONWARNING(4531, IGNORE);

                        // Get records with low cardinality
                        LOCAL #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_lcb_recs&apos;)) := IF
                            (
                                COUNT(#EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_uniq_value_recs&apos;))) &lt;= _lcbLimit,
                                PROJECT
                                    (
                                        SORT(#EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_uniq_value_recs&apos;)), -rec_count),
                                        TRANSFORM
                                            (
                                                ModeRec,
                                                SELF.value := LEFT.string_value,
                                                SELF.rec_count := LEFT.rec_count
                                            )
                                    ),
                                DATASET([], ModeRec)
                            );
                    #END
                #END
            #END
        #END

        // Run correlations on all unique pairs of numeric fields in the data

        #UNIQUENAME(BaseCorrelationLayout);
        LOCAL %BaseCorrelationLayout% := RECORD
            %Attribute_t%   attribute_x;
            %Attribute_t%   attribute_y;
            REAL            corr;
        END;

        #UNIQUENAME(corrNamePosX);
        #UNIQUENAME(corrNamePosY);
        #UNIQUENAME(fieldX);
        #UNIQUENAME(fieldY);
        #SET(needsDelim, 0);

        #UNIQUENAME(correlations0);
        LOCAL %correlations0% := DATASET
            (
                [
                    #SET(corrNamePosX, 1)
                    #LOOP
                        #SET(fieldX, REGEXFIND(&apos;^([^,]+)&apos;, %&apos;numericFields&apos;%[%corrNamePosX%..], 1))
                        #IF(%&apos;fieldX&apos;% != &apos;&apos;)
                            #SET(corrNamePosY, %corrNamePosX% + LENGTH(%&apos;fieldX&apos;%) + 1)
                            #LOOP
                                #SET(fieldY, REGEXFIND(&apos;^([^,]+)&apos;, %&apos;numericFields&apos;%[%corrNamePosY%..], 1))
                                #IF(%&apos;fieldY&apos;% != &apos;&apos;)
                                    #IF(%needsDelim% = 1) , #END
                                    {
                                        %&apos;fieldX&apos;%,
                                        %&apos;fieldY&apos;%,
                                        CORRELATION(_inFile, %fieldX%, %fieldY%)
                                    }
                                    #SET(needsDelim, 1)

                                    #SET(corrNamePosY, %corrNamePosY% + LENGTH(%&apos;fieldY&apos;%) + 1)
                                #ELSE
                                    #BREAK
                                #END
                            #END
                            #SET(corrNamePosX, %corrNamePosX% + LENGTH(%&apos;fieldX&apos;%) + 1)
                        #ELSE
                            #BREAK
                        #END
                    #END
                ],
                %BaseCorrelationLayout%
            );

        // Append a duplicate of the correlations to itself with the X and Y fields
        // reversed so we can easily merge results on a per-attribute basis later
        #UNIQUENAME(correlations);
        LOCAL %correlations% := %correlations0% + PROJECT
            (
                %correlations0%,
                TRANSFORM
                    (
                        RECORDOF(LEFT),
                        SELF.attribute_x := LEFT.attribute_y,
                        SELF.attribute_y := LEFT.attribute_x,
                        SELF := LEFT
                    )
            );

        // Create a small dataset that specifies the output order of the named
        // attributes (which should be the same as the input order)
        #UNIQUENAME(resultOrderDS);
        LOCAL %resultOrderDS% := DATASET
            (
                [
                    #SET(needsDelim, 0)
                    #SET(corrNamePosX, 1)
                    #SET(fieldY, 1)
                    #LOOP
                        #SET(fieldX, REGEXFIND(&apos;^([^,]+)&apos;, %&apos;explicitFields&apos;%[%corrNamePosX%..], 1))
                        #IF(%&apos;fieldX&apos;% != &apos;&apos;)
                            #IF(%needsDelim% = 1) , #END
                            {%fieldY%, %&apos;fieldX&apos;%}
                            #SET(needsDelim, 1)
                            #SET(corrNamePosX, %corrNamePosX% + LENGTH(%&apos;fieldX&apos;%) + 1)
                            #SET(fieldY, %fieldY% + 1)
                        #ELSE
                            #BREAK
                        #END
                    #END
                ],
                {
                    UNSIGNED2       nameOrder,
                    %Attribute_t%   attrName
                }
            );

        //--------------------------------------------------------------------------
        // Collect individual stats for each attribute; these are grouped by the
        // criteria used to group them
        //--------------------------------------------------------------------------

        // Count data patterns used per attribute; extract the most common and
        // most rare, taking care to not allow the two to overlap; we will
        // replace the &apos;0&apos; character left in from the pattern generation with
        // a &apos;9&apos; character to make the numeric pattern complete
        #UNIQUENAME(dataPatternStats0);
        LOCAL %dataPatternStats0% := PROJECT
            (
                %filledDataInfoNumeric%,
                TRANSFORM
                    (
                        RECORDOF(LEFT),
                        SELF.data_pattern := Std.Str.FindReplace(LEFT.data_pattern, &apos;0&apos;, &apos;9&apos;),
                        SELF := LEFT
                    )
            );

        #UNIQUENAME(dataPatternStats);
        LOCAL %dataPatternStats% := TABLE
            (
                DISTRIBUTE(%dataPatternStats0%, HASH32(attribute)),
                {
                    attribute,
                    data_pattern,
                    STRING      example := string_value[..%foundMaxPatternLen%],
                    UNSIGNED4   rec_count := SUM(GROUP, value_count)
                },
                attribute, data_pattern,
                LOCAL
            ) : ONWARNING(2168, IGNORE);
        #UNIQUENAME(groupedDataPatterns);
        LOCAL %groupedDataPatterns% := GROUP(SORT(%dataPatternStats%, attribute, LOCAL), attribute, LOCAL);
        #UNIQUENAME(topDataPatterns);
        LOCAL %topDataPatterns% := UNGROUP(TOPN(%groupedDataPatterns%, (UNSIGNED)_maxPatterns, -rec_count, data_pattern));
        #UNIQUENAME(rareDataPatterns0);
        LOCAL %rareDataPatterns0% := UNGROUP(TOPN(%groupedDataPatterns%, (UNSIGNED)_maxPatterns, rec_count, data_pattern));
        #UNIQUENAME(rareDataPatterns);
        LOCAL %rareDataPatterns% := JOIN
            (
                %rareDataPatterns0%,
                %topDataPatterns%,
                LEFT.attribute = RIGHT.attribute AND LEFT.data_pattern = RIGHT.data_pattern,
                TRANSFORM(LEFT),
                LEFT ONLY
            ) : ONWARNING(4531, IGNORE);

        // Find min, max and average data lengths per attribute
        #UNIQUENAME(dataLengthStats);
        LOCAL %dataLengthStats% := TABLE
            (
                %filledDataInfoNumeric%,
                {
                    attribute,
                    UNSIGNED4   min_length := MIN(GROUP, data_length),
                    UNSIGNED4   max_length := MAX(GROUP, data_length),
                    UNSIGNED4   ave_length := SUM(GROUP, data_length * value_count) / SUM(GROUP, value_count)
                },
                attribute,
                MERGE
            );

        // Count attribute fill rates per attribute; will be turned into
        // percentages later
        #UNIQUENAME(dataFilledStats);
        LOCAL %dataFilledStats% := TABLE
            (
                %dataInfo%,
                {
                    attribute,
                    given_attribute_type,
                    UNSIGNED4   rec_count := SUM(GROUP, value_count),
                    UNSIGNED4   filled_count := SUM(GROUP, IF(is_filled, value_count, 0))
                },
                attribute, given_attribute_type,
                MERGE
            );

        // Compute the cardinality and pull in previously-computed explicit
        // attribute values at the same time
        #UNIQUENAME(cardinalityAndNumerics);
        LOCAL %cardinalityAndNumerics% := DATASET
            (
                [
                    #SET(recLevel, 0)
                    #SET(fieldStack, &apos;&apos;)
                    #SET(namePrefix, &apos;&apos;)
                    #SET(needsDelim, 0)
                    #FOR(inFileFields)
                        #FOR(Field)
                            #IF(%{@isRecord}% = 1)
                                #SET(fieldStack, &apos;r&apos; + %&apos;fieldStack&apos;%)
                                #APPEND(namePrefix, %&apos;@name&apos;% + &apos;.&apos;)
                            #ELSEIF(%{@isDataset}% = 1)
                                #SET(fieldStack, &apos;d&apos; + %&apos;fieldStack&apos;%)
                                #SET(recLevel, %recLevel% + 1)
                            #ELSEIF(%{@isEnd}% = 1)
                                #IF(%&apos;fieldStack&apos;%[1] = &apos;d&apos;)
                                    #SET(recLevel, %recLevel% - 1)
                                #ELSE
                                    #SET(namePrefix, REGEXREPLACE(&apos;\\w+\\.$&apos;, %&apos;namePrefix&apos;%, &apos;&apos;))
                                #END
                                #SET(fieldStack, %&apos;fieldStack&apos;%[2..])
                            #ELSEIF(%recLevel% = 0)
                                #IF(%_CanProcessAttribute%(%&apos;namePrefix&apos;% + %&apos;@name&apos;%))
                                    #IF(%needsDelim% = 1) , #END

                                    {
                                        %&apos;namePrefix&apos;% + %&apos;@name&apos;%,
                                        #IF(%_IsSetType%(%&apos;@type&apos;%))
                                            FALSE,
                                        #ELSEIF(REGEXFIND(&apos;(integer)|(unsigned)|(decimal)|(real)&apos;, %&apos;@type&apos;%))
                                            TRUE,
                                        #ELSE
                                            FALSE,
                                        #END
                                        #IF(%FeatureEnabledCardinality%())
                                            COUNT(#EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_uniq_value_recs&apos;))),
                                        #ELSE
                                            0,
                                        #END
                                        #IF(%FeatureEnabledMinMax%())
                                            #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_min&apos;)),
                                            #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_max&apos;)),
                                        #ELSE
                                            0,
                                            0,
                                        #END
                                        #IF(%FeatureEnabledMean%())
                                            #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_ave&apos;)),
                                        #ELSE
                                            0,
                                        #END
                                        #IF(%FeatureEnabledStdDev%())
                                            #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_std_dev&apos;)),
                                        #ELSE
                                            0,
                                        #END
                                        #IF(%FeatureEnabledQuartiles%())
                                            #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_q1_value&apos;)),
                                            #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_q2_value&apos;)),
                                            #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_q3_value&apos;)),
                                        #ELSE
                                            0,
                                            0,
                                            0,
                                        #END
                                        #IF(%FeatureEnabledModes%())
                                            #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_mode_values&apos;))(rec_count &gt; 1), // Modes must have more than one instance
                                        #ELSE
                                            DATASET([], ModeRec),
                                        #END
                                        #IF(%FeatureEnabledLowCardinalityBreakdown%())
                                            #EXPAND(%_MakeAttr%(%&apos;namePrefix&apos;% + %&apos;@name&apos;% + &apos;_lcb_recs&apos;))
                                        #ELSE
                                            DATASET([], ModeRec)
                                        #END
                                    }

                                    #SET(needsDelim, 1)
                                #END
                            #END
                        #END
                    #END
                ],
                {
                    %Attribute_t%       attribute,
                    BOOLEAN             is_numeric,
                    UNSIGNED4           cardinality,
                    REAL                numeric_min,
                    REAL                numeric_max,
                    REAL                numeric_mean,
                    REAL                numeric_std_dev,
                    REAL                numeric_lower_quartile,
                    REAL                numeric_median,
                    REAL                numeric_upper_quartile,
                    DATASET(ModeRec)    modes;
                    DATASET(ModeRec)    cardinality_breakdown;
                }
            );

        //--------------------------------------------------------------------------
        // Collect the individual results into a single output dataset
        //--------------------------------------------------------------------------

        #UNIQUENAME(final10);
        LOCAL %final10% := PROJECT
            (
                %dataFilledStats%,
                TRANSFORM
                    (
                        _resultLayout,
                        SELF.attribute := TRIM(LEFT.attribute, RIGHT),
                        SELF.given_attribute_type := TRIM(LEFT.given_attribute_type, RIGHT),
                        SELF.rec_count := LEFT.rec_count,
                        SELF.fill_rate := #IF(%FeatureEnabledFillRate%()) LEFT.filled_count / LEFT.rec_count * 100 #ELSE 0 #END,
                        SELF.fill_count := #IF(%FeatureEnabledFillRate%()) LEFT.filled_count #ELSE 0 #END,
                        SELF := []
                    )
            );

        #UNIQUENAME(final15);
        LOCAL %final15% :=
            #IF(%FeatureEnabledBestECLTypes%())
                JOIN
                    (
                        %final10%,
                        %attributeBestTypeInfo%,
                        LEFT.attribute = RIGHT.attribute,
                        TRANSFORM
                            (
                                RECORDOF(LEFT),
                                SELF.best_attribute_type := IF(TRIM(RIGHT.best_attribute_type, RIGHT) != &apos;&apos;, TRIM(RIGHT.best_attribute_type, RIGHT), LEFT.given_attribute_type),
                                SELF := LEFT
                            ),
                        LEFT OUTER
                    ) : ONWARNING(4531, IGNORE)
            #ELSE
                %final10%
            #END;

        #UNIQUENAME(final20);
        LOCAL %final20% :=
            #IF(%FeatureEnabledLengths%())
                JOIN
                    (
                        %final15%,
                        %dataLengthStats%,
                        LEFT.attribute = RIGHT.attribute,
                        TRANSFORM
                            (
                                RECORDOF(LEFT),
                                SELF.attribute := LEFT.attribute,
                                SELF := RIGHT,
                                SELF := LEFT
                            ),
                        LEFT OUTER, KEEP(1), SMART
                    ) : ONWARNING(4531, IGNORE)
            #ELSE
                %final15%
            #END;

        #UNIQUENAME(final30);
        LOCAL %final30% :=
            #IF(%FeatureEnabledCardinality%() OR %FeatureEnabledLowCardinalityBreakdown%() OR %FeatureEnabledMinMax%() OR %FeatureEnabledMean%() OR %FeatureEnabledStdDev%() OR %FeatureEnabledQuartiles%() OR %FeatureEnabledModes%())
                JOIN
                    (
                        %final20%,
                        %cardinalityAndNumerics%,
                        LEFT.attribute = RIGHT.attribute,
                        TRANSFORM
                            (
                                RECORDOF(LEFT),
                                SELF.attribute := LEFT.attribute,
                                SELF := RIGHT,
                                SELF := LEFT
                            ),
                        LEFT OUTER, KEEP(1), SMART
                    ) : ONWARNING(4531, IGNORE)
            #ELSE
                %final20%
            #END;

        #UNIQUENAME(final35);
        LOCAL %final35% := JOIN
            (
                %final30%,
                %attributeBestTypeInfo%,
                LEFT.attribute = RIGHT.attribute,
                TRANSFORM
                    (
                        RECORDOF(LEFT),
                        SELF.is_numeric := LEFT.is_numeric OR (REGEXFIND(&apos;(integer)|(unsigned)|(decimal)|(real)&apos;, RIGHT.best_attribute_type) AND NOT REGEXFIND(&apos;set of &apos;, RIGHT.best_attribute_type)),
                        SELF := LEFT
                    ),
                LEFT OUTER, KEEP(1), SMART
            ) : ONWARNING(4531, IGNORE);

        #UNIQUENAME(final40);
        LOCAL %final40% :=
            #IF(%FeatureEnabledPatterns%())
                DENORMALIZE
                    (
                        %final35%,
                        %topDataPatterns%,
                        LEFT.attribute = RIGHT.attribute,
                        GROUP,
                        TRANSFORM
                            (
                                RECORDOF(LEFT),
                                SELF.popular_patterns := SORT(PROJECT(ROWS(RIGHT), TRANSFORM(PatternCountRec, SELF := LEFT)), -rec_count, data_pattern),
                                SELF := LEFT
                            ),
                        LEFT OUTER, SMART
                    ) : ONWARNING(4531, IGNORE)
            #ELSE
                %final35%
            #END;

        #UNIQUENAME(final50);
        LOCAL %final50% :=
            #IF(%FeatureEnabledPatterns%())
                DENORMALIZE
                    (
                        %final40%,
                        %rareDataPatterns%,
                        LEFT.attribute = RIGHT.attribute,
                        GROUP,
                        TRANSFORM
                            (
                                RECORDOF(LEFT),
                                SELF.rare_patterns := SORT(PROJECT(ROWS(RIGHT), TRANSFORM(PatternCountRec, SELF := LEFT)), rec_count, data_pattern),
                                SELF := LEFT
                            ),
                        LEFT OUTER, SMART
                    ) : ONWARNING(4531, IGNORE)
            #ELSE
                %final40%
            #END;

        #UNIQUENAME(final60);
        LOCAL %final60% :=
                #IF(%FeatureEnabledCorrelations%())
                    DENORMALIZE
                        (
                            %final50%,
                            %correlations%,
                            LEFT.attribute = RIGHT.attribute_x,
                            GROUP,
                            TRANSFORM
                                (
                                    RECORDOF(LEFT),
                                    SELF.correlations := SORT
                                        (
                                            PROJECT
                                                (
                                                    ROWS(RIGHT),
                                                    TRANSFORM
                                                        (
                                                            CorrelationRec,
                                                            SELF.attribute := LEFT.attribute_y,
                                                            SELF.corr := LEFT.corr
                                                        )
                                                ),
                                            -corr
                                        ),
                                    SELF := LEFT
                                ),
                            LEFT OUTER, SMART
                        )
                #ELSE
                    %final50%
                #END;

        // Append the attribute order to the results; we will sort on the order
        // when creating the final output
        #UNIQUENAME(final70);
        LOCAL %final70% := JOIN
            (
                %final60%,
                %resultOrderDS%,
                TRIM(LEFT.attribute, LEFT, RIGHT) = RIGHT.attrName,
                TRANSFORM
                    (
                        RECORDOF(LEFT),
                        SELF.sortValue := _sortPrefix + INTFORMAT(RIGHT.nameOrder, 5, 1),
                        SELF.attribute := _attrNamePrefix + LEFT.attribute,
                        SELF := LEFT
                    )
            ) : ONWARNING(4531, IGNORE);

        RETURN #IF(%fieldCount% &gt; 0) %final70% #ELSE DATASET([], _resultLayout) #END;
    ENDMACRO;

    //==========================================================================

    // Call _Inner_Profile() with the given input dataset top-level scalar attributes,
    // then again for each child dataset that has been found; combine the
    // results of all the calls
    #UNIQUENAME(collectedResults);
    LOCAL %collectedResults% :=
        #IF(%&apos;explicitScalarFields&apos;% != &apos;&apos;)
            _Inner_Profile
                (
                    GLOBAL(%distributedInFile%),
                    %&apos;explicitScalarFields&apos;%,
                    maxPatterns,
                    maxPatternLen,
                    %lowCardinalityThreshold%,
                    %MAX_MODES%,
                    OutputLayout,
                    &apos;&apos;,
                    &apos;&apos;
                )
        #ELSE
            DATASET([], OutputLayout)
        #END
        #UNIQUENAME(dsNameValue)
        #SET(namePos, 1)
        #LOOP
            #SET(dsNameValue, REGEXFIND(&apos;^([^,]+)&apos;, %&apos;childDSFields&apos;%[%namePos%..], 1))
            #IF(%&apos;dsNameValue&apos;% != &apos;&apos;)
                #SET(numValue, REGEXFIND(&apos;^(\\d+):&apos;, %&apos;dsNameValue&apos;%, 1))
                #SET(nameValue, REGEXFIND(&apos;:([^:]+)$&apos;, %&apos;dsNameValue&apos;%, 1))
                // The child dataset should have been extracted into its own
                // local attribute; reference it during our call to the inner
                // profile function macro
                #SET(temp, #MANGLE(%&apos;nameValue&apos;%))
                + _Inner_Profile
                    (
                        GLOBAL(%temp%),
                        &apos;&apos;,
                        maxPatterns,
                        maxPatternLen,
                        %lowCardinalityThreshold%,
                        %MAX_MODES%,
                        OutputLayout,
                        %&apos;nameValue&apos;% + &apos;.&apos;,
                        INTFORMAT(%numValue%, 5, 1) + &apos;.&apos;
                    )
                #SET(namePos, %namePos% + LENGTH(%&apos;dsNameValue&apos;%) + 1)
            #ELSE
                #BREAK
            #END
        #END;

    // Put the combined _Inner_Profile() results in the right order and layout
    #UNIQUENAME(finalData);
    LOCAL %finalData% := PROJECT(SORT(%collectedResults%, sortValue), %FinalOutputLayout%);

    RETURN %finalData%;
ENDMACRO;&#10;
  </Attribute>
 </Module>
 <Module flags="5"
         fullname="/home/gavin/buildr/RelWithDebInfo/libs/libstringlib.so"
         key="lib_stringlib"
         name="lib_stringlib"
         plugin="libstringlib.so"
         sourcePath="lib_stringlib"
         ts="1584540210000000"
         version="STRINGLIB 1.1.14">
  <Text>export StringLib := SERVICE:fold
  string StringFilterOut(const string src, const string _within) : c, pure,entrypoint=&apos;slStringFilterOut&apos;;
  string StringFilter(const string src, const string _within) : c, pure,entrypoint=&apos;slStringFilter&apos;;
  string StringSubstituteOut(const string src, const string _within, const string _newchar) : c, pure,entrypoint=&apos;slStringSubsOut&apos;;
  string StringSubstitute(const string src, const string _within, const string _newchar) : c, pure,entrypoint=&apos;slStringSubs&apos;;
  string StringRepad(const string src, unsigned4 size) : c, pure,entrypoint=&apos;slStringRepad&apos;;
  string StringTranslate(const string src, const string _within, const string _mapping) : c, pure,entrypoint=&apos;slStringTranslate&apos;;
  unsigned integer4 StringFind(const string src, const string tofind, unsigned4 instance ) : c, pure,entrypoint=&apos;slStringFind&apos;;
  unsigned integer4 StringUnboundedUnsafeFind(const string src, const string tofind ) : c,pure,nofold,entrypoint=&apos;slStringFind2&apos;;
  unsigned integer4 StringFindCount(const string src, const string tofind) : c, pure,entrypoint=&apos;slStringFindCount&apos;;
  unsigned integer4 EbcdicStringFind(const ebcdic string src, const ebcdic string tofind , unsigned4 instance ) : c,pure,entrypoint=&apos;slStringFind&apos;;
  unsigned integer4 EbcdicStringUnboundedUnsafeFind(const ebcdic string src, const ebcdic string tofind ) : c,pure,nofold,entrypoint=&apos;slStringFind2&apos;;
  string StringExtract(const string src, unsigned4 instance) : c,pure,entrypoint=&apos;slStringExtract&apos;;
  string8 GetDateYYYYMMDD() : c,once,entrypoint=&apos;slGetDateYYYYMMDD2&apos;;
  varstring GetBuildInfo() : c,once,entrypoint=&apos;slGetBuildInfo&apos;;
  string Data2String(const data src) : c,pure,entrypoint=&apos;slData2String&apos;;
  data String2Data(const string src) : c,pure,entrypoint=&apos;slString2Data&apos;;
  string StringToLowerCase(const string src) : c,pure,entrypoint=&apos;slStringToLowerCase&apos;;
  string StringToUpperCase(const string src) : c,pure,entrypoint=&apos;slStringToUpperCase&apos;;
  string StringToProperCase(const string src) : c,pure,entrypoint=&apos;slStringToProperCase&apos;;
  string StringToCapitalCase(const string src) : c,pure,entrypoint=&apos;slStringToCapitalCase&apos;;
  string StringToTitleCase(const string src) : c,pure,entrypoint=&apos;slStringToTitleCase&apos;;
  integer4 StringCompareIgnoreCase(const string src1, const string src2) : c,pure,entrypoint=&apos;slStringCompareIgnoreCase&apos;;
  string StringReverse(const string src) : c,pure,entrypoint=&apos;slStringReverse&apos;;
  string StringFindReplace(const string src, const string stok, const string rtok) : c,pure,entrypoint=&apos;slStringFindReplace&apos;;
  string StringCleanSpaces(const string src) : c,pure,entrypoint=&apos;slStringCleanSpaces&apos;;
  boolean StringWildMatch(const string src, const string _pattern, boolean _noCase) : c, pure,entrypoint=&apos;slStringWildMatch&apos;;
  boolean StringWildExactMatch(const string src, const string _pattern, boolean _noCase) : c, pure,entrypoint=&apos;slStringWildExactMatch&apos;;
  boolean StringContains(const string src, const string _pattern, boolean _noCase) : c, pure,entrypoint=&apos;slStringContains&apos;;
  string StringExtractMultiple(const string src, unsigned8 mask) : c,pure,entrypoint=&apos;slStringExtractMultiple&apos;;
  unsigned integer4 EditDistance(const string l, const string r) : c, time, pure,entrypoint=&apos;slEditDistanceV2&apos;;
  boolean EditDistanceWithinRadius(const string l, const string r, unsigned4 radius) : c,time,pure,entrypoint=&apos;slEditDistanceWithinRadiusV2&apos;;
  unsigned integer4 EditDistanceV2(const string l, const string r) : c,time,pure,entrypoint=&apos;slEditDistanceV2&apos;;
  unsigned integer4 EditDistanceV3(const string l, const string r, unsigned4 radius) : c,time,pure,entrypoint=&apos;slEditDistanceV3&apos;;
  boolean EditDistanceWithinRadiusV2(const string l, const string r, unsigned4 radius) : c,time,pure,entrypoint=&apos;slEditDistanceWithinRadiusV2&apos;;
  string StringGetNthWord(const string src, unsigned4 n) : c, pure,entrypoint=&apos;slStringGetNthWord&apos;;
  string StringExcludeLastWord(const string src) : c, pure,entrypoint=&apos;slStringExcludeLastWord&apos;;
  string StringExcludeNthWord(const string src, unsigned4 n) : c, pure,entrypoint=&apos;slStringExcludeNthWord&apos;;
  unsigned4 StringWordCount(const string src) : c, pure,entrypoint=&apos;slStringWordCount&apos;;
  unsigned4 CountWords(const string src, const string _separator, BOOLEAN allow_blanks) : c, pure,entrypoint=&apos;slCountWords&apos;;
  SET OF STRING SplitWords(const string src, const string _separator, BOOLEAN allow_blanks) : c, pure,entrypoint=&apos;slSplitWords&apos;;
  STRING CombineWords(set of string src, const string _separator) : c, pure,entrypoint=&apos;slCombineWords&apos;;
  UNSIGNED4 StringToDate(const string src, const varstring format) : c, pure,entrypoint=&apos;slStringToDate&apos;;
  UNSIGNED4 StringToTimeOfDay(const string src, const varstring format) : c, pure,entrypoint=&apos;slStringToTimeOfDay&apos;;
  UNSIGNED4 MatchDate(const string src, set of varstring formats) : c, pure,entrypoint=&apos;slMatchDate&apos;;
  UNSIGNED4 MatchTimeOfDay(const string src, set of varstring formats) : c, pure,entrypoint=&apos;slMatchTimeOfDay&apos;;
  STRING FormatDate(UNSIGNED4 date, const varstring format) : c, pure,entrypoint=&apos;slFormatDate&apos;;
  STRING StringRepeat(const string src, unsigned4 n) : c, pure,entrypoint=&apos;slStringRepeat&apos;;
END;</Text>
 </Module>
 <Option name="eclcc_compiler_version" value="7.6.19"/>
</Archive>
