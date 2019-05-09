/***
 * Function macro for profiling all or part of a dataset.  The output is a
 * dataset containing the following information for each profiled attribute:
 *
 *      attribute               The name of the attribute
 *      given_attribute_type    The ECL type of the attribute as it was defined
 *                              in the input dataset
 *      best_attribute_type     And ECL data type that both allows all values
 *                              in the input dataset and consumes the least
 *                              amount of memory
 *      rec_count               The number of records analyzed in the dataset;
 *                              this may be fewer than the total number of
 *                              records, if the optional sampleSize argument
 *                              was provided with a value less than 100
 *      fill_count              The number of rec_count records containing
 *                              non-nil values
 *      fill_rate               The percentage of rec_count records containing
 *                              non-nil values; a 'nil value' is an empty
 *                              string or a numeric zero; note that BOOLEAN
 *                              attributes are always counted as filled,
 *                              regardless of their value; also, fixed-length
 *                              DATA attributes (e.g. DATA10) are also counted
 *                              as filled, given their typical function of
 *                              holding data blobs
 *      cardinality             The number of unique, non-nil values within
 *                              the attribute
 *      cardinality_breakdown   For those attributes with a low number of
 *                              unique, non-nil values, show each value and the
 *                              number of records containing that value; the
 *                              lcbLimit parameter governs what "low number"
 *                              means
 *      modes                   The most common values in the attribute, after
 *                              coercing all values to STRING, along with the
 *                              number of records in which the values were
 *                              found; if no value is repeated more than once
 *                              then no mode will be shown; up to five (5)
 *                              modes will be shown; note that string values
 *                              longer than the maxPatternLen argument will
 *                              be truncated
 *      min_length              The shortest length of a value when expressed
 *                              as a string; null values are ignored
 *      max_length              The longest length of a value when expressed
 *                              as a string
 *      ave_length              The average length of a value when expressed
 *                              as a string
 *      popular_patterns        The most common patterns of values; see below
 *      rare_patterns           The least common patterns of values; see below
 *      is_numeric              Boolean indicating if the original attribute
 *                              was numeric and therefore whether or not
 *                              the numeric_xxxx output fields will be
 *                              populated with actual values; if this value
 *                              is FALSE then all numeric_xxxx output values
 *                              should be ignored
 *      numeric_min             The smallest non-nil value found within the
 *                              attribute as a DECIMAL; the attribute must be
 *                              a numeric ECL datatype; non-numeric attributes
 *                              will return zero
 *      numeric_max             The largest non-nil value found within the
 *                              attribute as a DECIMAL; the attribute must be
 *                              a numeric ECL datatype; non-numeric attributes
 *                              will return zero
 *      numeric_mean            The mean (average) non-nil value found within
 *                              the attribute as a DECIMAL; the attribute must
 *                              be a numeric ECL datatype; non-numeric
 *                              attributes will return zero
 *      numeric_std_dev         The standard deviation of the non-nil values
 *                              in the attribute as a DECIMAL; the attribute
 *                              must be a numeric ECL datatype; non-numeric
 *                              attributes will return zero
 *      numeric_lower_quartile  The value separating the first (bottom) and
 *                              second quarters of non-nil values within
 *                              the attribute as a DECIMAL; the attribute must
 *                              be a numeric ECL datatype; non-numeric
 *                              attributes will return zero
 *      numeric_median          The median non-nil value within the attribute
 *                              as a DECIMAL; the attribute must be a numeric
 *                              ECL datatype; non-numeric attributes will return
 *                              zero
 *      numeric_upper_quartile  The value separating the third and fourth
 *                              (top) quarters of non-nil values within
 *                              the attribute as a DECIMAL; the attribute must
 *                              be a numeric ECL datatype; non-numeric
 *                              attributes will return zero
 *      numeric_correlations    A child dataset containing correlation values
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
 * Most profile outputs can be disabled.  See the 'features' argument, below.
 *
 * Data patterns can give you an idea of what your data looks like when it is
 * expressed as a (human-readable) string.  The function converts each
 * character of the string into a fixed character palette to producing a "data
 * pattern" and then counts the number of unique patterns for that attribute.
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
 * Child datasets and SET data types (such as SET OF INTEGER) are not
 * supported.  If the input dataset cannot be processed then an error will be
 * produced at compile time.
 *
 * This function works best when the incoming dataset contains attributes that
 * have precise data types (e.g. UNSIGNED4 data types instead of numbers
 * stored in a STRING data type).
 *
 * Function parameters:
 *
 * @param   inFile          The dataset to process; REQUIRED
 * @param   fieldListStr    A string containing a comma-delimited list of
 *                          attribute names to process; use an empty string to
 *                          process all attributes in inFile; attributes named
 *                          here that are not found in the top level of inFile
 *                          will be ignored; OPTIONAL, defaults to an
 *                          empty string
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
 *                              correlations            numeric_correlations
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
 * @param   lcbLimit        A positive integer (<= 500) indicating the maximum
 *                          cardinality allowed for an attribute in order to
 *                          emit a breakdown of the attribute's values; this
 *                          parameter will be ignored if cardinality_breakdown
 *                          is not included in the features argument; OPTIONAL,
 *                          defaults to 64
 */
EXPORT Profile(inFile,
               fieldListStr = '\'\'',
               maxPatterns = 100,
               maxPatternLen = 100,
               features = '\'fill_rate,best_ecl_types,cardinality,cardinality_breakdown,modes,lengths,patterns,min_max,mean,std_dev,quartiles,correlations\'',
               sampleSize = 100,
               lcbLimit = 64) := FUNCTIONMACRO
    LOADXML('<xml/>');
    #EXPORTXML(inFileFields, RECORDOF(inFile));
    #UNIQUENAME(recLevel);                          // Will be used to determine at which level we are processing
    #UNIQUENAME(fieldStack);                        // String-based stack telling us whether we're within an embedded dataset or record
    #SET(fieldStack, '');
    #UNIQUENAME(namePrefix);
    #UNIQUENAME(needsDelim);                        // Boolean indicating whether we need to insert a delimiter somewhere
    #UNIQUENAME(needsDelim2);                       // Another boolean indicating whether we need to insert a delimiter somewhere
    #UNIQUENAME(attributeSize);                     // Will become the length of the longest attribute name we will be processing
    #SET(attributeSize, 0);
    #UNIQUENAME(foundMaxPatternLen);                // Will become the length of the longest pattern we will be processing
    #SET(foundMaxPatternLen, 33);                   // Preset to minimum length for an attribute pattern
    #UNIQUENAME(explicitFields);                    // Attributes from fieldListStr that are found in the top level of the dataset
    #SET(explicitFields, '');
    #UNIQUENAME(numericFields);                     // Numeric attributes from fieldListStr that are found in the top level of the dataset
    #SET(numericFields, '');
    #UNIQUENAME(fieldCount);                        // Used primarily to enable syntax checking of code when dynamic record lookup is enabled

    // Remove all spaces from field list so we can parse it more easily
    LOCAL trimmedFieldList := TRIM(fieldListStr, ALL);
    // Remove all spaces from features list so we can parse it more easily
    LOCAL trimmedFeatures := TRIM(features, ALL);
    // The maximum number of mode values to return
    LOCAL MAX_MODES := 5;
    // Clamp lcbLimit to 0..500
    LOCAL lowCardinalityThreshold := MIN(MAX(lcbLimit, 0), 500);

    // Validate that attribute is okay for us to process (not a SET OF XXX
    // data type, and that either there is no explicit attribute list or the
    // name is in the list)
    LOCAL CanProcessAttribute(STRING attrName, STRING attrType) := (attrType[..7] != 'set of ' AND (trimmedFieldList = '' OR REGEXFIND('(^|,)' + attrName + '(,|$)', trimmedFieldList, NOCASE)));

    // Helper function to convert a full field name into somthing we
    // can reference as an ECL attribute
    LOCAL MakeAttr(STRING attr) := REGEXREPLACE('\\.', attr, '_');

    // Useful functions for pattern mapping
    LOCAL MapUpperCharStr(STRING s) := REGEXREPLACE('[[:upper:]]', s, 'A');
    LOCAL MapLowerCharStr(STRING s) := REGEXREPLACE('[[:lower:]]', s, 'a');
    LOCAL MapDigitStr(STRING s) := REGEXREPLACE('[[:digit:]]', s, '9');
    LOCAL MapAllStr(STRING s) := MapDigitStr(MapLowerCharStr(MapUpperCharStr(s)));

    LOCAL MapUpperCharUni(UNICODE s) := REGEXREPLACE(u'[[:upper:]]', s, u'A');
    LOCAL MapLowerCharUni(UNICODE s) := REGEXREPLACE(u'[[:lower:]]', s, u'a');
    LOCAL MapDigitUni(UNICODE s) := REGEXREPLACE(u'[[:digit:]]', s, u'9');
    LOCAL MapAllUni(UNICODE s) := (STRING)MapDigitUni(MapLowerCharUni(MapUpperCharUni(s)));

    LOCAL TrimmedStr(STRING s) := TRIM(s, LEFT, RIGHT);
    LOCAL TrimmedUni(UNICODE s) := TRIM(s, LEFT, RIGHT);

    // Tests for enabled features
    LOCAL FeatureEnabledFillRate() := REGEXFIND('\\bfill_rate\\b', trimmedFeatures, NOCASE);
    LOCAL FeatureEnabledBestECLTypes() := REGEXFIND('\\bbest_ecl_types\\b', trimmedFeatures, NOCASE);
    LOCAL FeatureEnabledLowCardinalityBreakdown() := lowCardinalityThreshold > 0 AND REGEXFIND('\\bcardinality_breakdown\\b', trimmedFeatures, NOCASE);
    LOCAL FeatureEnabledCardinality() := FeatureEnabledLowCardinalityBreakdown() OR REGEXFIND('\\bcardinality\\b', trimmedFeatures, NOCASE);
    LOCAL FeatureEnabledModes() := REGEXFIND('\\bmodes\\b', trimmedFeatures, NOCASE);
    LOCAL FeatureEnabledLengths() := REGEXFIND('\\blengths\\b', trimmedFeatures, NOCASE);
    LOCAL FeatureEnabledPatterns() := REGEXFIND('\\bpatterns\\b', trimmedFeatures, NOCASE);
    LOCAL FeatureEnabledMinMax() := REGEXFIND('\\bmin_max\\b', trimmedFeatures, NOCASE);
    LOCAL FeatureEnabledMean() := REGEXFIND('\\bmean\\b', trimmedFeatures, NOCASE);
    LOCAL FeatureEnabledStdDev() := REGEXFIND('\\bstd_dev\\b', trimmedFeatures, NOCASE);
    LOCAL FeatureEnabledQuartiles() := REGEXFIND('\\bquartiles\\b', trimmedFeatures, NOCASE);
    LOCAL FeatureEnabledCorrelations() := REGEXFIND('\\bcorrelations\\b', trimmedFeatures, NOCASE);

    // Determine the maximum length of an attribute name that we will be
    // processing; this will be used to determine the length of the fixed-width
    // string datatype used to store the attribute name; along the way, collect
    // a list of the top-level attributes that we can process and the also
    // determine the actual maximum length of a data pattern (if we can reduce
    // that length then we can save on memory allocation); while we're at it,
    // collect the numeric fields for correlation
    #SET(needsDelim, 0);
    #SET(needsDelim2, 0);
    #SET(recLevel, 0);
    #SET(fieldStack, '');
    #SET(namePrefix, '');
    #SET(fieldCount, 0);
    #FOR(inFileFields)
        #FOR(Field)
            #SET(fieldCount, %fieldCount% + 1);
            #IF(%{@isRecord}% = 1)
                #SET(fieldStack, 'r' + %'fieldStack'%)
                #APPEND(namePrefix, %'@name'% + '.')
            #ELSEIF(%{@isDataset}% = 1)
                #SET(fieldStack, 'd' + %'fieldStack'%)
                #SET(recLevel, %recLevel% + 1)
            #ELSEIF(%{@isEnd}% = 1)
                #IF(%'fieldStack'%[1] = 'd')
                    #SET(recLevel, %recLevel% - 1)
                #ELSE
                    #SET(namePrefix, REGEXREPLACE('\\w+\\.$', %'namePrefix'%, ''))
                #END
                #SET(fieldStack, %'fieldStack'%[2..])
            #ELSEIF(%recLevel% = 0)
                #IF(CanProcessAttribute(%'namePrefix'% + %'@name'%, %'@type'%))
                    #SET(attributeSize, MAX(%attributeSize%, LENGTH(%'namePrefix'% + %'@name'%)))

                    #IF(%needsDelim% = 1)
                        #APPEND(explicitFields, ',')
                    #END
                    #APPEND(explicitFields, %'namePrefix'% + %'@name'%)
                    #SET(needsDelim, 1)

                    #IF(REGEXFIND('(string)|(data)|(utf)', %'@type'%))
                        #IF(%@size% < 0)
                            #SET(foundMaxPatternLen, MAX(maxPatternLen, %foundMaxPatternLen%))
                        #ELSE
                            #SET(foundMaxPatternLen, MIN(MAX(%@size%, %foundMaxPatternLen%), maxPatternLen))
                        #END
                    #ELSEIF(REGEXFIND('unicode', %'@type'%))
                        // Unicode is UTF-16 so the size reflects two bytes per character
                        #IF(%@size% < 0)
                            #SET(foundMaxPatternLen, MAX(maxPatternLen, %foundMaxPatternLen%))
                        #ELSE
                            #SET(foundMaxPatternLen, MIN(MAX(%@size% DIV 2 + 1, %foundMaxPatternLen%), maxPatternLen))
                        #END
                    #ELSEIF(REGEXFIND('(integer)|(unsigned)|(decimal)|(real)', %'@type'%))
                        #IF(%needsDelim2% = 1)
                            #APPEND(numericFields, ',')
                        #END
                        #APPEND(numericFields, %'namePrefix'% + %'@name'%)
                        #SET(needsDelim2, 1)
                    #END
                #END
            #END
        #END
    #END
    // Error check:  If attributeSize is still zero then we don't have any
    // attributes to process
    #IF(%fieldCount% > 0 AND %attributeSize% = 0)
        #ERROR('No valid top-level record attributes to process')
    #END

    // Typedefs
    LOCAL Attribute_t := #EXPAND('STRING' + %'attributeSize'%);
    LOCAL DataPattern_t := #EXPAND('STRING' + %'foundMaxPatternLen'%);
    LOCAL StringValue_t := #EXPAND('STRING' + %'foundMaxPatternLen'%);
    LOCAL AttributeType_t := STRING36;
    LOCAL NumericStat_t := DECIMAL32_4;

    // Ungroup the given dataset, in case it was grouped
    LOCAL ungroupedInFile := UNGROUP(inFile);

    // Clamp the sample size to something reasonable
    LOCAL clampedSampleSize := MAP
        (
            (INTEGER)sampleSize < 1     =>  1,
            (INTEGER)sampleSize > 100   =>  100,
            (INTEGER)sampleSize
        );

    // Create a sample dataset if needed
    LOCAL sampledData := IF
        (
            clampedSampleSize < 100,
            ENTH(ungroupedInFile, clampedSampleSize, 100, 1, LOCAL),
            ungroupedInFile
        );

    // Slim the dataset to distribute if the caller provided an explicit
    // set of attributes
    LOCAL workingInFile :=
        #IF(fieldListStr = '')
            sampledData
        #ELSE
            TABLE(sampledData, {%explicitFields%})
        #END;

    // Distribute the inbound dataset across all our nodes for faster processing
    LOCAL distributedInFile := DISTRIBUTE(workingInFile, SKEW(0.05));

    // Create a dataset containing pattern information, string length, and
    // booleans indicating filled and numeric datatypes for each processed
    // attribute; note that this is created by appending a series of PROJECT
    // results; to protect against skew problems when dealing with attributes
    // with low cardinality, and to attempt to reduce our temporary storage
    // footprint, create a reduced dataset that contains unique values for
    // our attributes and the number of times the values appear, as well as
    // some of the other interesting bits we can collect at the same time; note
    // that we try to explicitly target the original attribute's data type and
    // perform the minimal amount of work necessary on the value to transform
    // it to our common structure

    LOCAL DataInfoRec := RECORD
        Attribute_t         attribute;
        AttributeType_t     given_attribute_type;
        StringValue_t       string_value;
        UNSIGNED4           value_count;
        DataPattern_t       data_pattern;
        UNSIGNED4           data_length;
        BOOLEAN             is_filled;
        BOOLEAN             is_number;
    END;

    LOCAL dataInfo :=
        #SET(recLevel, 0)
        #SET(fieldStack, '')
        #SET(namePrefix, '')
        #SET(needsDelim, 0)
        #SET(fieldCount, 0)
        #FOR(inFileFields)
            #FOR(Field)
                #IF(%{@isRecord}% = 1)
                    #SET(fieldStack, 'r' + %'fieldStack'%)
                    #APPEND(namePrefix, %'@name'% + '.')
                #ELSEIF(%{@isDataset}% = 1)
                    #SET(fieldStack, 'd' + %'fieldStack'%)
                    #SET(recLevel, %recLevel% + 1)
                #ELSEIF(%{@isEnd}% = 1)
                    #IF(%'fieldStack'%[1] = 'd')
                        #SET(recLevel, %recLevel% - 1)
                    #ELSE
                        #SET(namePrefix, REGEXREPLACE('\\w+\\.$', %'namePrefix'%, ''))
                    #END
                    #SET(fieldStack, %'fieldStack'%[2..])
                #ELSEIF(%recLevel% = 0)
                    #IF(CanProcessAttribute(%'namePrefix'% + %'@name'%, %'@type'%))
                        #SET(fieldCount, %fieldCount% + 1)
                        #IF(%needsDelim% = 1) + #END

                        PROJECT
                            (
                                TABLE
                                    (
                                        distributedInFile,
                                        {
                                            Attribute_t     attribute := %'namePrefix'% + %'@name'%,
                                            AttributeType_t given_attribute_type := %'@ecltype'%,
                                            StringValue_t   string_value :=
                                                                #IF(REGEXFIND('(integer)|(unsigned)|(decimal)|(real)|(boolean)', %'@type'%))
                                                                    (StringValue_t)distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%)
                                                                #ELSEIF(REGEXFIND('string', %'@type'%))
                                                                    TrimmedStr(distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%))
                                                                #ELSE
                                                                    TrimmedStr((StringValue_t)distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%))
                                                                #END,
                                            UNSIGNED4       value_count := COUNT(GROUP),
                                            DataPattern_t   data_pattern :=
                                                                #IF(REGEXFIND('(integer)|(unsigned)|(decimal)|(real)', %'@type'%))
                                                                    MapAllStr((STRING)distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%))
                                                                #ELSEIF(REGEXFIND('(unicode)|(utf)', %'@type'%))
                                                                    #IF(%@size% < 0 OR (%@size% DIV 2 + 1) > %foundMaxPatternLen%)
                                                                        MapAllUni(TrimmedUni((UNICODE)distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%))[..%foundMaxPatternLen%])
                                                                    #ELSE
                                                                        MapAllUni(TrimmedUni((UNICODE)distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%)))
                                                                    #END
                                                                #ELSEIF(REGEXFIND('string', %'@type'%))
                                                                    #IF(%@size% < 0 OR %@size% > %foundMaxPatternLen%)
                                                                        MapAllStr(TrimmedStr(distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%))[..%foundMaxPatternLen%])
                                                                    #ELSE
                                                                        MapAllStr(TrimmedStr(distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%)))
                                                                    #END
                                                                #ELSEIF(%'@type'% = 'boolean')
                                                                    'B'
                                                                #ELSE
                                                                    MapAllStr(TrimmedStr((STRING)distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%))[..%foundMaxPatternLen%])
                                                                #END,
                                            UNSIGNED4       data_length :=
                                                                #IF(REGEXFIND('(unicode)|(utf)', %'@type'%))
                                                                    LENGTH(TrimmedUni((UNICODE)distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%)))
                                                                #ELSEIF(REGEXFIND('string', %'@type'%))
                                                                    LENGTH(TrimmedStr(distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%)))
                                                                #ELSEIF(%'@type'% = 'boolean')
                                                                    1
                                                                #ELSE
                                                                    LENGTH((STRING)distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%))
                                                                #END,
                                            BOOLEAN         is_filled :=
                                                                #IF(REGEXFIND('(unicode)|(utf)', %'@type'%))
                                                                    LENGTH(TrimmedUni(distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%))) > 0
                                                                #ELSEIF(REGEXFIND('string', %'@type'%))
                                                                    LENGTH(TrimmedStr(distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%))) > 0
                                                                #ELSEIF(REGEXFIND('data', %'@type'%))
                                                                    LENGTH(distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%)) > 0
                                                                #ELSEIF(%'@type'% = 'boolean')
                                                                    TRUE
                                                                #ELSE
                                                                    distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%) != 0
                                                                #END,
                                            BOOLEAN         is_number :=
                                                                #IF(REGEXFIND('(integer)|(unsigned)|(decimal)|(real)', %'@type'%))
                                                                    TRUE
                                                                #ELSE
                                                                    FALSE
                                                                #END
                                        },
                                        distributedInFile.#EXPAND(%'namePrefix'% + %'@name'%),
                                        LOCAL
                                    ),
                                    TRANSFORM(DataInfoRec, SELF := LEFT)
                            )

                        #SET(needsDelim, 1)
                    #END
                #END
            #END
        #END

        // Insert empty value for syntax checking
        #IF(%fieldCount% = 0)
            DATASET([], DataInfoRec)
        #END;

    // Get only those attributes that are filled
    filledDataInfo := dataInfo(is_filled);

    // Determine the best ECL data type for each attribute
    LOCAL DataTypeEnum := ENUM
        (
            UNSIGNED4,
                AsIs = 0,
                SignedInteger = 1,
                UnsignedInteger = 2,
                FloatingPoint = 4,
                ExpNotation = 8
        );

    LOCAL DataTypeEnum BestTypeFlag(STRING dataPattern) := FUNCTION
        isSignedInteger := REGEXFIND('^\\-9{1,19}$', dataPattern);
        isShortUnsignedInteger := REGEXFIND('^9{1,19}$', dataPattern);
        isUnsignedInteger := REGEXFIND('^\\+?9{1,20}$', dataPattern);
        isFloatingPoint := REGEXFIND('^(\\-|\\+)?9{0,15}\\.9{1,15}$', dataPattern);
        isExpNotation := REGEXFIND('^(\\-|\\+)?9\\.9{1,6}a\\-9{1,3}$', dataPattern, NOCASE);

        RETURN MAP
            (
                isSignedInteger         =>  DataTypeEnum.SignedInteger | DataTypeEnum.FloatingPoint | DataTypeEnum.ExpNotation,
                isShortUnsignedInteger  =>  DataTypeEnum.SignedInteger | DataTypeEnum.UnsignedInteger | DataTypeEnum.FloatingPoint | DataTypeEnum.ExpNotation,
                isUnsignedInteger       =>  DataTypeEnum.UnsignedInteger | DataTypeEnum.FloatingPoint | DataTypeEnum.ExpNotation,
                isFloatingPoint         =>  DataTypeEnum.FloatingPoint | DataTypeEnum.ExpNotation,
                isExpNotation           =>  DataTypeEnum.ExpNotation,
                DataTypeEnum.AsIs
            );
    END;

    // Estimate integer size from readable data length
    LOCAL Len2Size(UNSIGNED2 c) := MAP ( c < 3 => 1, c < 5 => 2, c < 7 => 3, c < 9 => 4, c < 11 => 5, c < 14 => 6, c < 16 => 7, 8 );

    LOCAL attributeTypePatterns := TABLE
        (
            filledDataInfo,
            {
                attribute,
                given_attribute_type,
                data_pattern,
                data_length,
                DataTypeEnum    type_flag := BestTypeFlag(TRIM(data_pattern))

            },
            attribute, given_attribute_type, data_pattern, data_length,
            MERGE
        );

    LOCAL attributesWithTypeFlagsSummary := AGGREGATE
        (
            attributeTypePatterns,
            RECORDOF(attributeTypePatterns),
            TRANSFORM
                (
                    RECORDOF(attributeTypePatterns),
                    SELF.data_length := MAX(LEFT.data_length, RIGHT.data_length) ,
                    SELF.type_flag := IF(TRIM(RIGHT.attribute) != '', LEFT.type_flag & RIGHT.type_flag, LEFT.type_flag),
                    SELF := LEFT
                ),
            TRANSFORM
                (
                    RECORDOF(attributeTypePatterns),
                    SELF.data_length := MAX(RIGHT1.data_length, RIGHT2.data_length),
                    SELF.type_flag := RIGHT1.type_flag & RIGHT2.type_flag,
                    SELF := RIGHT1
                ),
            LEFT.attribute,
            FEW
        );

    LOCAL AttributeTypeRec := RECORD
        Attribute_t     attribute;
        AttributeType_t given_attribute_type;
        AttributeType_t best_attribute_type;
    END;

    LOCAL attributeBestTypeInfo := PROJECT
        (
            attributesWithTypeFlagsSummary,
            TRANSFORM
                (
                    AttributeTypeRec,
                    SELF.best_attribute_type := MAP
                        (
                            REGEXFIND('(integer)|(unsigned)|(decimal)|(real)|(boolean)', LEFT.given_attribute_type) =>  LEFT.given_attribute_type,
                            REGEXFIND('data', LEFT.given_attribute_type)                                            =>  'data' + IF(LEFT.data_length > 0, (STRING)LEFT.data_length, ''),
                            (LEFT.type_flag & DataTypeEnum.UnsignedInteger) != 0                                    =>  'unsigned' + Len2Size(LEFT.data_length),
                            (LEFT.type_flag & DataTypeEnum.SignedInteger) != 0                                      =>  'integer' + Len2Size(LEFT.data_length),
                            (LEFT.type_flag & DataTypeEnum.FloatingPoint) != 0                                      =>  'real' + IF(LEFT.data_length < 8, '4', '8'),
                            (LEFT.type_flag & DataTypeEnum.ExpNotation) != 0                                        =>  'real8',
                            REGEXFIND('utf', LEFT.given_attribute_type)                                             =>  LEFT.given_attribute_type,
                            REGEXREPLACE('\\d+$', TRIM(LEFT.given_attribute_type), '') + IF(LEFT.data_length > 0, (STRING)LEFT.data_length, '')
                        ),
                    SELF := LEFT
                )
        );

    // Record definition for mode values that we'll be returning
    LOCAL ModeRec := RECORD
        STRING          value;
        UNSIGNED4       rec_count;
    END;

    // Build a set of attributes for quartiles, unique values, and modes for
    // each processed attribute
    #SET(recLevel, 0);
    #SET(fieldStack, '');
    #SET(namePrefix, '');
    #FOR(inFileFields)
        #FOR(Field)
            #IF(%{@isRecord}% = 1)
                #SET(fieldStack, 'r' + %'fieldStack'%)
                #APPEND(namePrefix, %'@name'% + '.')
            #ELSEIF(%{@isDataset}% = 1)
                #SET(fieldStack, 'd' + %'fieldStack'%)
                #SET(recLevel, %recLevel% + 1)
            #ELSEIF(%{@isEnd}% = 1)
                #IF(%'fieldStack'%[1] = 'd')
                    #SET(recLevel, %recLevel% - 1)
                #ELSE
                    #SET(namePrefix, REGEXREPLACE('\\w+\\.$', %'namePrefix'%, ''))
                #END
                #SET(fieldStack, %'fieldStack'%[2..]);
            #ELSEIF(%recLevel% = 0)
                #IF(CanProcessAttribute(%'namePrefix'% + %'@name'%, %'@type'%))
                    // Note that we create explicit attributes here for all
                    // top-level attributes in the dataset that we're
                    // processing, even if they are not numeric datatypes
                    #UNIQUENAME(uniqueNumericValueCounts)
                    %uniqueNumericValueCounts% := PROJECT
                        (
                            filledDataInfo(attribute = %'namePrefix'% + %'@name'% AND is_number),
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
                    LOCAL #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_min')) := MIN(%uniqueNumericValueCounts%, value);
                    LOCAL #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_max')) := MAX(%uniqueNumericValueCounts%, value);
                    LOCAL #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_ave')) := SUM(%uniqueNumericValueCounts%, value * cnt) / SUM(%uniqueNumericValueCounts%, cnt);
                    LOCAL #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_std_dev')) := SQRT(SUM(%uniqueNumericValueCounts%, (value - #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_ave'))) * (value - #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_ave'))) * cnt) / SUM(%uniqueNumericValueCounts%, cnt));

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
                    LOCAL %q2Value1% := MIN(%uniqueNumericValuePos%(valueEndPos >= %q2Pos1%), value);
                    #UNIQUENAME(q2Pos2);
                    LOCAL %q2Pos2% := %q2Pos1% + ((%wholeNumRecs% + 1) % 2);
                    #UNIQUENAME(q2Value2);
                    LOCAL %q2Value2% := MIN(%uniqueNumericValuePos%(valueEndPos >= %q2Pos2%), value);
                    LOCAL #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_q2_value')) := AVE(%q2Value1%, %q2Value2%);

                    // Find the lower quartile
                    #UNIQUENAME(q1Pos1);
                    LOCAL %q1Pos1% := (%halfNumRecs% DIV 2) + (%halfNumRecs% % 2);
                    #UNIQUENAME(q1Value1);
                    LOCAL %q1Value1% := MIN(%uniqueNumericValuePos%(valueEndPos >= %q1Pos1%), value);
                    #UNIQUENAME(q1Pos2);
                    LOCAL %q1Pos2% := %q1Pos1% + ((%halfNumRecs% + 1) % 2);
                    #UNIQUENAME(q1Value2);
                    LOCAL %q1Value2% := MIN(%uniqueNumericValuePos%(valueEndPos >= %q1Pos2%), value);
                    LOCAL #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_q1_value')) := IF(%halfNumRecs% > 0, AVE(%q1Value1%, %q1Value2%), 0);

                    // Find the upper quartile
                    #UNIQUENAME(q3Pos1);
                    LOCAL %q3Pos1% := MAX(%q2Pos1%, %q2Pos2%) + (%halfNumRecs% DIV 2) + (%halfNumRecs% % 2);
                    #UNIQUENAME(q3Value1);
                    LOCAL %q3Value1% := MIN(%uniqueNumericValuePos%(valueEndPos >= %q3Pos1%), value);
                    #UNIQUENAME(q3Pos2);
                    LOCAL %q3Pos2% := %q3Pos1% - ((%halfNumRecs% + 1) % 2);
                    #UNIQUENAME(q3Value2);
                    LOCAL %q3Value2% := MIN(%uniqueNumericValuePos%(valueEndPos >= %q3Pos2%), value);
                    LOCAL #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_q3_value')) := IF(%halfNumRecs% > 0, AVE(%q3Value1%, %q3Value2%), 0);

                    // Derive all unique data values and the number of times
                    // each occurs in the data
                    LOCAL #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_uniq_value_recs')) := TABLE
                        (
                            filledDataInfo(attribute = %'namePrefix'% + %'@name'%),
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
                    // effectively limit the final result to MAX_MODES records
                    #UNIQUENAME(topRecords);
                    %topRecords% := TOPN(#EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_uniq_value_recs')), MAX_MODES, -rec_count);
                    #UNIQUENAME(topRecord)
                    %topRecord% := TOPN(%topRecords%, 1, -rec_count);
                    LOCAL #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_mode_values')) := JOIN
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
                    LOCAL #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_lcb_recs')) := IF
                        (
                            COUNT(#EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_uniq_value_recs'))) <= lowCardinalityThreshold,
                            PROJECT
                                (
                                    SORT(#EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_uniq_value_recs')), -rec_count),
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

    LOCAL BaseCorrelationLayout := RECORD
        Attribute_t     attribute_x;
        Attribute_t     attribute_y;
        REAL            corr;
    END;

    #UNIQUENAME(corrNamePosX);
    #UNIQUENAME(corrNamePosY);
    #UNIQUENAME(fieldX);
    #UNIQUENAME(fieldY);
    #SET(needsDelim, 0);

    LOCAL correlations0 := DATASET
        (
            [
                #SET(corrNamePosX, 1)
                #LOOP
                    #SET(fieldX, REGEXFIND('^([^,]+)', %'numericFields'%[%corrNamePosX%..], 1))
                    #IF(%'fieldX'% != '')
                        #SET(corrNamePosY, %corrNamePosX% + LENGTH(%'fieldX'%) + 1)
                        #LOOP
                            #SET(fieldY, REGEXFIND('^([^,]+)', %'numericFields'%[%corrNamePosY%..], 1))
                            #IF(%'fieldY'% != '')
                                #IF(%needsDelim% = 1) , #END
                                {
                                    %'fieldX'%,
                                    %'fieldY'%,
                                    CORRELATION(distributedInFile, %fieldX%, %fieldY%)
                                }
                                #SET(needsDelim, 1)

                                #SET(corrNamePosY, %corrNamePosY% + LENGTH(%'fieldY'%) + 1)
                            #ELSE
                                #BREAK
                            #END
                        #END
                        #SET(corrNamePosX, %corrNamePosX% + LENGTH(%'fieldX'%) + 1)
                    #ELSE
                        #BREAK
                    #END
                #END
            ],
            BaseCorrelationLayout
        );

    // Append a duplicate of the correlations to itself with the X and Y fields
    // reversed so we can easily merge results on a per-attribute basis later
    LOCAL correlations := correlations0 + PROJECT
        (
            correlations0,
            TRANSFORM
                (
                    RECORDOF(LEFT),
                    SELF.attribute_x := LEFT.attribute_y,
                    SELF.attribute_y := LEFT.attribute_x,
                    SELF := LEFT
                )
        );

    //--------------------------------------------------------------------------
    // Collect individual stats for each attribute; these are grouped by the
    // criteria used to group them
    //--------------------------------------------------------------------------

    // Count data patterns used per attribute; extract the most common and
    // most rare, taking care to not allow the two to overlap
    LOCAL dataPatternStats := TABLE
        (
            DISTRIBUTE(filledDataInfo, HASH32(attribute)),
            {
                attribute,
                data_pattern,
                STRING      example := string_value[..%foundMaxPatternLen%],
                UNSIGNED4   rec_count := SUM(GROUP, value_count)
            },
            attribute, data_pattern,
            LOCAL
        ) : ONWARNING(2168, IGNORE);
    LOCAL groupedDataPatterns := GROUP(SORT(dataPatternStats, attribute, LOCAL), attribute, LOCAL);
    LOCAL topDataPatterns := UNGROUP(TOPN(groupedDataPatterns, maxPatterns, -rec_count, data_pattern));
    LOCAL rareDataPatterns0 := UNGROUP(TOPN(groupedDataPatterns, maxPatterns, rec_count, data_pattern));
    LOCAL rareDataPatterns := JOIN
        (
            rareDataPatterns0,
            topDataPatterns,
            LEFT.attribute = RIGHT.attribute AND LEFT.data_pattern = RIGHT.data_pattern,
            TRANSFORM(LEFT),
            LEFT ONLY
        ) : ONWARNING(4531, IGNORE);

    // Find min, max and average data lengths per attribute
    LOCAL dataLengthStats := TABLE
        (
            filledDataInfo,
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
    LOCAL dataFilledStats := TABLE
        (
            dataInfo,
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
    LOCAL cardinalityAndNumerics := DATASET
        (
            [
                #SET(recLevel, 0)
                #SET(fieldStack, '')
                #SET(namePrefix, '')
                #SET(needsDelim, 0)
                #FOR(inFileFields)
                    #FOR(Field)
                        #IF(%{@isRecord}% = 1)
                            #SET(fieldStack, 'r' + %'fieldStack'%)
                            #APPEND(namePrefix, %'@name'% + '.')
                        #ELSEIF(%{@isDataset}% = 1)
                            #SET(fieldStack, 'd' + %'fieldStack'%)
                            #SET(recLevel, %recLevel% + 1)
                        #ELSEIF(%{@isEnd}% = 1)
                            #IF(%'fieldStack'%[1] = 'd')
                                #SET(recLevel, %recLevel% - 1)
                            #ELSE
                                #SET(namePrefix, REGEXREPLACE('\\w+\\.$', %'namePrefix'%, ''))
                            #END
                            #SET(fieldStack, %'fieldStack'%[2..])
                        #ELSEIF(%recLevel% = 0)
                            #IF(CanProcessAttribute(%'namePrefix'% + %'@name'%, %'@type'%))
                                #IF(%needsDelim% = 1) , #END

                                {
                                    %'namePrefix'% + %'@name'%,
                                    #IF(REGEXFIND('(integer)|(unsigned)|(decimal)|(real)', %'@type'%))
                                        TRUE,
                                    #ELSE
                                        FALSE,
                                    #END
                                    #IF(FeatureEnabledCardinality())
                                        COUNT(#EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_uniq_value_recs'))),
                                    #ELSE
                                        0,
                                    #END
                                    #IF(FeatureEnabledMinMax())
                                        #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_min')),
                                        #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_max')),
                                    #ELSE
                                        0,
                                        0,
                                    #END
                                    #IF(FeatureEnabledMean())
                                        #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_ave')),
                                    #ELSE
                                        0,
                                    #END
                                    #IF(FeatureEnabledStdDev())
                                        #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_std_dev')),
                                    #ELSE
                                        0,
                                    #END
                                    #IF(FeatureEnabledQuartiles())
                                        #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_q1_value')),
                                        #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_q2_value')),
                                        #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_q3_value')),
                                    #ELSE
                                        0,
                                        0,
                                        0,
                                    #END
                                    #IF(FeatureEnabledModes())
                                        #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_mode_values'))(rec_count > 1), // Modes must have more than one instance
                                    #ELSE
                                        DATASET([], ModeRec),
                                    #END
                                    #IF(FeatureEnabledLowCardinalityBreakdown())
                                        #EXPAND(MakeAttr(%'namePrefix'% + %'@name'% + '_lcb_recs'))
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
                Attribute_t         attribute,
                BOOLEAN             is_numeric,
                UNSIGNED4           cardinality,
                REAL                numeric_min,
                REAL                numeric_max,
                REAL                numeric_mean,
                REAL                numeric_std_dev,
                REAL                numeric_lower_quartile,
                REAL                numeric_median,
                REAL                numeric_upper_quartile,
                DATASET(ModeRec)    modes {MAXCOUNT(MAX_MODES)};
                DATASET(ModeRec)    cardinality_breakdown;
            }
        );

    //--------------------------------------------------------------------------
    // Collect the individual results into a single output dataset
    //--------------------------------------------------------------------------

    LOCAL PatternCountRec := RECORD
        STRING                      data_pattern;
        UNSIGNED4                   rec_count;
        STRING                      example;
    END;

    LOCAL CorrelationRec := RECORD
        STRING                      attribute;
        DECIMAL7_6                  corr;
    END;

    LOCAL OutputLayout := RECORD
        STRING                      attribute;
        UNSIGNED4                   rec_count;
        STRING                      given_attribute_type;
        DECIMAL9_6                  fill_rate;
        UNSIGNED4                   fill_count;
        UNSIGNED4                   cardinality;
        DATASET(ModeRec)            cardinality_breakdown;
        STRING                      best_attribute_type;
        DATASET(ModeRec)            modes {MAXCOUNT(MAX_MODES)};
        UNSIGNED4                   min_length;
        UNSIGNED4                   max_length;
        UNSIGNED4                   ave_length;
        DATASET(PatternCountRec)    popular_patterns {MAXCOUNT(maxPatterns)};
        DATASET(PatternCountRec)    rare_patterns {MAXCOUNT(maxPatterns)};
        BOOLEAN                     is_numeric;
        NumericStat_t               numeric_min;
        NumericStat_t               numeric_max;
        NumericStat_t               numeric_mean;
        NumericStat_t               numeric_std_dev;
        NumericStat_t               numeric_lower_quartile;
        NumericStat_t               numeric_median;
        NumericStat_t               numeric_upper_quartile;
        DATASET(CorrelationRec)     numeric_correlations;
    END;

    LOCAL final10 := PROJECT
        (
            dataFilledStats,
            TRANSFORM
                (
                    OutputLayout,
                    SELF.attribute := TRIM(LEFT.attribute, RIGHT),
                    SELF.given_attribute_type := TRIM(LEFT.given_attribute_type, RIGHT),
                    SELF.rec_count := LEFT.rec_count,
                    SELF.fill_rate := #IF(FeatureEnabledFillRate()) LEFT.filled_count / LEFT.rec_count * 100 #ELSE 0 #END,
                    SELF.fill_count := #IF(FeatureEnabledFillRate()) LEFT.filled_count #ELSE 0 #END,
                    SELF := []
                )
        );

    LOCAL final15 :=
        #IF(FeatureEnabledBestECLTypes())
            JOIN
                (
                    final10,
                    attributeBestTypeInfo,
                    LEFT.attribute = RIGHT.attribute,
                    TRANSFORM
                        (
                            OutputLayout,
                            SELF.best_attribute_type := IF(TRIM(RIGHT.best_attribute_type, RIGHT) != '', TRIM(RIGHT.best_attribute_type, RIGHT), LEFT.given_attribute_type),
                            SELF := LEFT
                        ),
                    LEFT OUTER
                ) : ONWARNING(4531, IGNORE)
        #ELSE
            final10
        #END;

    LOCAL final20 :=
        #IF(FeatureEnabledLengths())
            JOIN
                (
                    final15,
                    dataLengthStats,
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
            final15
        #END;

    LOCAL final30 :=
        #IF(FeatureEnabledCardinality() OR FeatureEnabledLowCardinalityBreakdown() OR FeatureEnabledMinMax() OR FeatureEnabledMean() OR FeatureEnabledStdDev() OR FeatureEnabledQuartiles() OR FeatureEnabledModes())
            JOIN
                (
                    final20,
                    cardinalityAndNumerics,
                    LEFT.attribute = RIGHT.attribute,
                    TRANSFORM
                        (
                            RECORDOF(LEFT),
                            SELF.attribute := LEFT.attribute,
                            SELF := RIGHT,
                            SELF := LEFT
                        ),
                    KEEP(1), SMART
                ) : ONWARNING(4531, IGNORE)
        #ELSE
            final20
        #END;

    LOCAL final40 :=
        #IF(FeatureEnabledPatterns())
            DENORMALIZE
                (
                    final30,
                    topDataPatterns,
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
            final30
        #END;

    LOCAL final50 :=
        #IF(FeatureEnabledPatterns())
            DENORMALIZE
                (
                    final40,
                    rareDataPatterns,
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
            final40
        #END;

    LOCAL final60 :=
            #IF(FeatureEnabledCorrelations())
                DENORMALIZE
                    (
                        final50,
                        correlations,
                        LEFT.attribute = RIGHT.attribute_x,
                        GROUP,
                        TRANSFORM
                            (
                                RECORDOF(LEFT),
                                SELF.numeric_correlations := SORT
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
                final50
            #END;

    // Create a small dataset that specifies the output order of the named
    // attributes (which should be the same as the input order)
    LOCAL attrOrderDS := DATASET
        (
            [
                #SET(needsDelim, 0)
                #SET(corrNamePosX, 1)
                #SET(fieldY, 1)
                #LOOP
                    #SET(fieldX, REGEXFIND('^([^,]+)', %'explicitFields'%[%corrNamePosX%..], 1))
                    #IF(%'fieldX'% != '')
                        #IF(%needsDelim% = 1) , #END
                        {%fieldY%, %'fieldX'%}
                        #SET(needsDelim, 1)
                        #SET(corrNamePosX, %corrNamePosX% + LENGTH(%'fieldX'%) + 1)
                        #SET(fieldY, %fieldY% + 1)
                    #ELSE
                        #BREAK
                    #END
                #END
            ],
            {
                UNSIGNED2   nameOrder,
                Attribute_t attrName
            }
        );

    // Append the attribute order to the results; we will sort on the order
    // when creating the final output
    LOCAL final70 := JOIN
        (
            final60,
            attrOrderDS,
            LEFT.attribute = RIGHT.attrName
        ) : ONWARNING(4531, IGNORE);

    LOCAL FinalOutputLayout := RECORD
        STRING                          attribute;
        STRING                          given_attribute_type;
        #IF(FeatureEnabledBestECLTypes())
            STRING                      best_attribute_type;
        #END
        UNSIGNED4                       rec_count;
        #IF(FeatureEnabledFillRate())
            UNSIGNED4                   fill_count;
            DECIMAL9_6                  fill_rate;
        #END
        #IF(FeatureEnabledCardinality())
            UNSIGNED4                   cardinality;
        #END
        #IF(FeatureEnabledLowCardinalityBreakdown())
            DATASET(ModeRec)            cardinality_breakdown;
        #END
        #IF(FeatureEnabledModes())
            DATASET(ModeRec)            modes {MAXCOUNT(MAX_MODES)};
        #END
        #IF(FeatureEnabledLengths())
            UNSIGNED4                   min_length;
            UNSIGNED4                   max_length;
            UNSIGNED4                   ave_length;
        #END
        #IF(FeatureEnabledPatterns())
            DATASET(PatternCountRec)    popular_patterns {MAXCOUNT(maxPatterns)};
            DATASET(PatternCountRec)    rare_patterns {MAXCOUNT(maxPatterns)};
        #END
        #IF(FeatureEnabledMinMax() OR FeatureEnabledMean() OR FeatureEnabledStdDev() OR FeatureEnabledQuartiles() OR FeatureEnabledCorrelations())
            BOOLEAN                     is_numeric;
        #END
        #IF(FeatureEnabledMinMax())
            NumericStat_t               numeric_min;
            NumericStat_t               numeric_max;
        #END
        #IF(FeatureEnabledMean())
            NumericStat_t               numeric_mean;
        #END
        #IF(FeatureEnabledStdDev())
            NumericStat_t               numeric_std_dev;
        #END
        #IF(FeatureEnabledQuartiles())
            NumericStat_t               numeric_lower_quartile;
            NumericStat_t               numeric_median;
            NumericStat_t               numeric_upper_quartile;
        #END
        #IF(FeatureEnabledCorrelations())
            DATASET(CorrelationRec)     numeric_correlations;
        #END
    END;

    LOCAL finalData := PROJECT(SORT(final70, nameOrder), TRANSFORM(FinalOutputLayout, SELF := LEFT));

    RETURN finalData;
ENDMACRO;
