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
 *                              non-nil values; a 'nil value' is an empty
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
 * Function parameters:
 *
 * @param   inFile          The dataset to process; this could be a child
 *                          dataset (e.g. inFile.childDS); REQUIRED
 * @param   fieldListStr    A string containing a comma-delimited list of
 *                          attribute names to process; use an empty string to
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
 * @param   lcbLimit        A positive integer (<= 1000) indicating the maximum
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

    #UNIQUENAME(temp);                      // Ubiquitous "contains random things"
    #UNIQUENAME(scalarFields);              // Contains a delimited list of scalar attributes (full names) along with their indexed positions
    #UNIQUENAME(explicitScalarFields);      // Contains a delimited list of scalar attributes (full names) without indexed positions
    #UNIQUENAME(childDSFields);             // Contains a delimited list of child dataset attributes (full names) along with their indexed positions
    #UNIQUENAME(fieldCount);                // Contains the number of fields we've seen while processing record layouts
    #UNIQUENAME(recLevel);                  // Will be used to determine at which level we are processing
    #UNIQUENAME(fieldStack);                // String-based stack telling us whether we're within an embedded dataset or record
    #UNIQUENAME(namePrefix);                // When processing child records and datasets, contains the leading portion of the attribute's full name
    #UNIQUENAME(fullName);                  // The full name of an attribute
    #UNIQUENAME(needsDelim);                // Boolean indicating whether we need to insert a delimiter somewhere
    #UNIQUENAME(namePos);                   // Contains character offset information, for parsing delimited strings
    #UNIQUENAME(namePos2);                  // Contains character offset information, for parsing delimited strings
    #UNIQUENAME(numValue);                  // Extracted numeric value from a string
    #UNIQUENAME(nameValue);                 // Extracted string value from a string
    #UNIQUENAME(nameValue2);                // Extracted string value from a string

    IMPORT Std;

    //--------------------------------------------------------------------------

    // Remove all spaces from features list so we can parse it more easily
    #UNIQUENAME(trimmedFeatures);
    LOCAL %trimmedFeatures% := TRIM(features, ALL);

    // Remove all spaces from field list so we can parse it more easily
    #UNIQUENAME(trimmedFieldList);
    LOCAL %trimmedFieldList% := TRIM((STRING)fieldListStr, ALL);

    // Clamp lcbLimit to 0..1000
    #UNIQUENAME(lowCardinalityThreshold);
    LOCAL %lowCardinalityThreshold% := MIN(MAX(lcbLimit, 0), 1000);

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
    LOCAL %FeatureEnabledFillRate%() := REGEXFIND('\\bfill_rate\\b', %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledBestECLTypes);
    LOCAL %FeatureEnabledBestECLTypes%() := REGEXFIND('\\bbest_ecl_types\\b', %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledLowCardinalityBreakdown);
    LOCAL %FeatureEnabledLowCardinalityBreakdown%() := %lowCardinalityThreshold% > 0 AND REGEXFIND('\\bcardinality_breakdown\\b', %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledCardinality);
    LOCAL %FeatureEnabledCardinality%() := %FeatureEnabledLowCardinalityBreakdown%() OR REGEXFIND('\\bcardinality\\b', %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledModes);
    LOCAL %FeatureEnabledModes%() := REGEXFIND('\\bmodes\\b', %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledLengths);
    LOCAL %FeatureEnabledLengths%() := REGEXFIND('\\blengths\\b', %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledPatterns);
    LOCAL %FeatureEnabledPatterns%() := (UNSIGNED)maxPatterns > 0 AND REGEXFIND('\\bpatterns\\b', %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledMinMax);
    LOCAL %FeatureEnabledMinMax%() := REGEXFIND('\\bmin_max\\b', %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledMean);
    LOCAL %FeatureEnabledMean%() := REGEXFIND('\\bmean\\b', %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledStdDev);
    LOCAL %FeatureEnabledStdDev%() := REGEXFIND('\\bstd_dev\\b', %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledQuartiles);
    LOCAL %FeatureEnabledQuartiles%() := REGEXFIND('\\bquartiles\\b', %trimmedFeatures%, NOCASE);
    #UNIQUENAME(FeatureEnabledCorrelations);
    LOCAL %FeatureEnabledCorrelations%() := REGEXFIND('\\bcorrelations\\b', %trimmedFeatures%, NOCASE);

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
            %clampedSampleSize% < 100,
            ENTH(%ungroupedInFile%, %clampedSampleSize%, 100, 1, LOCAL),
            %ungroupedInFile%
        );

    // Slim the dataset if the caller provided an explicit set of attributes;
    // note that explicit attributes within a top-level child dataset will
    // cause the entire top-level child dataset to be retained
    #UNIQUENAME(workingInFile);
    LOCAL %workingInFile% :=
        #IF(%trimmedFieldList% = '')
            %sampledData%
        #ELSE
            TABLE
                (
                    %sampledData%,
                    {
                        #SET(needsDelim, 0)
                        #SET(namePos, 1)
                        #SET(nameValue2, '')
                        #LOOP
                            #SET(temp, REGEXFIND('^([^,]+)', %trimmedFieldList%[%namePos%..], 1))
                            #IF(%'temp'% != '')
                                #SET(nameValue, REGEXFIND('^([^\\.]+)', %'temp'%, 1))
                                #IF(NOT REGEXFIND('\\b' + %'nameValue'% + '\\b', %'nameValue2'%))
                                    #IF(%'nameValue2'% != '')
                                        #APPEND(nameValue2, ',')
                                    #END
                                    #APPEND(nameValue2, %'nameValue'%)

                                    #IF(%needsDelim% = 1) , #END

                                    TYPEOF(%sampledData%.%nameValue%) %nameValue% := %nameValue%

                                    #SET(needsDelim, 1)
                                #END
                                #SET(namePos, %namePos% + LENGTH(%'temp'%) + 1)
                            #ELSE
                                #BREAK
                            #END
                        #END
                    }
                )
        #END;

    // Distribute the inbound dataset across all our nodes for faster processing
    #UNIQUENAME(distributedInFile);
    LOCAL %distributedInFile% := DISTRIBUTE(%workingInFile%, SKEW(0.05));

    #EXPORTXML(inFileFields, RECORDOF(%distributedInFile%));

    // Walk the slimmed dataset, pulling out top-level scalars and noting
    // child datasets
    #SET(scalarFields, '');
    #SET(childDSFields, '');
    #SET(fieldCount, 0);
    #SET(recLevel, 0);
    #SET(fieldStack, '');
    #SET(namePrefix, '');
    #SET(fullName, '');
    #FOR(inFileFields)
        #FOR(Field)
            #SET(fieldCount, %fieldCount% + 1)
            #IF(%{@isEnd}% != 1)
                // Adjust full name
                #SET(fullName, %'namePrefix'% + %'@name'%)
            #END
            #IF(%{@isRecord}% = 1)
                // Push record onto stack so we know what we're popping when we see @isEnd
                #SET(fieldStack, 'r' + %'fieldStack'%)
                #APPEND(namePrefix, %'@name'% + '.')
            #ELSEIF(%{@isDataset}% = 1)
                // Push dataset onto stack so we know what we're popping when we see @isEnd
                #SET(fieldStack, 'd' + %'fieldStack'%)
                #APPEND(namePrefix, %'@name'% + '.')
                #SET(recLevel, %recLevel% + 1)
                // Note the field index and field name so we can process it separately
                #IF(%'childDSFields'% != '')
                    #APPEND(childDSFields, ',')
                #END
                #APPEND(childDSFields, %'fieldCount'% + ':' + %'fullName'%)
                // Extract the child dataset into its own attribute so we can more easily
                // process it later
                #SET(temp, #MANGLE(%'fullName'%));
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
                #SET(namePrefix, REGEXREPLACE('\\w+\\.$', %'namePrefix'%, ''))
                #IF(%'fieldStack'%[1] = 'd')
                    #SET(recLevel, %recLevel% - 1)
                #END
                #SET(fieldStack, %'fieldStack'%[2..])
            #ELSEIF(%recLevel% = 0)
                // Note the field index and full name of the attribute so we can process it
                #IF(%'scalarFields'% != '')
                    #APPEND(scalarFields, ',')
                #END
                #APPEND(scalarFields, %'fieldCount'% + ':' + %'fullName'%)
            #END
        #END
    #END

    // Collect the gathered full attribute names so we can walk them later
    #SET(explicitScalarFields, REGEXREPLACE('\\d+:', %'scalarFields'%, ''));

    // Define the record layout that will be used by the inner _Inner_Profile() call
    LOCAL ModeRec := RECORD
        UTF8                            value;
        UNSIGNED4                       rec_count;
    END;

    LOCAL PatternCountRec := RECORD
        STRING                          data_pattern;
        UNSIGNED4                       rec_count;
        UTF8                            example;
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
        LOCAL %_CanProcessAttribute%(STRING attrName) := (_fieldListStr = '' OR REGEXFIND('(^|,)' + attrName + '(,|$)', _fieldListStr, NOCASE));

        // Test an attribute type to see if is a SET OF <something>
        #UNIQUENAME(_IsSetType);
        LOCAL %_IsSetType%(STRING attrType) := (attrType[..7] = 'set of ');

        // Helper function to convert a full field name into something we
        // can reference as an ECL attribute
        #UNIQUENAME(_MakeAttr);
        LOCAL %_MakeAttr%(STRING attr) := REGEXREPLACE('\\.', attr, '_');

        // Determine if a UTF-8 string really contains UTF-8 characters
        #UNIQUENAME(IsUTF8);
        LOCAL BOOLEAN %IsUTF8%(DATA str) := EMBED(C++)
            #option pure;

            if (lenStr == 0)
                return false;

            const unsigned char*    bytes = reinterpret_cast<const unsigned char*>(str);
            const unsigned char*    endPtr = bytes + lenStr;

            while (bytes < endPtr)
            {
                if (bytes[0] == 0x09 || bytes[0] == 0x0A || bytes[0] == 0x0D || (0x20 <= bytes[0] && bytes[0] <= 0x7E))
                {
                    // ASCII; continue scan
                    bytes += 1;
                }
                else if ((0xC2 <= bytes[0] && bytes[0] <= 0xDF) && (bytes+1 < endPtr) && (0x80 <= bytes[1] && bytes[1] <= 0xBF))
                {
                    // Valid non-overlong 2-byte
                    return true;
                }
                else if (bytes[0] == 0xE0 && (bytes+2 < endPtr) && (0xA0 <= bytes[1] && bytes[1] <= 0xBF) && (0x80 <= bytes[2] && bytes[2] <= 0xBF))
                {
                    // Valid excluding overlongs
                    return true;
                }
                else if (((0xE1 <= bytes[0] && bytes[0] <= 0xEC) || bytes[0] == 0xEE || bytes[0] == 0xEF) && (bytes+2 < endPtr) && (0x80 <= bytes[1] && bytes[1] <= 0xBF) && (0x80 <= bytes[2] && bytes[2] <= 0xBF))
                {
                    // Valid straight 3-byte
                    return true;
                }
                else if (bytes[0] == 0xED && (bytes+2 < endPtr) && (0x80 <= bytes[1] && bytes[1] <= 0x9F) && (0x80 <= bytes[2] && bytes[2] <= 0xBF))
                {
                    // Valid excluding surrogates
                    return true;
                }
                else if (bytes[0] == 0xF0 && (bytes+3 < endPtr) && (0x90 <= bytes[1] && bytes[1] <= 0xBF) && (0x80 <= bytes[2] && bytes[2] <= 0xBF) && (0x80 <= bytes[3] && bytes[3] <= 0xBF))
                {
                    // Valid planes 1-3
                    return true;
                }
                else if ((0xF1 <= bytes[0] && bytes[0] <= 0xF3) && (bytes+3 < endPtr) && (0x80 <= bytes[1] && bytes[1] <= 0xBF) && (0x80 <= bytes[2] && bytes[2] <= 0xBF) && (0x80 <= bytes[3] && bytes[3] <= 0xBF))
                {
                    // Valid planes 4-15
                    return true;
                }
                else if (bytes[0] == 0xF4 && (bytes+3 < endPtr) && (0x80 <= bytes[1] && bytes[1] <= 0x8F) && (0x80 <= bytes[2] && bytes[2] <= 0xBF) && (0x80 <= bytes[3] && bytes[3] <= 0xBF))
                {
                    // Valid plane 16
                    return true;
                }
                else
                {
                    // Invalid; abort
                    return false;
                }
            }

            return false;
        ENDEMBED;

        // Pattern mapping a STRING datatype
        #UNIQUENAME(_MapAllStr);
        LOCAL STRING %_MapAllStr%(STRING s) := EMBED(C++)
            #option pure;
            __lenResult = lenS;
            __result = static_cast<char*>(rtlMalloc(__lenResult));

            for (uint32_t x = 0; x < lenS; x++)
            {
                unsigned char   ch = s[x];

                if (ch >= 'A' && ch <= 'Z')
                    __result[x] = 'A';
                else if (ch >= 'a' && ch <= 'z')
                    __result[x] = 'a';
                else if (ch >= '1' && ch <= '9') // Leave '0' as-is and replace with '9' later
                    __result[x] = '9';
                else
                    __result[x] = ch;
            }
        ENDEMBED;

        // Pattern mapping a UNICODE datatype; using regex due to the complexity
        // of the character set
        #UNIQUENAME(_MapUpperCharUni);
        LOCAL %_MapUpperCharUni%(UNICODE s) := REGEXREPLACE(u'\\p{Uppercase_Letter}', s, u'A');
        #UNIQUENAME(_MapLowerCharUni);
        LOCAL %_MapLowerCharUni%(UNICODE s) := REGEXREPLACE(u'[[\\p{Lowercase_Letter}][\\p{Titlecase_Letter}][\\p{Modifier_Letter}][\\p{Other_Letter}]]', s, u'a');
        #UNIQUENAME(_MapDigitUni);
        LOCAL %_MapDigitUni%(UNICODE s) := REGEXREPLACE(u'[1-9]', s, u'9'); // Leave '0' as-is and replace with '9' later
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
        #SET(fieldStack, '');
        #SET(namePrefix, '');
        #SET(explicitFields, '');
        #SET(numericFields, '');
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
                    #IF(%_CanProcessAttribute%(%'namePrefix'% + %'@name'%))
                        #IF(%needsDelim% = 1)
                            #APPEND(explicitFields, ',')
                        #END
                        #APPEND(explicitFields, %'namePrefix'% + %'@name'%)
                        #SET(needsDelim, 1)

                        #IF(NOT %_IsSetType%(%'@type'%))
                            #IF(REGEXFIND('(string)|(data)|(utf)', %'@type'%))
                                #IF(%@size% < 0)
                                    #SET(foundMaxPatternLen, MAX(_maxPatternLen, %foundMaxPatternLen%))
                                #ELSE
                                    #SET(foundMaxPatternLen, MIN(MAX(%@size%, %foundMaxPatternLen%), _maxPatternLen))
                                #END
                            #ELSEIF(REGEXFIND('unicode', %'@type'%))
                                // UNICODE is UCS-2 so the size reflects two bytes per character
                                #IF(%@size% < 0)
                                    #SET(foundMaxPatternLen, MAX(_maxPatternLen, %foundMaxPatternLen%))
                                #ELSE
                                    #SET(foundMaxPatternLen, MIN(MAX(%@size% DIV 2 + 1, %foundMaxPatternLen%), _maxPatternLen))
                                #END
                            #ELSEIF(REGEXFIND('(integer)|(unsigned)|(decimal)|(real)', %'@type'%))
                                #IF(%'numericFields'% != '')
                                    #APPEND(numericFields, ',')
                                #END
                                #APPEND(numericFields, %'namePrefix'% + %'@name'%)
                            #END
                        #END
                    #END
                #END
            #END
        #END

        // Typedefs
        #UNIQUENAME(DataPattern_t);
        LOCAL %DataPattern_t% := #EXPAND('STRING' + %'foundMaxPatternLen'%);
        #UNIQUENAME(StringValue_t);
        LOCAL %StringValue_t% := #EXPAND('UTF8_' + %'foundMaxPatternLen'%);

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
            BOOLEAN             is_unicode;
        END;

        #UNIQUENAME(dataInfo);
        LOCAL %dataInfo% :=
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
                        #IF(%_CanProcessAttribute%(%'namePrefix'% + %'@name'%))
                            #SET(fieldCount, %fieldCount% + 1)
                            #IF(%needsDelim% = 1) + #END

                            IF(EXISTS(_inFile),
                                PROJECT
                                    (
                                        TABLE
                                            (
                                                _inFile,
                                                {
                                                    %Attribute_t%       attribute := %'namePrefix'% + %'@name'%,
                                                    %AttributeType_t%   given_attribute_type := %'@ecltype'%,
                                                    %StringValue_t%     string_value :=
                                                                            #IF(%_IsSetType%(%'@type'%))
                                                                                (%StringValue_t%)Std.Str.CombineWords((SET OF STRING)_inFile.#EXPAND(%'namePrefix'% + %'@name'%), ', ')
                                                                            #ELSEIF(REGEXFIND('(integer)|(unsigned)|(decimal)|(real)|(boolean)', %'@type'%))
                                                                                (%StringValue_t%)_inFile.#EXPAND(%'namePrefix'% + %'@name'%)
                                                                            #ELSEIF(REGEXFIND('string', %'@type'%))
                                                                                %_TrimmedUni%(_inFile.#EXPAND(%'namePrefix'% + %'@name'%))
                                                                            #ELSE
                                                                                %_TrimmedUni%((%StringValue_t%)_inFile.#EXPAND(%'namePrefix'% + %'@name'%))
                                                                            #END,
                                                    UNSIGNED4           value_count := COUNT(GROUP),
                                                    %DataPattern_t%     data_pattern :=
                                                                            #IF(%_IsSetType%(%'@type'%))
                                                                                %_MapAllStr%(%_TrimmedStr%(Std.Str.CombineWords((SET OF STRING)_inFile.#EXPAND(%'namePrefix'% + %'@name'%), ', '))[..%foundMaxPatternLen%])
                                                                            #ELSEIF(REGEXFIND('(integer)|(unsigned)|(decimal)|(real)', %'@type'%))
                                                                                %_MapAllStr%((STRING)_inFile.#EXPAND(%'namePrefix'% + %'@name'%))
                                                                            #ELSEIF(REGEXFIND('(unicode)|(utf)', %'@type'%))
                                                                                #IF(%@size% < 0 OR (%@size% DIV 2 + 1) > %foundMaxPatternLen%)
                                                                                    %_MapAllUni%(%_TrimmedUni%((UNICODE)_inFile.#EXPAND(%'namePrefix'% + %'@name'%))[..%foundMaxPatternLen%])
                                                                                #ELSE
                                                                                    %_MapAllUni%(%_TrimmedUni%((UNICODE)_inFile.#EXPAND(%'namePrefix'% + %'@name'%)))
                                                                                #END
                                                                            #ELSEIF(REGEXFIND('string', %'@type'%))
                                                                                #IF(%@size% < 0 OR %@size% > %foundMaxPatternLen%)
                                                                                    %_MapAllStr%(%_TrimmedStr%(_inFile.#EXPAND(%'namePrefix'% + %'@name'%))[..%foundMaxPatternLen%])
                                                                                #ELSE
                                                                                    %_MapAllStr%(%_TrimmedStr%(_inFile.#EXPAND(%'namePrefix'% + %'@name'%)))
                                                                                #END
                                                                            #ELSEIF(%'@type'% = 'boolean')
                                                                                'B'
                                                                            #ELSE
                                                                                %_MapAllStr%(%_TrimmedStr%((STRING)_inFile.#EXPAND(%'namePrefix'% + %'@name'%))[..%foundMaxPatternLen%])
                                                                            #END,
                                                    UNSIGNED4           data_length :=
                                                                            #IF(%_IsSetType%(%'@type'%))
                                                                                COUNT(_inFile.#EXPAND(%'namePrefix'% + %'@name'%))
                                                                            #ELSEIF(REGEXFIND('(unicode)|(utf)', %'@type'%))
                                                                                LENGTH(%_TrimmedUni%((UNICODE)_inFile.#EXPAND(%'namePrefix'% + %'@name'%)))
                                                                            #ELSEIF(REGEXFIND('string', %'@type'%))
                                                                                LENGTH(%_TrimmedStr%(_inFile.#EXPAND(%'namePrefix'% + %'@name'%)))
                                                                            #ELSEIF(%'@type'% = 'boolean')
                                                                                1
                                                                            #ELSE
                                                                                LENGTH((STRING)_inFile.#EXPAND(%'namePrefix'% + %'@name'%))
                                                                            #END,
                                                    BOOLEAN             is_filled :=
                                                                            #IF(%_IsSetType%(%'@type'%))
                                                                                COUNT(_inFile.#EXPAND(%'namePrefix'% + %'@name'%)) > 0
                                                                            #ELSEIF(REGEXFIND('(unicode)|(utf)', %'@type'%))
                                                                                LENGTH(%_TrimmedUni%(_inFile.#EXPAND(%'namePrefix'% + %'@name'%))) > 0
                                                                            #ELSEIF(REGEXFIND('string', %'@type'%))
                                                                                LENGTH(%_TrimmedStr%(_inFile.#EXPAND(%'namePrefix'% + %'@name'%))) > 0
                                                                            #ELSEIF(REGEXFIND('data', %'@type'%))
                                                                                LENGTH(_inFile.#EXPAND(%'namePrefix'% + %'@name'%)) > 0
                                                                            #ELSEIF(%'@type'% = 'boolean')
                                                                                TRUE
                                                                            #ELSE
                                                                                _inFile.#EXPAND(%'namePrefix'% + %'@name'%) != 0
                                                                            #END,
                                                    BOOLEAN             is_number :=
                                                                            #IF(%_IsSetType%(%'@type'%))
                                                                                FALSE
                                                                            #ELSEIF(REGEXFIND('(integer)|(unsigned)|(decimal)|(real)', %'@type'%))
                                                                                TRUE
                                                                            #ELSE
                                                                                FALSE
                                                                            #END,
                                                    BOOLEAN             is_unicode :=
                                                                            #IF(%_IsSetType%(%'@type'%))
                                                                                FALSE
                                                                            #ELSEIF(REGEXFIND('(unicode)|(utf)', %'@type'%))
                                                                                %IsUTF8%((DATA)_inFile.#EXPAND(%'namePrefix'% + %'@name'%))
                                                                            #ELSE
                                                                                FALSE
                                                                            #END
                                                },
                                                _inFile.#EXPAND(%'namePrefix'% + %'@name'%),
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
                                                SELF.attribute := %'namePrefix'% + %'@name'%,
                                                SELF.given_attribute_type := %'@ecltype'%,
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
            isLeadingZeroInteger := REGEXFIND('^0[09]{1,18}$', dataPattern);
            isSignedInteger := REGEXFIND('^\\-[09]{1,19}$', dataPattern);
            isShortUnsignedInteger := REGEXFIND('^[09]{1,19}$', dataPattern);
            isUnsignedInteger := REGEXFIND('^\\+?[09]{1,20}$', dataPattern);
            isFloatingPoint := REGEXFIND('^(\\-|\\+)?[09]{0,15}\\.[09]{1,15}$', dataPattern);
            isExpNotation := REGEXFIND('^(\\-|\\+)?[09]\\.[09]{1,6}[aA]\\-[09]{1,3}$', dataPattern);

            stringWithNumbersType := MAP
                (
                    isSignedInteger         =>  %DataTypeEnum%.SignedInteger | %DataTypeEnum%.FloatingPoint | %DataTypeEnum%.ExpNotation,
                    isShortUnsignedInteger  =>  %DataTypeEnum%.SignedInteger | %DataTypeEnum%.UnsignedInteger | %DataTypeEnum%.FloatingPoint | %DataTypeEnum%.ExpNotation,
                    isUnsignedInteger       =>  %DataTypeEnum%.UnsignedInteger | %DataTypeEnum%.FloatingPoint | %DataTypeEnum%.ExpNotation,
                    isFloatingPoint         =>  %DataTypeEnum%.FloatingPoint | %DataTypeEnum%.ExpNotation,
                    isExpNotation           =>  %DataTypeEnum%.ExpNotation,
                    %DataTypeEnum%.AsIs
                );

            bestType := MAP
                (
                    %_IsSetType%(attributeType)                                                 =>  %DataTypeEnum%.AsIs,
                    REGEXFIND('(integer)|(unsigned)|(decimal)|(real)|(boolean)', attributeType) =>  %DataTypeEnum%.AsIs,
                    isLeadingZeroInteger                                                        =>  %DataTypeEnum%.AsIs,
                    stringWithNumbersType
                );

            RETURN bestType;
        END;

        // Estimate integer size from readable data length
        #UNIQUENAME(Len2Size);
        LOCAL %Len2Size%(UNSIGNED2 c) := MAP ( c < 3 => 1, c < 5 => 2, c < 7 => 3, c < 9 => 4, c < 11 => 5, c < 14 => 6, c < 16 => 7, 8 );

        #UNIQUENAME(attributeTypePatterns);
        LOCAL %attributeTypePatterns% := TABLE
            (
                %filledDataInfo%,
                {
                    attribute,
                    given_attribute_type,
                    data_pattern,
                    data_length,
                    is_unicode,
                    %DataTypeEnum%      type_flag := %BestTypeFlag%(TRIM(data_pattern), given_attribute_type),
                    UNSIGNED4           min_data_length := 0 // will be populated within %attributesWithTypeFlagsSummary%

                },
                attribute, given_attribute_type, data_pattern, data_length, is_unicode,
                MERGE
            );

        #UNIQUENAME(MinNotZero);
        LOCAL %MinNotZero%(UNSIGNED4 n1, UNSIGNED4 n2) := MAP
            (
                n1 = 0  =>  n2,
                n2 = 0  =>  n1,
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
                        SELF.is_unicode := LEFT.is_unicode OR RIGHT.is_unicode,
                        SELF.type_flag := IF(TRIM(RIGHT.attribute) != '', LEFT.type_flag & RIGHT.type_flag, LEFT.type_flag),
                        SELF := LEFT
                    ),
                TRANSFORM
                    (
                        RECORDOF(%attributeTypePatterns%),
                        SELF.data_length := MAX(RIGHT1.data_length, RIGHT2.data_length),
                        SELF.min_data_length := %MinNotZero%(RIGHT1.data_length, RIGHT2.data_length),
                        SELF.is_unicode := RIGHT1.is_unicode OR RIGHT2.is_unicode,
                        SELF.type_flag := RIGHT1.type_flag & RIGHT2.type_flag,
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
                                %_IsSetType%(LEFT.given_attribute_type)                                                 =>  LEFT.given_attribute_type,
                                REGEXFIND('(integer)|(unsigned)|(decimal)|(real)|(boolean)', LEFT.given_attribute_type) =>  LEFT.given_attribute_type,
                                REGEXFIND('data', LEFT.given_attribute_type)                                            =>  'data' + IF(LEFT.data_length > 0 AND (LEFT.data_length < (LEFT.min_data_length * 1000)), (STRING)LEFT.data_length, ''),
                                (LEFT.type_flag & %DataTypeEnum%.UnsignedInteger) != 0                                  =>  'unsigned' + %Len2Size%(LEFT.data_length),
                                (LEFT.type_flag & %DataTypeEnum%.SignedInteger) != 0                                    =>  'integer' + %Len2Size%(LEFT.data_length),
                                (LEFT.type_flag & %DataTypeEnum%.FloatingPoint) != 0                                    =>  'real' + IF(LEFT.data_length < 8, '4', '8'),
                                (LEFT.type_flag & %DataTypeEnum%.ExpNotation) != 0                                      =>  'real8',
                                REGEXFIND('utf', LEFT.given_attribute_type) AND LEFT.is_unicode                         =>  LEFT.given_attribute_type,
                                REGEXFIND('utf', LEFT.given_attribute_type)                                             =>  'string' + IF(LEFT.data_length > 0 AND (LEFT.data_length < (LEFT.min_data_length * 1000)), (STRING)LEFT.data_length, ''),
                                REGEXREPLACE('\\d+$', TRIM(LEFT.given_attribute_type), '') + IF(LEFT.data_length > 0 AND (LEFT.data_length < (LEFT.min_data_length * 1000)), (STRING)LEFT.data_length, '')
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
                        SELF.is_number := LEFT.is_number OR (REGEXFIND('(integer)|(unsigned)|(decimal)|(real)', RIGHT.best_attribute_type) AND NOT REGEXFIND('set of ', RIGHT.best_attribute_type)),
                        SELF := LEFT
                    ),
                LEFT OUTER, KEEP(1), SMART
            ) : ONWARNING(4531, IGNORE);

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
                    #IF(%_CanProcessAttribute%(%'namePrefix'% + %'@name'%))
                        // Note that we create explicit attributes here for all
                        // top-level attributes in the dataset that we're
                        // processing, even if they are not numeric datatypes
                        #UNIQUENAME(uniqueNumericValueCounts)
                        %uniqueNumericValueCounts% := PROJECT
                            (
                                %filledDataInfoNumeric%(attribute = %'namePrefix'% + %'@name'% AND is_number),
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
                        LOCAL #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_min')) := MIN(%uniqueNumericValueCounts%, value);
                        LOCAL #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_max')) := MAX(%uniqueNumericValueCounts%, value);
                        LOCAL #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_ave')) := SUM(%uniqueNumericValueCounts%, value * cnt) / SUM(%uniqueNumericValueCounts%, cnt);
                        LOCAL #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_std_dev')) := SQRT(SUM(%uniqueNumericValueCounts%, (value - #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_ave'))) * (value - #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_ave'))) * cnt) / SUM(%uniqueNumericValueCounts%, cnt));

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
                        LOCAL #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_q2_value')) := AVE(%q2Value1%, %q2Value2%);

                        // Find the lower quartile
                        #UNIQUENAME(q1Pos1);
                        LOCAL %q1Pos1% := (%halfNumRecs% DIV 2) + (%halfNumRecs% % 2);
                        #UNIQUENAME(q1Value1);
                        LOCAL %q1Value1% := MIN(%uniqueNumericValuePos%(valueEndPos >= %q1Pos1%), value);
                        #UNIQUENAME(q1Pos2);
                        LOCAL %q1Pos2% := %q1Pos1% + ((%halfNumRecs% + 1) % 2);
                        #UNIQUENAME(q1Value2);
                        LOCAL %q1Value2% := MIN(%uniqueNumericValuePos%(valueEndPos >= %q1Pos2%), value);
                        LOCAL #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_q1_value')) := IF(%halfNumRecs% > 0, AVE(%q1Value1%, %q1Value2%), 0);

                        // Find the upper quartile
                        #UNIQUENAME(q3Pos1);
                        LOCAL %q3Pos1% := MAX(%q2Pos1%, %q2Pos2%) + (%halfNumRecs% DIV 2) + (%halfNumRecs% % 2);
                        #UNIQUENAME(q3Value1);
                        LOCAL %q3Value1% := MIN(%uniqueNumericValuePos%(valueEndPos >= %q3Pos1%), value);
                        #UNIQUENAME(q3Pos2);
                        LOCAL %q3Pos2% := %q3Pos1% - ((%halfNumRecs% + 1) % 2);
                        #UNIQUENAME(q3Value2);
                        LOCAL %q3Value2% := MIN(%uniqueNumericValuePos%(valueEndPos >= %q3Pos2%), value);
                        LOCAL #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_q3_value')) := IF(%halfNumRecs% > 0, AVE(%q3Value1%, %q3Value2%), 0);

                        // Derive all unique data values and the number of times
                        // each occurs in the data
                        LOCAL #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_uniq_value_recs')) := TABLE
                            (
                                %filledDataInfoNumeric%(attribute = %'namePrefix'% + %'@name'%),
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
                        %topRecords% := TOPN(#EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_uniq_value_recs')), _maxModes, -rec_count);
                        #UNIQUENAME(topRecord)
                        %topRecord% := TOPN(%topRecords%, 1, -rec_count);
                        LOCAL #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_mode_values')) := JOIN
                            (
                                %topRecords%,
                                %topRecord%,
                                LEFT.rec_count = RIGHT.rec_count,
                                TRANSFORM
                                    (
                                        ModeRec,
                                        SELF.value := (UTF8)LEFT.string_value,
                                        SELF.rec_count := LEFT.rec_count
                                    ),
                                SMART
                            ) : ONWARNING(4531, IGNORE);

                        // Get records with low cardinality
                        LOCAL #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_lcb_recs')) := IF
                            (
                                COUNT(#EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_uniq_value_recs'))) <= _lcbLimit,
                                PROJECT
                                    (
                                        SORT(#EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_uniq_value_recs')), -rec_count),
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
                                        CORRELATION(_inFile, _inFile.%fieldX%, _inFile.%fieldY%)
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
        // replace the '0' character left in from the pattern generation with
        // a '9' character to make the numeric pattern complete
        #UNIQUENAME(dataPatternStats0);
        LOCAL %dataPatternStats0% := PROJECT
            (
                %filledDataInfoNumeric%,
                TRANSFORM
                    (
                        RECORDOF(LEFT),
                        SELF.data_pattern := Std.Str.FindReplace(LEFT.data_pattern, '0', '9'),
                        SELF := LEFT
                    )
            );

        #UNIQUENAME(dataPatternStats);
        LOCAL %dataPatternStats% := TABLE
            (
                %dataPatternStats0%,
                {
                    attribute,
                    data_pattern,
                    UTF8        example := string_value[..%foundMaxPatternLen%],
                    UNSIGNED4   rec_count := SUM(GROUP, value_count)
                },
                attribute, data_pattern,
                MERGE
            ) : ONWARNING(2168, IGNORE);
        #UNIQUENAME(groupedDataPatterns);
        LOCAL %groupedDataPatterns% := GROUP(SORT(DISTRIBUTE(%dataPatternStats%, HASH32(attribute)), attribute, LOCAL), attribute, LOCAL);
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
                                #IF(%_CanProcessAttribute%(%'namePrefix'% + %'@name'%))
                                    #IF(%needsDelim% = 1) , #END

                                    {
                                        %'namePrefix'% + %'@name'%,
                                        #IF(%_IsSetType%(%'@type'%))
                                            FALSE,
                                        #ELSEIF(REGEXFIND('(integer)|(unsigned)|(decimal)|(real)', %'@type'%))
                                            TRUE,
                                        #ELSE
                                            FALSE,
                                        #END
                                        #IF(%FeatureEnabledCardinality%())
                                            COUNT(#EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_uniq_value_recs'))),
                                        #ELSE
                                            0,
                                        #END
                                        #IF(%FeatureEnabledMinMax%())
                                            #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_min')),
                                            #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_max')),
                                        #ELSE
                                            0,
                                            0,
                                        #END
                                        #IF(%FeatureEnabledMean%())
                                            #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_ave')),
                                        #ELSE
                                            0,
                                        #END
                                        #IF(%FeatureEnabledStdDev%())
                                            #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_std_dev')),
                                        #ELSE
                                            0,
                                        #END
                                        #IF(%FeatureEnabledQuartiles%())
                                            #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_q1_value')),
                                            #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_q2_value')),
                                            #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_q3_value')),
                                        #ELSE
                                            0,
                                            0,
                                            0,
                                        #END
                                        #IF(%FeatureEnabledModes%())
                                            #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_mode_values'))(rec_count > 1), // Modes must have more than one instance
                                        #ELSE
                                            DATASET([], ModeRec),
                                        #END
                                        #IF(%FeatureEnabledLowCardinalityBreakdown%())
                                            #EXPAND(%_MakeAttr%(%'namePrefix'% + %'@name'% + '_lcb_recs'))
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
                                SELF.best_attribute_type := IF(TRIM(RIGHT.best_attribute_type, RIGHT) != '', TRIM(RIGHT.best_attribute_type, RIGHT), LEFT.given_attribute_type),
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
                        SELF.is_numeric := LEFT.is_numeric OR (REGEXFIND('(integer)|(unsigned)|(decimal)|(real)', RIGHT.best_attribute_type) AND NOT REGEXFIND('set of ', RIGHT.best_attribute_type)),
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

        RETURN #IF(%fieldCount% > 0) %final70% #ELSE DATASET([], _resultLayout) #END;
    ENDMACRO;

    //==========================================================================

    // Call _Inner_Profile() with the given input dataset top-level scalar attributes,
    // then again for each child dataset that has been found; combine the
    // results of all the calls
    #UNIQUENAME(collectedResults);
    LOCAL %collectedResults% :=
        #IF(%'explicitScalarFields'% != '')
            _Inner_Profile
                (
                    GLOBAL(%distributedInFile%),
                    %'explicitScalarFields'%,
                    maxPatterns,
                    maxPatternLen,
                    %lowCardinalityThreshold%,
                    %MAX_MODES%,
                    OutputLayout,
                    '',
                    ''
                )
        #ELSE
            DATASET([], OutputLayout)
        #END
        #UNIQUENAME(dsNameValue)
        #SET(namePos, 1)
        #LOOP
            #SET(dsNameValue, REGEXFIND('^([^,]+)', %'childDSFields'%[%namePos%..], 1))
            #IF(%'dsNameValue'% != '')
                #SET(numValue, REGEXFIND('^(\\d+):', %'dsNameValue'%, 1))
                #SET(nameValue, REGEXFIND(':([^:]+)$', %'dsNameValue'%, 1))
                // Extract a list of fields within this child dataset if necessary
                #SET(explicitScalarFields, '')
                #SET(needsDelim, 0)
                #SET(namePos2, 1)
                #LOOP
                    #SET(temp, REGEXFIND('^([^,]+)', %trimmedFieldList%[%namePos2%..], 1))
                    #IF(%'temp'% != '')
                        #SET(nameValue2, REGEXFIND('^' + %'nameValue'% + '\\.([^,]+)', %'temp'%, 1))
                        #IF(%'nameValue2'% != '')
                            #IF(%needsDelim% = 1)
                                #APPEND(explicitScalarFields, ',')
                            #END
                            #APPEND(explicitScalarFields, %'nameValue2'%)
                            #SET(needsDelim, 1)
                        #END
                        #SET(namePos2, %namePos2% + LENGTH(%'temp'%) + 1)
                    #ELSE
                        #BREAK
                    #END
                #END
                // The child dataset should have been extracted into its own
                // local attribute; reference it during our call to the inner
                // profile function macro
                #SET(temp, #MANGLE(%'nameValue'%))
                + _Inner_Profile
                    (
                        GLOBAL(%temp%),
                        %'explicitScalarFields'%,
                        maxPatterns,
                        maxPatternLen,
                        %lowCardinalityThreshold%,
                        %MAX_MODES%,
                        OutputLayout,
                        %'nameValue'% + '.',
                        INTFORMAT(%numValue%, 5, 1) + '.'
                    )
                #SET(namePos, %namePos% + LENGTH(%'dsNameValue'%) + 1)
            #ELSE
                #BREAK
            #END
        #END;

    // Put the combined _Inner_Profile() results in the right order and layout
    #UNIQUENAME(finalData);
    LOCAL %finalData% := PROJECT(SORT(%collectedResults%, sortValue), %FinalOutputLayout%);

    RETURN %finalData%;
ENDMACRO;
