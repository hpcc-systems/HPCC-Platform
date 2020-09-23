/**
 * Benford's law, also called the Newcomb–Benford law, or the law of anomalous
 * numbers, is an observation about the frequency distribution of leading
 * digits in many real-life sets of numerical data.
 *
 * Benford's law doesn't apply to every set of numbers, but it usually applies
 * to large sets of naturally occurring numbers with some connection like:
 *
 *      Companies' stock market values
 *      Data found in texts — like the Reader's Digest, or a copy of Newsweek
 *      Demographic data, including state and city populations
 *      Income tax data
 *      Mathematical tables, like logarithms
 *      River drainage rates
 *      Scientific data
 *
 * The law usually doesn't apply to data sets that have a stated minimum and
 * maximum, like interest rates or hourly wages. If numbers are assigned,
 * rather than naturally occurring, they will also not follow the law. Examples
 * of assigned numbers include: zip codes, telephone numbers and Social
 * Security numbers.
 *
 * For more information: https://en.wikipedia.org/wiki/Benford%27s_law
 *
 * This function computes the distribution of digits within one or more
 * attributes in a dataset and displays the result, one attribute per row,
 * with an "expected" row showing the expected distributions.  Included
 * in each data row is a chi-squared computation for that row indicating how
 * well the computed result matches the expected result (if the chi-squared
 * value exceeds the one shown in the --EXPECTED-- row then the data row
 * DOES NOT follow Benford's Law).
 *
 * Note that when computing the distribution of the most significant digit,
 * the digit zero is ignored.  So for instance, the values 0100, 100, 1.0,
 * 0.10, and 0.00001 all have a most-significant digit of '1'.  The digit
 * zero is considered for all other positions.
 *
 * @param   inFile          The dataset to process; REQUIRED
 * @param   fieldListStr    A string containing a comma-delimited list of
 *                          attribute names to process; note that attributes
 *                          listed here must be top-level attributes (not child
 *                          records or child datasets); use an empty string to
 *                          process all top-level attributes in inFile;
 *                          OPTIONAL, defaults to an empty string
 * @param   digit           The 1-based digit within the number to examine; the
 *                          first significant digit is '1' and it only increases;
 *                          OPTIONAL, defaults to 1, meaning the most-significant
 *                          non-zero digit
 * @param   sampleSize      A positive integer representing a percentage of
 *                          inFile to examine, which is useful when analyzing a
 *                          very large dataset and only an estimated data
 *                          analysis is sufficient; valid range for this
 *                          argument is 1-100; values outside of this range
 *                          will be clamped; OPTIONAL, defaults to 100 (which
 *                          indicates that all rows in the dataset will be used)
 *
 * @return  A new dataset with the following record structure:
 *
 *      RECORD
 *          STRING      attribute;   // Name of data attribute examined
 *          DECIMAL4_1  zero;        // Percentage of rows with digit of '0'
 *          DECIMAL4_1  one;         // Percentage of rows with digit of '1'
 *          DECIMAL4_1  two;         // Percentage of rows with digit of '2'
 *          DECIMAL4_1  three;       // Percentage of rows with digit of '3'
 *          DECIMAL4_1  four;        // Percentage of rows with digit of '4'
 *          DECIMAL4_1  five;        // Percentage of rows with digit of '5'
 *          DECIMAL4_1  six;         // Percentage of rows with digit of '6'
 *          DECIMAL4_1  seven;       // Percentage of rows with digit of '7'
 *          DECIMAL4_1  eight;       // Percentage of rows with digit of '8'
 *          DECIMAL4_1  nine;        // Percentage of rows with digit of '9'
 *          DECIMAL7_3  chi_squared; // Chi-squared "fitness test" result
 *          UNSIGNED8   num_values;  // Number of rows with non-zero values for this attribute
 *      END;
 *
 * The named digit fields (e.g. "zero" and "one" and so on) represent the
 * digit found in the 'digit' position of the associated attribute.  The values
 * that appear there are percentages.  num_values shows the number of
 * non-zero values processed, and chi_squared shows the result of applying
 * that test using the observed vs expected distribution values.
 *
 * The first row of the results will show the expected values for the named
 * digits, with "-- EXPECTED DIGIT n --" showing as the attribute name.'n' will
 * be replaced with the value of 'digit' which indicates which digit position
 * was examined.
 *
 * Note that when viewing the results for the mosts significant digit (digit = 1),
 * the 'zero' field will show a -1 value, indicating that it was ignored.
 */
EXPORT Benford(inFile, fieldListStr = '\'\'', digit = 1, sampleSize = 100) := FUNCTIONMACRO

    #UNIQUENAME(minDigit);
    LOCAL %minDigit% := MAX((INTEGER)digit, 1);

    #UNIQUENAME(clampedDigit);
    LOCAL %clampedDigit% := MIN(%minDigit%, 4);

    // Chi-squared critical value table:
    // https://www.itl.nist.gov/div898/handbook/eda/section3/eda3674.htm

    // Chi-squared critical values for 8 degrees of freedom at various probabilities
    // Probability:     0.90    0.95    0.975   0.99    0.999
    // Critical value:  13.362  15.507  17.535  20.090  26.125
    #UNIQUENAME(CHI_SQUARED_CRITICAL_VALUE_1);
    #SET(CHI_SQUARED_CRITICAL_VALUE_1, 20.090); // 99% probability

    // Chi-squared critical values for 9 degrees of freedom at various probabilities
    // Probability:     0.90    0.95    0.975   0.99    0.999
    // Critical value:  14.684  16.919  19.023  21.666  27.877
    #UNIQUENAME(CHI_SQUARED_CRITICAL_VALUE_2);
    #SET(CHI_SQUARED_CRITICAL_VALUE_2, 21.666); // 99% probability

    #UNIQUENAME(CHI_SQUARED_CRITICAL_VALUE);
    LOCAL %CHI_SQUARED_CRITICAL_VALUE% := IF(%clampedDigit% = 1, %CHI_SQUARED_CRITICAL_VALUE_1%, %CHI_SQUARED_CRITICAL_VALUE_2%);

    #UNIQUENAME(expectedDistribution);
    LOCAL %expectedDistribution% := DATASET
        (
            [
                {1, -1, 30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6},
                {2, 12.0, 11.4, 10.9, 10.4, 10.0, 9.7, 9.3, 9.0, 8.8, 8.5},
                {3, 10.2, 10.1, 10.1, 10.1, 10.0, 10.0, 9.9, 9.9, 9.9, 9.8},
                {4, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0}
            ],
            {
                UNSIGNED1   pos,
                DECIMAL4_1  zero,
                DECIMAL4_1  one,
                DECIMAL4_1  two,
                DECIMAL4_1  three,
                DECIMAL4_1  four,
                DECIMAL4_1  five,
                DECIMAL4_1  six,
                DECIMAL4_1  seven,
                DECIMAL4_1  eight,
                DECIMAL4_1  nine
            }
        );

    // Remove all spaces from field list so we can parse it more easily
    #UNIQUENAME(trimmedFieldList);
    LOCAL %trimmedFieldList% := TRIM((STRING)fieldListStr, ALL);

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
    // note that the TABLE function will fail if %trimmedFieldList% cites an
    // attribute that is a child dataset (this is an ECL limitation)
    #UNIQUENAME(workingInFile);
    LOCAL %workingInFile% :=
        #IF(%trimmedFieldList% = '')
            %sampledData%
        #ELSE
            TABLE(%sampledData%, {#EXPAND(%trimmedFieldList%)})
        #END;

    // Helper function that returns the 'pos' significant digit in a string;
    // if pos = 1 then th digit must be non-zero; returns 10
    // (an invalid *digit*) if no suitable digit is found
    #UNIQUENAME(NthDigit);
    LOCAL UNSIGNED1 %NthDigit%(STRING s, UNSIGNED1 pos) := EMBED(C++)
        #option pure
        unsigned char   foundDigit = 10; // impossible value
        int             digitsFound = 0;

        for (unsigned int x = 0; x < lenS; x++)
        {
            char ch = s[x];

            if (isdigit(ch) && (digitsFound > 0 || ch != '0'))
            {
                ++digitsFound;

                if (digitsFound >= pos)
                {
                    foundDigit = ch - '0';
                    break;
                }

                // Once we find a significant digit, the default return value
                // is a trailing zero (assumed after an implied decimal point
                // if we're parsing an integer)
                foundDigit = 0;
            }
            else if (ch == '.')
            {
                // Once we find a decimal point, the default return value
                // is a trailing zero
                foundDigit = 0;
            }
        }

        return foundDigit;
    ENDEMBED;

    // Temp field name we will use to ensure proper ordering of results
    #UNIQUENAME(idField);

    // One-record dataset containing expected Benford results, per-digit
    #UNIQUENAME(expectedDS);
    LOCAL %expectedDS% := PROJECT
        (
            %expectedDistribution%(pos = %clampedDigit%),
            TRANSFORM
                (
                    {
                        UNSIGNED2   %idField%,
                        RECORDOF(LEFT) - [pos],
                        DECIMAL7_3  chi_squared,
                        UNSIGNED8   num_values,
                        STRING      attribute // Put this at the end for now, because it is variable-length
                    },
                    SELF.%idField% := 0,
                    SELF.chi_squared := 0,
                    SELF.num_values := COUNT(%workingInFile%),
                    SELF.attribute := '-- EXPECTED DIGIT ' + (STRING)%minDigit% + ' --',
                    SELF := LEFT
                )
        );

    // This will be used later as a datatype in a function signature
    #UNIQUENAME(DataRec);
    LOCAL %DataRec% := RECORDOF(%expectedDS%);

    // Get the internal representation of our working dataset
    #EXPORTXML(inFileFields, RECORDOF(%workingInFile%));

    // Create a dataset composed of the expectedDS and a row for each
    // field we will be processing
    #UNIQUENAME(interimResult);
    LOCAL %interimResult% := %expectedDS%
        #UNIQUENAME(recLevel)
        #SET(recLevel, 0)
        #UNIQUENAME(fieldNum)
        #SET(fieldNum, 0)
        #FOR(inFileFields)
            #FOR(Field)
                #IF(%{@isRecord}% = 1 OR %{@isDataset}% = 1)
                    #SET(recLevel, %recLevel% + 1)
                #ELSEIF(%{@isEnd}% = 1)
                    #SET(recLevel, %recLevel% - 1)
                #ELSEIF(%recLevel% = 0)
                    #SET(fieldNum, %fieldNum% + 1)
                    + TABLE
                        (
                            PROJECT(%workingInFile%, TRANSFORM({UNSIGNED1 n}, SELF.n := %NthDigit%((STRING)LEFT.%@name%, %minDigit%)))(n != 10),
                            {
                                UNSIGNED2   %idField% := %fieldNum%,
                                DECIMAL4_1  zero := IF(%minDigit% = 1, -1.0, COUNT(GROUP, n = 0) / COUNT(GROUP) * 100),
                                DECIMAL4_1  one := COUNT(GROUP, n = 1) / COUNT(GROUP) * 100,
                                DECIMAL4_1  two := COUNT(GROUP, n = 2) / COUNT(GROUP) * 100,
                                DECIMAL4_1  three := COUNT(GROUP, n = 3) / COUNT(GROUP) * 100,
                                DECIMAL4_1  four := COUNT(GROUP, n = 4) / COUNT(GROUP) * 100,
                                DECIMAL4_1  five := COUNT(GROUP, n = 5) / COUNT(GROUP) * 100,
                                DECIMAL4_1  six := COUNT(GROUP, n = 6) / COUNT(GROUP) * 100,
                                DECIMAL4_1  seven := COUNT(GROUP, n = 7) / COUNT(GROUP) * 100,
                                DECIMAL4_1  eight := COUNT(GROUP, n = 8) / COUNT(GROUP) * 100,
                                DECIMAL4_1  nine := COUNT(GROUP, n = 9) / COUNT(GROUP) * 100,
                                DECIMAL7_3  chi_squared := 0, // Fill in later
                                UNSIGNED8   num_values := COUNT(GROUP),
                                STRING      attribute := %'@name'%
                            },
                            MERGE
                        )
                #END
            #END
        #END;

    // Helper function for computing chi-squared values from the interim results
    #UNIQUENAME(ComputeChiSquared);
    LOCAL %ComputeChiSquared%(%DataRec% expected, %DataRec% actual, UNSIGNED1 pos) := FUNCTION
        Term(DECIMAL4_1 e, DECIMAL4_1 o) := (((o - e) * (o - e)) / e);

        RETURN Term(expected.one, actual.one)
                + Term(expected.two, actual.two)
                + Term(expected.three, actual.three)
                + Term(expected.four, actual.four)
                + Term(expected.five, actual.five)
                + Term(expected.six, actual.six)
                + Term(expected.seven, actual.seven)
                + Term(expected.eight, actual.eight)
                + Term(expected.nine, actual.nine)
                + IF(pos = 1, 0, Term(expected.zero, actual.zero));
    END;

    // Insert the chi-squared results
    #UNIQUENAME(chiSquaredResult);
    LOCAL %chiSquaredResult% := PROJECT
        (
            %interimResult%,
            TRANSFORM
                (
                    RECORDOF(LEFT),
                    SELF.chi_squared := IF(LEFT.%idField% > 0, %ComputeChiSquared%(%expectedDS%[1], LEFT, %clampedDigit%), %CHI_SQUARED_CRITICAL_VALUE%),
                    SELF := LEFT
                )
        );

    // Rewrite the result into our final format; changes:
    //  - Sort by ID field to put rows into the proper order
    //  - Remove the ID field
    //  - Make the attribute field first
    #UNIQUENAME(finalResult);
    LOCAL %finalResult% := PROJECT
        (
            SORT(%chiSquaredResult%, %idField%),
            {
                STRING attribute,
                RECORDOF(%chiSquaredResult%) - [%idField%, attribute]
            }
        );

    RETURN %finalResult%;
ENDMACRO;
