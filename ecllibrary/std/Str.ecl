/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

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
 * The pattern can contain wildcards '?' (single character) and '*' (multiple character).
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
 * @see                 Std.Str.SubstituteExcluded
 */

EXPORT STRING SubstituteIncluded(STRING src, STRING filter, STRING replace_char) :=
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

EXPORT STRING SubstituteExcluded(STRING src, STRING filter, STRING replace_char) :=
  lib_stringlib.StringLib.StringSubstitute(src, filter, replace_char);

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
 * A contiguous sequence of alphabetic characters is treated as a word.
 * 
 * @param src           The string that is being converted.
 */

EXPORT STRING ToCapitalCase(STRING src) := lib_stringlib.StringLib.StringToCapitalCase(src);

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
 * // x.myString.StartsWith('x') as an alternative syntax would be even better
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
export BOOLEAN EndsWith(STRING src, STRING suffix) := src[LENGTH(TRIM(src))-LENGTH(TRIM(suffix))+1..]=suffix;


/**
 * Removes the suffix from the search string, if present, and returns the result.  Trailing spaces are 
 * stripped from both strings before matching.
 * 
 * @param src           The string being searched in.
 * @param suffix        The prefix to search for.
 */
export STRING RemoveSuffix(STRING src, STRING suffix) :=
            IF(EndsWith(src, suffix), src[1..length(trim(src))-length(trim(suffix))], src);


EXPORT STRING ExtractMultiple(string src, unsigned8 mask) := lib_stringlib.StringLib.StringExtractMultiple(src, mask);

/**
 * Returns the number of words that the string contains.  Words are separated by one or more separator strings. No 
 * spaces are stripped from either string before matching.
 * 
 * @param src           The string being searched in.
 * @param separator     The string used to separate words
 */

export UNSIGNED4 CountWords(STRING src, STRING separator) := BEGINC++
    if (lenSrc == 0)
        return 0;
            
    if ((lenSeparator == 0) || (lenSrc < lenSeparator))
        return 1;
    
    unsigned numWords=0;
    const char * end = src + lenSrc;
    const char * max = end - (lenSeparator - 1);
    const char * cur = src;
    const char * startWord = NULL;
    //MORE: optimize lenSeparator == 1!
    while (cur < max)
    {
        if (memcmp(cur, separator, lenSeparator) == 0)
        {
            if (startWord)
            {
                numWords++;
                startWord = NULL;
            }
            cur += lenSeparator;
        }
        else
        {
            if (!startWord)
                startWord = cur;
            cur++;
        }
    }
    if (startWord || (cur != end))
        numWords++;
    return numWords;
ENDC++;


SHARED UNSIGNED4 calcWordSetSize(STRING src, STRING separator) := BEGINC++
    if (lenSrc == 0)
        return 0;
            
    if ((lenSeparator == 0) || (lenSrc < lenSeparator))
        return sizeof(size32_t) + lenSrc;
    
    unsigned sizeWords=0;
    const char * end = src + lenSrc;
    const char * max = end - (lenSeparator - 1);
    const char * cur = src;
    const char * startWord = NULL;
    //MORE: optimize lenSeparator == 1!
    while (cur < max)
    {
        if (memcmp(cur, separator, lenSeparator) == 0)
        {
            if (startWord)
            {
                sizeWords += sizeof(size32_t) + (cur - startWord);
                startWord = NULL;
            }
            cur += lenSeparator;
        }
        else
        {
            if (!startWord)
                startWord = cur;
            cur++;
        }
    }
    if (startWord || (cur != end))
    {
        if (!startWord)
            startWord = cur;
        sizeWords += sizeof(size32_t) + (end - startWord);
    }
    return sizeWords;
ENDC++;


//Should be moved into the stringlib helper dll + single character case optimized.
SHARED SET OF STRING doSplitWords(STRING src, STRING separator, unsigned calculatedSize) := BEGINC++
    char * result = static_cast<char *>(rtlMalloc(calculatedsize));
    __isAllResult = false;
    __lenResult = calculatedsize;
    __result = result;
    
    if (lenSrc == 0)
        return;
            
    if ((lenSeparator == 0) || (lenSrc < lenSeparator))
    {
        rtlWriteSize32t(result, lenSrc);
        memcpy(result+sizeof(size32_t), src, lenSrc);
        return;
    }
    
    unsigned sizeWords=0;
    const char * end = src + lenSrc;
    const char * max = end - (lenSeparator - 1);
    const char * cur = src;
    const char * startWord = NULL;
    //MORE: optimize lenSeparator == 1!
    while (cur < max)
    {
        if (memcmp(cur, separator, lenSeparator) == 0)
        {
            if (startWord)
            {
                size32_t len = (cur - startWord);
                rtlWriteSize32t(result, len);
                memcpy(result+sizeof(size32_t), startWord, len);
                result += sizeof(size32_t) + len;
                startWord = NULL;
            }
            cur += lenSeparator;
        }
        else
        {
            if (!startWord)
                startWord = cur;
            cur++;
        }
    }
    if (startWord || (cur != end))
    {
        if (!startWord)
            startWord = cur;
        size32_t len = (end - startWord);
        rtlWriteSize32t(result, len);
        memcpy(result+sizeof(size32_t), startWord, len);
    }
ENDC++;

/**
 * Returns the list of words extracted from the string.  Words are separated by one or more separator strings. No 
 * spaces are stripped from either string before matching.
 * 
 * @param src           The string being searched in.
 * @param separator     The string used to separate words
 */
 
EXPORT SET OF STRING SplitWords(STRING src, STRING separator) := doSplitWords(src, separator, calcWordSetSize(src, separator));


/**
 * Returns the minimum edit distance between the two strings.  An insert change or delete counts as a single edit.
 * The two strings are trimmed before comparing.
 * 
 * @param _left         The first string to be compared.
 * @param _right        The second string to be compared.
 * @return              The minimum edit distance between the two strings.
 */

EXPORT UNSIGNED4 EditDistance(string _left, string _right) :=
    lib_stringlib.StringLib.EditDistanceV2(_left, _right);

/**
 * Returns true if the minimum edit distance between the two strings is with a specific range.
 * The two strings are trimmed before comparing.
 * 
 * @param _left         The first string to be compared.
 * @param _right        The second string to be compared.
 * @param radius        The maximum edit distance that is accepable.
 * @return              Whether or not the two strings are within the given specified edit distance.
 */

EXPORT BOOLEAN EditDistanceWithinRadius(string _left, string _right, unsigned4 radius) :=
    lib_stringlib.StringLib.EditDistanceWithinRadiusV2(_left, _right, radius);


/**
 * Returns the number of words in the string.  Words are separated by one or more spaces.
 * 
 * @param text          The string to be broken into words.
 * @return              The number of words in the string.
 */

EXPORT unsigned4 WordCount(string text) :=
    lib_stringlib.StringLib.StringWordCount(text);

/**
 * Returns the n-th word from the string.  Words are separated by one or more spaces.
 * 
 * @param text          The string to be broken into words.
 * @param n             Which word should be returned from the function.
 * @return              The number of words in the string.
 */

EXPORT string GetNthWord(string text, unsigned4 n) :=
    lib_stringlib.StringLib.StringGetNthWord(text, n);

END;
