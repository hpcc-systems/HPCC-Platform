/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT lib_unicodelib;

EXPORT Uni := MODULE

/**
 * Returns the first string with all characters within the second string removed.
 * 
 * @param src           The string that is being tested.
 * @param filter        The string containing the set of characters to be excluded.
 * @see                 Std.Uni.Filter
 */
 
EXPORT unicode FilterOut(unicode src, unicode filter) :=
    lib_unicodelib.UnicodeLib.UnicodeFilterOut(src, filter);

/**
 * Returns the first string with all characters not within the second string removed.
 * 
 * @param src           The string that is being tested.
 * @param filter        The string containing the set of characters to be included.
 * @see                 Std.Uni.FilterOut
 */
 
EXPORT unicode Filter(unicode src, unicode filter) :=
    lib_unicodelib.UnicodeLib.UnicodeFilter(src, filter);

/**
 * Returns the source string with the replacement character substituted for all characters included in the
 * filter string.
 * MORE: Should this be a general string substitution?
 * 
 * @param src           The string that is being tested.
 * @param filter        The string containing the set of characters to be included.
 * @param replace_char  The character to be substituted into the result.
 * @see                 Std.Uni.SubstituteOut
 */

EXPORT unicode SubstituteIncluded(unicode src, unicode filter, unicode replace_char) :=
    lib_unicodelib.UnicodeLib.UnicodeSubstituteOut(src, filter, replace_char);

/**
 * Returns the source string with the replacement character substituted for all characters not included in the
 * filter string.
 * MORE: Should this be a general string substitution?
 * 
 * @param src           The string that is being tested.
 * @param filter        The string containing the set of characters to be included.
 * @param replace_char  The character to be substituted into the result.
 * @see                 Std.Uni.SubstituteIncluded
 */

EXPORT unicode SubstituteExcluded(unicode src, unicode filter, unicode replace_char) :=
    lib_unicodelib.UnicodeLib.UnicodeSubstitute(src, filter, replace_char);

/**
 * Returns the character position of the nth match of the search string with the first string.
 * If no match is found the attribute returns 0.
 * If an instance is omitted the position of the first instance is returned.
 * 
 * @param src           The string that is searched
 * @param sought        The string being sought.
 * @param instance      Which match instance are we interested in?
 */
 
EXPORT UNSIGNED4 Find(unicode src, unicode sought, unsigned4 instance) :=
    lib_unicodelib.UnicodeLib.UnicodeFind(src, sought, instance);

/**
 * Tests if the search string contains the supplied word as a whole word.
 *
 * @param src           The string that is being tested.
 * @param word          The word to be searched for.
 * @param ignore_case   Whether to ignore differences in case between characters.
 */

EXPORT BOOLEAN FindWord(UNICODE src, UNICODE word, BOOLEAN ignore_case=FALSE) := FUNCTION
   return IF (ignore_case,
              REGEXFIND(u'\\b'+word+u'\\b', src, NOCASE),
              REGEXFIND(u'\\b'+word+u'\\b', src));
END;

/**
 * Returns the character position of the nth match of the search string with the first string.
 * If no match is found the attribute returns 0.
 * If an instance is omitted the position of the first instance is returned.
 * 
 * @param src           The string that is searched
 * @param sought        The string being sought.
 * @param instance      Which match instance are we interested in?
 * @param locale_name   The locale to use for the comparison
 */
 
EXPORT UNSIGNED4 LocaleFind(unicode src, unicode sought, unsigned4 instance, varstring locale_name) :=
    lib_unicodelib.UnicodeLib.UnicodeLocaleFind(src, sought, instance, locale_name);

/**
 * Returns the character position of the nth match of the search string with the first string.
 * If no match is found the attribute returns 0.
 * If an instance is omitted the position of the first instance is returned.
 * 
 * @param src           The string that is searched
 * @param sought        The string being sought.
 * @param instance      Which match instance are we interested in?
 * @param locale_name   The locale to use for the comparison
 * @param strength      The strength of the comparison
                        1 ignores accents and case, differentiating only between letters
                        2 ignores case but differentiates between accents.
                        3 differentiates between accents and case but ignores e.g. differences between Hiragana and Katakana
                        4 differentiates between accents and case and e.g. Hiragana/Katakana, but ignores e.g. Hebrew cantellation marks
                        5 differentiates between all strings whose canonically decomposed forms (NFD�Normalization Form D) are non-identical
*/
 
EXPORT UNSIGNED4 LocaleFindAtStrength(unicode src, unicode tofind, unsigned4 instance, varstring locale_name, integer1 strength) :=
    lib_unicodelib.UnicodeLib.UnicodeLocaleFindAtStrength(src, tofind, instance, locale_name, strength);

/**
 * Returns the nth element from a comma separated string.
 * 
 * @param src           The string containing the comma separated list.
 * @param instance      Which item to select from the list.
 */

EXPORT unicode Extract(unicode src, unsigned4 instance) :=
    lib_unicodelib.UnicodeLib.UnicodeExtract(src, instance);

/**
 * Returns the argument string with all upper case characters converted to lower case.
 * 
 * @param src           The string that is being converted.
 */

EXPORT unicode ToLowerCase(unicode src) :=
    lib_unicodelib.UnicodeLib.UnicodeToLowerCase(src);

/**
 * Return the argument string with all lower case characters converted to upper case.
 * 
 * @param src           The string that is being converted.
 */

EXPORT unicode ToUpperCase(unicode src) :=
    lib_unicodelib.UnicodeLib.UnicodeToUpperCase(src);

/**
 * Returns the upper case variant of the string using the rules for a particular locale.
 * 
 * @param src           The string that is being converted.
 * @param locale_name   The locale to use for the comparison
 */

EXPORT unicode ToTitleCase(unicode src) :=
    lib_unicodelib.UnicodeLib.UnicodeToProperCase(src);

/**
 * Returns the lower case variant of the string using the rules for a particular locale.
 * 
 * @param src           The string that is being converted.
 * @param locale_name   The locale to use for the comparison
 */

EXPORT unicode LocaleToLowerCase(unicode src, varstring locale_name) :=
    lib_unicodelib.UnicodeLib.UnicodeLocaleToLowerCase(src, locale_name);

/**
 * Returns the upper case variant of the string using the rules for a particular locale.
 * 
 * @param src           The string that is being converted.
 * @param locale_name   The locale to use for the comparison
 */

EXPORT unicode LocaleToUpperCase(unicode src, varstring locale_name) :=
    lib_unicodelib.UnicodeLib.UnicodeLocaleToUpperCase(src, locale_name);

/**
 * Returns the upper case variant of the string using the rules for a particular locale.
 * 
 * @param src           The string that is being converted.
 * @param locale_name   The locale to use for the comparison
 */

EXPORT unicode LocaleToTitleCase(unicode src, varstring locale_name) :=
    lib_unicodelib.UnicodeLib.UnicodeLocaleToProperCase(src, locale_name);

/**
 * Compares the two strings case insensitively.  Equivalent to comparing at strength 2.
 * 
 * @param src1          The first string to be compared.
 * @param src2          The second string to be compared.
 * @see                 Std.Uni.CompareAtStrength
 */
 
EXPORT integer4 CompareIgnoreCase(unicode src1, unicode src2) :=
    lib_unicodelib.UnicodeLib.UnicodeCompareIgnoreCase(src1, src2);

/**
 * Compares the two strings case insensitively.  Equivalent to comparing at strength 2.
 * 
 * @param src1          The first string to be compared.
 * @param src2          The second string to be compared.
 * @param strength      The strength of the comparison
                        1 ignores accents and case, differentiating only between letters
                        2 ignores case but differentiates between accents.
                        3 differentiates between accents and case but ignores e.g. differences between Hiragana and Katakana
                        4 differentiates between accents and case and e.g. Hiragana/Katakana, but ignores e.g. Hebrew cantellation marks
                        5 differentiates between all strings whose canonically decomposed forms (NFD�Normalization Form D) are non-identical
 * @see                 Std.Uni.CompareAtStrength
*/
 
EXPORT integer4 CompareAtStrength(unicode src1, unicode src2, integer1 strength) :=
    lib_unicodelib.UnicodeLib.UnicodeCompareAtStrength(src1, src2, strength);

/**
 * Compares the two strings case insensitively.  Equivalent to comparing at strength 2.
 * 
 * @param src1          The first string to be compared.
 * @param src2          The second string to be compared.
 * @param locale_name   The locale to use for the comparison
 * @see                 Std.Uni.CompareAtStrength
 */
 
EXPORT integer4 LocaleCompareIgnoreCase(unicode src1, unicode src2, varstring locale_name) :=
    lib_unicodelib.UnicodeLib.UnicodeLocaleCompareIgnoreCase(src1, src2, locale_name);

/**
 * Compares the two strings case insensitively.  Equivalent to comparing at strength 2.
 * 
 * @param src1          The first string to be compared.
 * @param src2          The second string to be compared.
 * @param locale_name   The locale to use for the comparison
 * @param strength      The strength of the comparison
                        1 ignores accents and case, differentiating only between letters
                        2 ignores case but differentiates between accents.
                        3 differentiates between accents and case but ignores e.g. differences between Hiragana and Katakana
                        4 differentiates between accents and case and e.g. Hiragana/Katakana, but ignores e.g. Hebrew cantellation marks
                        5 differentiates between all strings whose canonically decomposed forms (NFD�Normalization Form D) are non-identical
*/

EXPORT integer4 LocaleCompareAtStrength(unicode src1, unicode src2, varstring locale_name, integer1 strength) :=
    lib_unicodelib.UnicodeLib.UnicodeLocaleCompareAtStrength(src1, src2, locale_name, strength);

/**
 * Returns the argument string with all characters in reverse order.
 * Note the argument is not TRIMMED before it is reversed.
 * 
 * @param src           The string that is being reversed.
 */

EXPORT unicode Reverse(unicode src) :=
    lib_unicodelib.UnicodeLib.UnicodeReverse(src);

/**
 * Returns the source string with the replacement string substituted for all instances of the search string.
 * 
 * @param src           The string that is being transformed.
 * @param sought        The string to be replaced.
 * @param replacement   The string to be substituted into the result.
 */

EXPORT unicode FindReplace(unicode src, unicode sought, unicode replacement) :=
    lib_unicodelib.UnicodeLib.UnicodeFindReplace(src, sought, replacement);

/**
 * Returns the source string with the replacement string substituted for all instances of the search string.
 * 
 * @param src           The string that is being transformed.
 * @param sought        The string to be replaced.
 * @param replacement   The string to be substituted into the result.
 * @param locale_name   The locale to use for the comparison
 */

EXPORT unicode LocaleFindReplace(unicode src, unicode sought, unicode replacement, varstring locale_name) :=
    lib_unicodelib.UnicodeLib.UnicodeLocaleFindReplace(src, sought, replacement, locale_name);

/**
 * Returns the source string with the replacement string substituted for all instances of the search string.
 * 
 * @param src           The string that is being transformed.
 * @param sought        The string to be replaced.
 * @param replacement   The string to be substituted into the result.
 * @param locale_name   The locale to use for the comparison
 * @param strength      The strength of the comparison
 */

EXPORT unicode LocaleFindAtStrengthReplace(unicode src, unicode sought, unicode replacement, varstring locale_name, integer1 strength) :=
    lib_unicodelib.UnicodeLib.UnicodeLocaleFindAtStrengthReplace(src, sought, replacement, locale_name, strength);

/**
 * Returns the source string with all accented characters replaced with unaccented.
 * 
 * @param src           The string that is being transformed.
 */

EXPORT unicode CleanAccents(unicode src) :=
    lib_unicodelib.UnicodeLib.UnicodeCleanAccents(src);

/**
 * Returns the source string with all instances of multiple adjacent space characters (2 or more spaces together)
 * reduced to a single space character.  Leading and trailing spaces are removed, and tab characters are converted
 * to spaces.
 * 
 * @param src           The string to be cleaned.
 */

EXPORT unicode CleanSpaces(unicode src) :=
    lib_unicodelib.UnicodeLib.UnicodeCleanSpaces(src);

/**
 * Tests if the search string matches the pattern.
 * The pattern can contain wildcards '?' (single character) and '*' (multiple character).
 * 
 * @param src           The string that is being tested.
 * @param pattern       The pattern to match against.
 * @param ignore_case   Whether to ignore differences in case between characters
 */
 
EXPORT boolean WildMatch(unicode src, unicode _pattern, boolean _noCase) :=
    lib_unicodelib.UnicodeLib.UnicodeWildMatch(src, _pattern, _noCase);

/**
 * Tests if the search string contains each of the characters in the pattern.
 * If the pattern contains duplicate characters those characters will match once for each occurence in the pattern.
 * 
 * @param src           The string that is being tested.
 * @param pattern       The pattern to match against.
 * @param ignore_case   Whether to ignore differences in case between characters
 */
 
EXPORT BOOLEAN Contains(unicode src, unicode _pattern, boolean _noCase) :=
    lib_unicodelib.UnicodeLib.UnicodeContains(src, _pattern, _noCase);

/**
 * Returns the minimum edit distance between the two strings.  An insert change or delete counts as a single edit.
 * The two strings are trimmed before comparing.
 * 
 * @param _left         The first string to be compared.
 * @param _right        The second string to be compared.
 * @param localname     The locale to use for the comparison.  Defaults to ''.
 * @return              The minimum edit distance between the two strings.
 */

EXPORT UNSIGNED4 EditDistance(unicode _left, unicode _right, varstring localename = '') :=
    lib_unicodelib.UnicodeLib.UnicodeLocaleEditDistance(_left, _right, localename);

/**
 * Returns true if the minimum edit distance between the two strings is with a specific range.
 * The two strings are trimmed before comparing.
 * 
 * @param _left         The first string to be compared.
 * @param _right        The second string to be compared.
 * @param radius        The maximum edit distance that is accepable.
 * @param localname     The locale to use for the comparison.  Defaults to ''.
 * @return              Whether or not the two strings are within the given specified edit distance.
 */

EXPORT BOOLEAN EditDistanceWithinRadius(unicode _left, unicode _right, unsigned4 radius, varstring localename = '') :=
    lib_unicodelib.UnicodeLib.UnicodeLocaleEditDistanceWithinRadius(_left, _right, radius, localename);

/**
 * Returns the number of words in the string.  Word boundaries are marked by the unicode break semantics.
 * 
 * @param text          The string to be broken into words.
 * @param localname     The locale to use for the break semantics.  Defaults to ''.
 * @return              The number of words in the string.
 */

EXPORT unsigned4 WordCount(unicode text, varstring localename = '') :=
    lib_unicodelib.UnicodeLib.UnicodeLocaleWordCount(text, localename);

/**
 * Returns the n-th word from the string.  Word boundaries are marked by the unicode break semantics.
 * 
 * @param text          The string to be broken into words.
 * @param n             Which word should be returned from the function.
 * @param localname     The locale to use for the break semantics.  Defaults to ''.
 * @return              The number of words in the string.
 */

EXPORT unicode GetNthWord(unicode text, unsigned4 n, varstring localename = '') :=
    lib_unicodelib.UnicodeLib.UnicodeLocaleGetNthWord(text, n, localename);

/**
 * Returns everything but the string's nth word and some whitespaces. Words are marked by the unicode break semantics.
 * Trailing whitespaes are always removed with the word.
 * Leading whitespaces are only removed with the word if the nth word is the first word.
 * Returns a blank string if there are no words in the source string.
 * Returns the source string if the number of words in the string is less than the n parameter's assigned value.
 *
 * @param text          The string to be broken into words.
 * @param n             Which word should be removed from the string.
 * @param localname     The locale to use for the break semantics.  Defaults to ''.
 * @return              The string excluding the nth word.
 */

EXPORT ExcludeNthWord(unicode text, unsigned4 n, varstring localename = '') :=
    lib_unicodelib.UnicodeLib.UnicodeLocaleExcludeNthWord(text, n, localename);

/**
 * Returns the source string with the all characters that match characters in the search string replaced
 * with the character at the corresponding position in the replacement string.
 *
 * @param src           The string that is being tested.
 * @param search        The string containing the set of characters to be included.
 * @param replacement   The string containing the characters to act as replacements.
 * @return              The string containing the source string but with the translated characters.
 */

EXPORT Translate(unicode text, unicode sear, unicode repl, varstring localename = '') :=
    lib_unicodelib.UnicodeLib.UnicodeLocaleTranslate(text, sear, repl, localename);

END;
