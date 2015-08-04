/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.  All rights reserved.
############################################################################## */


EXPORT Metaphone3 := MODULE

IMPORT lib_metaphone3;

/**
 * Returns the primary metaphone value
 *
 * @param src           The string whose metphone is to be calculated.
 * @see                 http://en.wikipedia.org/wiki/Metaphone#Metaphone_3
 */

EXPORT String primary(STRING src, boolean encodeVowels=false, boolean encodeExact=false, unsigned4 maxLength=0) :=
  lib_metaphone3.Metaphone3Lib.Metaphone3(src, encodeVowels, encodeExact, maxLength);

/**
 * Returns the secondary metaphone value
 *
 * @param src           The string whose metphone is to be calculated.
 * @see                 http://en.wikipedia.org/wiki/Metaphone#Metaphone_3
 */

EXPORT String secondary(STRING src, boolean encodeVowels=false, boolean encodeExact=false, unsigned4 maxLength=0) :=
  lib_metaphone3.Metaphone3Lib.Metaphone3Alt(src, encodeVowels, encodeExact, maxLength);

/**
 * Returns the double metaphone value (primary and secondary concatenated)
 *
 * @param src           The string whose metphone is to be calculated.
 * @see                 http://en.wikipedia.org/wiki/Metaphone#Metaphone_3
 */

EXPORT String double(STRING src, boolean encodeVowels=false, boolean encodeExact=false, unsigned4 maxLength=0) :=
  lib_metaphone3.Metaphone3Lib.Metaphone3Both(src, encodeVowels, encodeExact, maxLength);

END;
