/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.  All rights reserved.
############################################################################## */


EXPORT Metaphone := MODULE


IMPORT lib_metaphone;

/**
 * Returns the primary metaphone value
 *
 * @param src           The string whose metphone is to be calculated.
 * @see                 http://en.wikipedia.org/wiki/Metaphone#Double_Metaphone
 */

EXPORT String primary(STRING src) :=
  lib_metaphone.MetaphoneLib.DMetaphone1(src);

/**
 * Returns the secondary metaphone value
 *
 * @param src           The string whose metphone is to be calculated.
 * @see                 http://en.wikipedia.org/wiki/Metaphone#Double_Metaphone
 */

EXPORT String secondary(STRING src) :=
  lib_metaphone.MetaphoneLib.DMetaphone2(src);

/**
 * Returns the double metaphone value (primary and secondary concatenated
 *
 * @param src           The string whose metphone is to be calculated.
 * @see                 http://en.wikipedia.org/wiki/Metaphone#Double_Metaphone
 */

EXPORT String double(STRING src) :=
  lib_metaphone.MetaphoneLib.DMetaphoneBoth(src);

END;
