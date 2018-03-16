/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestRepeat := MODULE

   EXPORT TestConst := MODULE

    angstrom := U'A\u030A';         // Single character
    angstrom2d := x'41000A03';      // Bytes for A followed by circle
    angstrom2 := (>unicode<)angstrom2d; // Convert to a unicode, but it will not be normalized
    revangstrom := U'\u030AA';      // circle followed by an A

    EXPORT Tests := [
        ASSERT(Uni.Repeat('Repeat this string ', 0) = '');
        ASSERT(Uni.Repeat('Repeat this string ', 1) = 'Repeat this string ');
        ASSERT(Uni.Repeat('Repeat this string ', 2) = 'Repeat this string Repeat this string');

        ASSERT(Uni.Repeat(U'', 0) = '');
        ASSERT(Uni.Repeat(U'', 1) = '');
        ASSERT(Uni.Repeat(U'', 2) = '');
        ASSERT(Uni.Repeat(U'', 10) = '');
        ASSERT(Uni.Repeat(U'', -2) = '');

        ASSERT(Uni.Repeat(U'r', 0) = '');
        ASSERT(Uni.Repeat(U'r', 1) = 'r');
        ASSERT(Uni.Repeat(U'r', 2) = 'rr');
        ASSERT(Uni.Repeat(U'r', 10) = 'rrrrrrrrrr');
        ASSERT(Uni.Repeat(U'r', -2) = '');

        ASSERT(Uni.Repeat(U'abc', 0) = '');
        ASSERT(Uni.Repeat(U'abc', 1) = 'abc');
        ASSERT(Uni.Repeat(U'abc', 2) = 'abcabc');
        ASSERT(Uni.Repeat(U'abc', 10) = 'abcabcabcabcabcabcabcabcabcabc');
        ASSERT(Uni.Repeat(U'abc', -2) = '');

        //Various checks to ensure that strings are correctly normalized after duplicating
        ASSERT(Uni.Repeat(angstrom, 1) = U'\u212B');
        ASSERT(LENGTH(angstrom) = 1);
        ASSERT(LENGTH(angstrom2) = 2);
        ASSERT(LENGTH(TRIM(angstrom2)) = 2);
        ASSERT(LENGTH(Uni.Repeat(angstrom, 1)) = 1);
        ASSERT(LENGTH(Uni.Repeat(angstrom2, 1)) = 2);
        ASSERT(LENGTH(TRIM(Uni.Repeat(angstrom2, 1))) = 1);
        ASSERT(LENGTH(Uni.Repeat(angstrom2, 2)) = 4);
        ASSERT(LENGTH(TRIM(Uni.Repeat(angstrom2, 2))) = 2);
        ASSERT(Uni.Repeat(angstrom2, 1) = U'\u212B');
        ASSERT(revangstrom[2] = 'A');

        ASSERT(LENGTH(Uni.Repeat(revangstrom, 1)) = 2);
        ASSERT(LENGTH(TRIM(Uni.Repeat(revangstrom   , 1))) = 2);
        ASSERT(LENGTH(Uni.Repeat(revangstrom, 2)) = 4);
        ASSERT(LENGTH(TRIM(Uni.Repeat(revangstrom   , 2))) = 3);
        ASSERT(Uni.Repeat(revangstrom, 2) = U'\u030A\u212bA');

        ASSERT(TRUE)];
   END;
END;