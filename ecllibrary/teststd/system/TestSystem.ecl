/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std;

EXPORT TestSystem := MODULE

    SHARED CURRENT_MAJOR_0 := (STRING)__ecl_version_major__;
    SHARED CURRENT_MAJOR_1 := CURRENT_MAJOR_0 + '.0';
    SHARED CURRENT_MAJOR_2 := CURRENT_MAJOR_1 + '.0';
    SHARED CURRENT_MAJOR_3 := CURRENT_MAJOR_2 + '-1';

    SHARED CURRENT_MINOR_1 := (STRING)__ecl_version_major__ + '.' + (STRING)__ecl_version_minor__;
    SHARED CURRENT_MINOR_2 := CURRENT_MINOR_1 + '.0';
    SHARED CURRENT_MINOR_3 := CURRENT_MINOR_2 + '-1';

    SHARED CURRENT_SUBMINOR_0 := (STRING)__ecl_version_major__ + '.' + (STRING)__ecl_version_minor__ + '.' + (STRING)__ecl_version_subminor__;
    SHARED CURRENT_SUBMINOR_1 := CURRENT_SUBMINOR_0 + '-1';

    SHARED CURRENT_PLUS_ONE_MAJOR_0 := (STRING)(__ecl_version_major__ + 1);
    SHARED CURRENT_PLUS_ONE_MAJOR_1 := CURRENT_PLUS_ONE_MAJOR_0 + '.0';
    SHARED CURRENT_PLUS_ONE_MAJOR_2 := CURRENT_PLUS_ONE_MAJOR_1 + '.0';
    SHARED CURRENT_PLUS_ONE_MAJOR_3 := CURRENT_PLUS_ONE_MAJOR_2 + '-1';

    SHARED CURRENT_PLUS_ONE_MINOR_1 := (STRING)__ecl_version_major__ + '.' + (STRING)(__ecl_version_minor__ + 1);
    SHARED CURRENT_PLUS_ONE_MINOR_2 := CURRENT_PLUS_ONE_MINOR_1 + '.0';
    SHARED CURRENT_PLUS_ONE_MINOR_3 := CURRENT_PLUS_ONE_MINOR_2 + '-1';

    SHARED CURRENT_PLUS_ONE_SUBMINOR_0 := (STRING)__ecl_version_major__ + '.' + (STRING)__ecl_version_minor__ + '.' + (STRING)(__ecl_version_subminor__ + 1);
    SHARED CURRENT_PLUS_ONE_SUBMINOR_1 := CURRENT_PLUS_ONE_SUBMINOR_0 + '-1';

    EXPORT TestConstant := [

        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_MAJOR_0) = TRUE, CONST);
        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_MAJOR_1) = TRUE, CONST);
        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_MAJOR_2) = TRUE, CONST);
        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_MAJOR_3) = TRUE, CONST);

        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_MINOR_1) = TRUE, CONST);
        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_MINOR_2) = TRUE, CONST);
        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_MINOR_3) = TRUE, CONST);

        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_SUBMINOR_0) = TRUE, CONST);
        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_SUBMINOR_1) = TRUE, CONST);

        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_PLUS_ONE_MAJOR_0) = FALSE, CONST);
        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_PLUS_ONE_MAJOR_1) = FALSE, CONST);
        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_PLUS_ONE_MAJOR_2) = FALSE, CONST);
        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_PLUS_ONE_MAJOR_3) = FALSE, CONST);

        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_PLUS_ONE_MINOR_1) = FALSE, CONST);
        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_PLUS_ONE_MINOR_2) = FALSE, CONST);
        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_PLUS_ONE_MINOR_3) = FALSE, CONST);

        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_PLUS_ONE_SUBMINOR_0) = FALSE, CONST);
        ASSERT(Std.System.Util.PlatformVersionCheck(CURRENT_PLUS_ONE_SUBMINOR_1) = FALSE, CONST);

        ASSERT(TRUE, CONST)

    ];

    EXPORT Main := [EVALUATE(TestConstant)];

END;
