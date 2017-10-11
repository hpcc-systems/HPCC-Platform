/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.File;

EXPORT TestGetEspURL := MODULE

    EXPORT TestRuntime := MODULE
        // Assumes that there is no user authentication on the test system
        SHARED buildString := SOAPCALL
            (
                File.GetEspURL() + '/WsSMC',
                'Activity',
                {
                    STRING sortby       {XPATH('SortBy')} := 'Name',
                    STRING descending   {XPATH('Descending')} := '1'
                },
                DATASET({STRING activityBuild {XPATH('Build')}}),
                XPATH('ActivityResponse')
            );

        EXPORT TestActivity01 := ASSERT(buildString[1].activityBuild != '');
    END;

END;