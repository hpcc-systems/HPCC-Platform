/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#OPTION('globalFold', FALSE);

IMPORT Std;

regexDS := DATASET
    (
        100000,
        TRANSFORM
            (
                {UNICODE a},
                SELF.a := (UNICODE)RANDOM()
            ),
        DISTRIBUTED
    );

res := PROJECT
    (
        NOFOLD(regexDS),
        TRANSFORM
            (
                {
                    RECORDOF(LEFT),
                    UNICODE via_regex,
                    UNICODE via_find,
                    BOOLEAN is_matching
                },
                SELF.via_regex := REGEXREPLACE(LEFT.a[1], LEFT.a, u'x'),
                SELF.via_find := (UNICODE)Std.Uni.SubstituteIncluded(LEFT.a, LEFT.a[1], u'x'),
                SELF.is_matching := SELF.via_regex = SELF.via_find,
                SELF := LEFT
            ),
        PARALLEL(10)
    );

numTests := COUNT(regexDS);
testsPassed := res(is_matching);
numTestsPassed := COUNT(testsPassed);
testsFailed := res(~is_matching);
numTestsFailed := COUNT(testsFailed);

MIN_PASS_PERCENTAGE := 0.95;

passedPercentage := numTestsPassed / numTests;
isSuccess := passedPercentage >= MIN_PASS_PERCENTAGE;
resultStr := IF(isSuccess, 'PASSED', 'FAILED');
fullResultStr := resultStr + ': ' + (STRING)(ROUND(passedPercentage * 100, 2));

// Output for unit test parsing
OUTPUT(resultStr, NAMED('result'));

// Uncomment the following to see details
// OUTPUT(numTests, NAMED('num_tests'));
// OUTPUT(numTestsPassed, NAMED('num_passed'));
// OUTPUT(numTestsFailed, NAMED('num_failed'));
// OUTPUT(fullResultStr, NAMED('result_desc'));
// OUTPUT(testsFailed, NAMED('failed_tests'), ALL);
