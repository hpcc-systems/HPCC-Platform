/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

import lib_phonenumber;

TestRecord := RECORD
  STRING phonenumber;
  STRING countryCode;
  BOOLEAN valid;
  STRING description;
END;

// #synthpii
testNumbers := DATASET([
  {'+1 555 012 3456', 'US', false, 'US with international prefix'},
  {'+1 516 924 2448', 'US', true, 'US with international prefix'},
  {'555 012 3456', 'US', false, 'US without international prefix'},
  {'516 924 2448', 'US', true, 'US without international prefix'},
  {'(555) 012-3456', 'US', false, 'US with parentheses'},
  {'(516) 924-2448', 'US', true, 'US with parentheses'},
  {'+44 20 7946 0958', 'GB', true, 'UK London number'},
  {'+49 30 123456', 'DE', true, 'German Berlin number'},
  {'+81 3 1234 5678', 'JP', true, 'Japanese Tokyo number'},
  {'+86 10 1234 5678', 'CN', true, 'Chinese Beijing number'},
  {'+33 1 42 68 53 00', 'FR', true, 'French Paris number'},
  {'+39 06 698 80000', 'IT', true, 'Italian Rome number'},
  {'+1 800 555 1212', 'US', true, 'US toll-free number'}
  ], TestRecord);
// end #synthpii


// Test entire dataset against lib_phonenumber plugin calls
testResults := PROJECT(testNumbers, TRANSFORM({TestRecord, BOOLEAN isValidNumber, lib_phonenumber.phonenumber_type pType, INTEGER pCountryCode, STRING pRegionCode},
                                               SELF.isValidNumber := lib_phonenumber.phonenumber.isValidNumber(LEFT.phonenumber, LEFT.countryCode),
                                               SELF.pType := lib_phonenumber.phonenumber.getType(LEFT.phonenumber, LEFT.countryCode),
                                               SELF.pCountryCode := lib_phonenumber.phonenumber.getCountryCode(LEFT.phonenumber, LEFT.countryCode),
                                               SELF.pRegionCode := lib_phonenumber.phonenumber.getRegionCode(LEFT.phonenumber, LEFT.countryCode),
                                               SELF := LEFT));

OUTPUT(testResults, NAMED('fullTestResults'));

// Show only Valid Numbers
validNumbers := testResults(isValidNumber = true);
OUTPUT(validNumbers, NAMED('validNumbers'));

// Show only Invalid Numbers
invalidNumbers := testResults(isValidNumber = false);
OUTPUT(invalidNumbers, NAMED('invalidNumbers'));
