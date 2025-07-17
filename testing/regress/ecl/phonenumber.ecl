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
  STRING country_code;
  BOOLEAN tr_valid;
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
  {'+1 800 555 1212', 'US', true, 'US toll-free number'},
  
  // Invalid UK numbers
  {'+44 20 123', 'GB', false, 'UK number too short'},
  {'+44 20 123456789012', 'GB', false, 'UK number too long'},
  {'+44 99 7946 0958', 'GB', false, 'UK invalid area code'},
  {'44 20 7946 0958', 'GB', false, 'UK missing international prefix'},
  {'+44 0 7946 0958', 'GB', false, 'UK invalid leading zero'},
  
  // Invalid German numbers
  {'+49 30 123', 'DE', false, 'German number too short'},
  {'+49 30 123456789012345', 'DE', false, 'German number too long'},
  {'+49 99 123456', 'DE', false, 'German invalid area code'},
  {'49 30 123456', 'DE', false, 'German missing international prefix'},
  {'+49 0 123456', 'DE', false, 'German invalid area code format'},
  
  // Invalid Japanese numbers
  {'+81 3 123', 'JP', false, 'Japanese number too short'},
  {'+81 3 123456789012345', 'JP', false, 'Japanese number too long'},
  {'+81 0 1234 5678', 'JP', false, 'Japanese invalid area code'},
  {'81 3 1234 5678', 'JP', false, 'Japanese missing international prefix'},
  {'+81 99 1234 5678', 'JP', false, 'Japanese invalid area code'},
  
  // Invalid Chinese numbers
  {'+86 10 123', 'CN', false, 'Chinese number too short'},
  {'+86 10 123456789012345', 'CN', false, 'Chinese number too long'},
  {'+86 99 1234 5678', 'CN', false, 'Chinese invalid area code'},
  {'86 10 1234 5678', 'CN', false, 'Chinese missing international prefix'},
  {'+86 0 1234 5678', 'CN', false, 'Chinese invalid area code format'},
  
  // Invalid French numbers
  {'+33 1 42', 'FR', false, 'French number too short'},
  {'+33 1 42686853001234567', 'FR', false, 'French number too long'},
  {'+33 9 42 68 53 00', 'FR', false, 'French invalid area code'},
  {'33 1 42 68 53 00', 'FR', false, 'French missing international prefix'},
  {'+33 0 42 68 53 00', 'FR', false, 'French invalid leading zero'},
  
  // Invalid Italian numbers
  {'+39 06 698', 'IT', false, 'Italian number too short'},
  {'+39 06 69880000123456789', 'IT', false, 'Italian number too long'},
  {'+39 00 698 80000', 'IT', false, 'Italian invalid area code'},
  {'39 06 698 80000', 'IT', false, 'Italian missing international prefix'},
  {'+39 99 698 80000', 'IT', false, 'Italian invalid area code'},
  
  // Additional invalid international numbers
  {'+1', 'US', false, 'Incomplete US number'},
  {'+44', 'GB', false, 'Incomplete UK number'},
  {'+49', 'DE', false, 'Incomplete German number'},
  {'+81', 'JP', false, 'Incomplete Japanese number'},
  {'+86', 'CN', false, 'Incomplete Chinese number'},
  {'+33', 'FR', false, 'Incomplete French number'},
  {'+39', 'IT', false, 'Incomplete Italian number'},
  
  // Numbers with wrong country codes
  {'+1 20 7946 0958', 'GB', false, 'UK number with US country code'},
  {'+44 516 924 2448', 'US', false, 'US number with UK country code'},
  {'+49 3 1234 5678', 'JP', false, 'Japanese number with German country code'},
  {'+81 30 123456', 'DE', false, 'German number with Japanese country code'},
  
  // Numbers without '+' prefix - Valid international format without plus
  {'1 516 924 2448', 'US', true, 'US number without plus prefix'},
  {'44 20 7946 0958', 'GB', true, 'UK London number without plus prefix'},
  {'49 30 123456', 'DE', true, 'German Berlin number without plus prefix'},
  {'81 3 1234 5678', 'JP', true, 'Japanese Tokyo number without plus prefix'},
  {'86 10 1234 5678', 'CN', true, 'Chinese Beijing number without plus prefix'},
  {'33 1 42 68 53 00', 'FR', true, 'French Paris number without plus prefix'},
  {'39 06 698 80000', 'IT', true, 'Italian Rome number without plus prefix'},
  
  // Numbers without '+' prefix - National format
  {'20 7946 0958', 'GB', true, 'UK national format without country code'},
  {'30 123456', 'DE', true, 'German national format without country code'},
  {'3 1234 5678', 'JP', true, 'Japanese national format without country code'},
  {'10 1234 5678', 'CN', true, 'Chinese national format without country code'},
  {'1 42 68 53 00', 'FR', true, 'French national format without country code'},
  {'06 698 80000', 'IT', true, 'Italian national format without country code'},
  
  // Numbers without '+' prefix - Invalid cases
  {'1 555 012 3456', 'US', false, 'US invalid number without plus prefix'},
  {'44 20 123', 'GB', false, 'UK too short without plus prefix'},
  {'49 30 123', 'DE', false, 'German too short without plus prefix'},
  {'81 3 123', 'JP', false, 'Japanese too short without plus prefix'},
  {'86 10 123', 'CN', false, 'Chinese too short without plus prefix'},
  {'33 1 42', 'FR', false, 'French too short without plus prefix'},
  {'39 06 698', 'IT', false, 'Italian too short without plus prefix'},
  
  // Domestic format numbers (no country code at all)
  {'7946 0958', 'GB', true, 'UK domestic format'},
  {'123456', 'DE', true, 'German domestic format'},
  {'1234 5678', 'JP', true, 'Japanese domestic format'},
  {'1234 5678', 'CN', true, 'Chinese domestic format'},
  {'42 68 53 00', 'FR', true, 'French domestic format'},
  {'698 80000', 'IT', true, 'Italian domestic format'},
  
  // Edge cases - numbers that could be ambiguous
  {'123456789', 'US', false, 'Ambiguous 9-digit number'},
  {'1234567890', 'US', true, 'US 10-digit domestic format'},
  {'01234567890', 'GB', true, 'UK with leading zero'},
  {'0123456789', 'DE', true, 'German with leading zero'},
  {'03 1234 5678', 'JP', true, 'Japanese with leading zero'},
  {'010 1234 5678', 'CN', true, 'Chinese with leading zero'},
  {'01 42 68 53 00', 'FR', true, 'French with leading zero'},
  {'06 698 80000', 'IT', true, 'Italian with leading zero'},

  // Omit country codes - test with empty country_code parameter
  {'+1 516 924 2448', '', true, 'US international number without country context'},
  {'+44 20 7946 0958', '', true, 'UK international number without country context'},
  {'+49 30 123456', '', true, 'German international number without country context'},
  {'+81 3 1234 5678', '', true, 'Japanese international number without country context'},
  {'+86 10 1234 5678', '', true, 'Chinese international number without country context'},
  {'+33 1 42 68 53 00', '', true, 'French international number without country context'},
  {'+39 06 698 80000', '', true, 'Italian international number without country context'},
  {'+1 800 555 1212', '', true, 'US toll-free without country context'},
  
  // Omit country codes - ambiguous national numbers
  {'516 924 2448', '', false, 'National US number without country context'},
  {'20 7946 0958', '', false, 'National UK number without country context'},
  {'30 123456', '', false, 'National German number without country context'},
  {'3 1234 5678', '', false, 'National Japanese number without country context'},
  {'10 1234 5678', '', false, 'National Chinese number without country context'},
  {'1 42 68 53 00', '', false, 'National French number without country context'},
  {'06 698 80000', '', false, 'National Italian number without country context'},
  
  // Omit country codes - domestic format numbers
  {'7946 0958', '', false, 'UK domestic format without country context'},
  {'123456', '', false, 'German domestic format without country context'},
  {'1234 5678', '', false, 'Generic domestic format without country context'},
  {'42 68 53 00', '', false, 'French domestic format without country context'},
  {'698 80000', '', false, 'Italian domestic format without country context'},
  
  // Omit country codes - US format numbers (might be defaulted)
  {'(516) 924-2448', '', false, 'US format with parentheses without country context'},
  {'516-924-2448', '', false, 'US format with hyphens without country context'},
  {'516.924.2448', '', false, 'US format with dots without country context'},
  {'5169242448', '', false, 'US format no separators without country context'},
  {'1234567890', '', false, 'US 10-digit format without country context'},
  
  // Omit country codes - invalid international numbers
  {'+1 555 012 3456', '', false, 'Invalid US international without country context'},
  {'+44 20 123', '', false, 'Invalid UK international without country context'},
  {'+49 30 123', '', false, 'Invalid German international without country context'},
  {'+81 3 123', '', false, 'Invalid Japanese international without country context'},
  {'+86 10 123', '', false, 'Invalid Chinese international without country context'},
  {'+33 1 42', '', false, 'Invalid French international without country context'},
  {'+39 06 698', '', false, 'Invalid Italian international without country context'},
  
  // Omit country codes - malformed numbers
  {'123', '', false, 'Too short number without country context'},
  {'12345678901234567890', '', false, 'Too long number without country context'},
  {'abc123def', '', false, 'Non-numeric characters without country context'},
  {'', '', false, 'Empty number without country context'},
  {'+', '', false, 'Just plus sign without country context'},
  {'+999 123 456', '', false, 'Invalid country code without country context'},
  
  // Omit country codes - edge cases
  {'00 1 516 924 2448', '', false, 'International prefix 00 without country context'},
  {'011 1 516 924 2448', '', false, 'US international prefix without country context'},
  {'0044 20 7946 0958', '', false, 'UK with 00 prefix without country context'},
  {'*123#', '', false, 'Service code without country context'},
  {'#123*', '', false, 'Another service code without country context'}

  ], TestRecord);
// end #synthpii


// Test entire dataset against lib_phonenumber plugin calls
testResults := NORMALIZE(testNumbers, 
    lib_phonenumber.phonenumber.parseNumber(LEFT.phonenumber, LEFT.country_code),
    TRANSFORM({TestRecord, lib_phonenumber.phonenumber_data},
        SELF := LEFT,   // Copy original test record fields
        SELF := RIGHT   // Copy parsed phone number data
    )
);

OUTPUT(testResults, NAMED('fullTestResults'));

// Show only Valid Numbers (using the 'valid' field from phonenumber_data)
validNumbers := testResults(valid = true);
OUTPUT(validNumbers, NAMED('validNumbers'));

// Show only Invalid Numbers
invalidNumbers := testResults(valid = false);
OUTPUT(invalidNumbers, NAMED('invalidNumbers'));

// Show phone number type analysis
typeAnalysis := PROJECT(validNumbers, TRANSFORM({
    TestRecord,
    lib_phonenumber.phonenumber_data,
    STRING typeDescription
},
    SELF.typeDescription := CASE(LEFT.lineType,
        lib_phonenumber.phonenumber_type.MOBILE => 'Mobile Phone',
        lib_phonenumber.phonenumber_type.FIXED_LINE => 'Landline',
        lib_phonenumber.phonenumber_type.TOLL_FREE => 'Toll-Free',
        lib_phonenumber.phonenumber_type.VOIP => 'VoIP Service',
        lib_phonenumber.phonenumber_type.PREMIUM_RATE => 'Premium Rate',
        'Other/Unknown'
    );
    SELF := LEFT;
));

OUTPUT(typeAnalysis, NAMED('phoneTypeAnalysis'));

// Show country code distribution
countryStats := TABLE(validNumbers, {
    countryCode,
    regionCode,
    numberCount := COUNT(GROUP)
}, countryCode, regionCode);

OUTPUT(countryStats, NAMED('countryDistribution'));
