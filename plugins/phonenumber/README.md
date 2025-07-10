# Phone Number Plugin

This plugin provides phone number validation and information extraction capabilities for the HPCC Systems platform, utilizing Google's libphonenumber library.

## Overview

The phonenumber plugin offers comprehensive phone number processing capabilities for ECL applications. It leverages Google's industry-standard libphonenumber library to provide accurate validation, formatting, and metadata extraction for international phone numbers.

### Key Features

- **Phone number validation** - Determine if a number is valid for its region
- **Type classification** - Identify line types (mobile, fixed line, toll-free, etc.)
- **Region/country code extraction** - Get ISO country codes and numeric country codes
- **International formatting** - Parse and format numbers in E164 standard
- **Comprehensive parsing** - Single function to extract all phone number metadata

## Dependencies

- Google libphonenumber library
- Boost (date_time, system, thread)
- CMake build system
- HPCC Platform runtime

## Installation

The plugin is built automatically when phone number support is enabled in the CMake configuration:

```bash
# CMake flags which alter plugin lib_phonenumber builds
  -DINCLUDE_PLUGINS=ON
  -DPHONENUEMBER=ON 
  -DINCLUDE_PHONENUMBER=ON
  -DSUPPRESS_PHONENUMBER=ON
```

## ECL Interface

### Import Statement

To use the plugin in your ECL code, import the library:

```ecl
IMPORT lib_phonenumber;
```

### Data Types

The plugin defines the following ECL types:

#### phonenumber_type Enum

```ecl
phonenumber_type := ENUM(INTEGER1,
    FIXED_LINE=0,           // Traditional landline
    MOBILE,                 // Mobile/cellular number
    FIXED_LINE_OR_MOBILE,   // Could be either type
    TOLL_FREE,              // Toll-free number (800, 888, etc.)
    PREMIUM_RATE,           // Premium rate service
    SHARED_COST,            // Shared cost service
    VOIP,                   // Voice over IP number
    PERSONAL_NUMBER,        // Personal numbering service
    PAGER,                  // Pager number
    UAN,                    // Universal Access Number
    VOICEMAIL,              // Voicemail access number
    UNKNOWN                 // Type cannot be determined
);
```
#### phonenumber_error Enum

```ecl
phonenumber_error := ENUM(INTEGER1,
    NO_PARSING_ERROR=0,
    INVALID_COUNTRY_CODE_ERROR,
    NOT_A_NUMBER,
    TOO_SHORT_AFTER_IDD,
    TOO_SHORT_NSN,
    TOO_LONG_NSN
);
```

#### phonenumber_data Record

```ecl
phonenumber_data := RECORD
    STRING number{MAXLENGTH(30)};      // Formatted phone number (E164)
    BOOLEAN valid;                     // Whether number is valid
    phonenumber_type lineType;         // Type of phone line
    STRING regionCode{MAXLENGTH(5)};   // ISO country code (e.g., "US", "GB")
    INTEGER2 countryCode;              // Numeric country code (e.g., 1, 44)
END;
```

### Functions

#### parseNumber

The primary function that performs comprehensive phone number parsing:

```ecl
DATASET(phonenumber_data) parseNumber(CONST STRING phonenumber, CONST STRING countryCode)
```

**Parameters:**
- `phonenumber`: The phone number string to parse (can include formatting)
- `countryCode`: Two-letter ISO country code for parsing context (e.g., 'US', 'GB', 'DE')

**Returns:** A dataset containing one `phonenumber_data` record with all extracted information

**Example:**
<!--#synthpii-->
```ecl
IMPORT lib_phonenumber;

// Parse a US phone number
result := lib_phonenumber.phonenumber.parseNumber('+1 516 924 2448', 'US');
OUTPUT(result);

// Access the parsed data
phoneData := result[1];  // Get the first (and only) record
OUTPUT(phoneData.number);        // "+15169242448" (E164 format)
OUTPUT(phoneData.valid);         // TRUE
OUTPUT(phoneData.lineType);      // MOBILE or FIXED_LINE
OUTPUT(phoneData.regionCode);    // "US"
OUTPUT(phoneData.countryCode);   // 1
```
<!--#synthpii-->

## Usage Examples

### Basic Phone Number Parsing
<!--#synthpii-->
```ecl
IMPORT lib_phonenumber;

// Parse a single phone number
phoneData := lib_phonenumber.phonenumber.parseNumber('(516) 924-2448', 'US')[1];

OUTPUT(phoneData.number);        // "+15169242448"
OUTPUT(phoneData.valid);         // TRUE
OUTPUT(phoneData.lineType);      // Phone type enum value
OUTPUT(phoneData.regionCode);    // "US"
OUTPUT(phoneData.countryCode);   // 1
```
<!--#synthpii-->


### Batch Processing Phone Numbers
<!--#synthpii-->
```ecl
IMPORT lib_phonenumber;

// Input dataset with phone numbers
PhoneRecord := RECORD
    STRING phone;
    STRING country;
    STRING description;
END;

testNumbers := DATASET([
    {'+1 516 924 2448', 'US', 'Valid US number'},
    {'+44 20 7946 0958', 'GB', 'Valid UK number'},
    {'555 012 3456', 'US', 'Invalid US number'},
    {'+49 30 123456', 'DE', 'German number'}
], PhoneRecord);

// Parse all numbers using NORMALIZE
parsedResults := NORMALIZE(testNumbers, 
    lib_phonenumber.phonenumber.parseNumber(LEFT.phone, LEFT.country),
    TRANSFORM({PhoneRecord, lib_phonenumber.phonenumber_data},
        SELF := LEFT,  // Copy original fields
        SELF := RIGHT  // Copy parsed data
    )
);

OUTPUT(parsedResults);
```
<!--#synthpii-->

### Filtering Valid Numbers

```ecl
IMPORT lib_phonenumber;

// Parse and filter to only valid numbers
validNumbers := NORMALIZE(testNumbers, 
    lib_phonenumber.phonenumber.parseNumber(LEFT.phone, LEFT.country),
    TRANSFORM({PhoneRecord, lib_phonenumber.phonenumber_data},
        SELF := LEFT,
        SELF := RIGHT
    )
)(valid = TRUE);  // Filter to only valid numbers

OUTPUT(validNumbers, NAMED('ValidPhoneNumbers'));
```

### Phone Number Type Analysis

```ecl
IMPORT lib_phonenumber;

// Analyze phone number types
phoneAnalysis := PROJECT(testNumbers, TRANSFORM({
    PhoneRecord,
    lib_phonenumber.phonenumber_data,
    STRING typeDescription
},
    phoneData := lib_phonenumber.phonenumber.parseNumber(LEFT.phone, LEFT.country)[1];
    SELF.typeDescription := CASE(phoneData.lineType,
        lib_phonenumber.phonenumber_type.MOBILE => 'Mobile Phone',
        lib_phonenumber.phonenumber_type.FIXED_LINE => 'Landline',
        lib_phonenumber.phonenumber_type.TOLL_FREE => 'Toll-Free',
        lib_phonenumber.phonenumber_type.VOIP => 'VoIP Service',
        'Other/Unknown'
    );
    SELF := LEFT;
    SELF := phoneData;
));

OUTPUT(phoneAnalysis);
```

## Testing

Comprehensive tests are available in the HPCC Platform test suite:

```bash
# Run the phone number plugin tests
ecl run thor testing/regress/ecl/phonenumber.ecl
```

## Performance Considerations

- The plugin uses Google's optimized libphonenumber library
- Results can be cached for repeated operations on the same numbers
- Batch processing with NORMALIZE is more efficient than individual calls
- Consider pre-filtering obviously invalid formats before parsing

## Supported Regions

The plugin supports all regions covered by Google's libphonenumber library, including:

- **North America**: US, CA (country codes +1)
- **Europe**: GB (+44), DE (+49), FR (+33), IT (+39), ES (+34)
- **Asia**: JP (+81), CN (+86), IN (+91), KR (+82)
- **Oceania**: AU (+61), NZ (+64)
- **And 200+ other countries and territories**

For the complete list, refer to the [libphonenumber documentation](https://github.com/google/libphonenumber).

## Contributing

When contributing to this plugin:

1. Follow the HPCC Platform coding standards in `devdoc/StyleGuide.md`
2. Add appropriate tests to the test suite
3. Update this README for new functionality
4. Ensure compatibility across supported platforms

## License

Licensed under the Apache License, Version 2.0. See the HPCC Systems Platform LICENSE.txt file.