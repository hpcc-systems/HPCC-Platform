# Phone Number Plugin

This plugin provides phone number validation and information extraction capabilities for the HPCC Systems platform, utilizing Google's libphonenumber library.

## Overview

The phonenumber plugin offers functions to:
- Validate phone numbers
- Extract phone number type (mobile, fixed line, toll-free, etc.)
- Get region/country codes
- Parse international phone numbers

## Dependencies

- Google libphonenumber library
- Boost (date_time, system, thread)
- Abseil (absl)

## Installation

The plugin is built automatically when `INCLUDE_PLUGINS`, `INCLUDE_PHONENUMBER` or `PHONENUMBER` is enabled in the CMake configuration:

## Usage

Import the plugin in your ECL code:

```ecl
IMPORT lib_phonenumber;
```

### Functions

#### isValidNumber
Validates whether a phone number is valid for the given country.

```ecl
BOOLEAN isValidNumber(CONST STRING phonenumber, CONST STRING countryCode)
```

**Parameters:**
- `phonenumber`: The phone number to validate
- `countryCode`: Default Two-letter country code (e.g., 'US', 'GB', 'DE')

**Returns:** `TRUE` if the number is valid, `FALSE` otherwise

**Example:**
<!--#synthpii-->
```ecl
IMPORT lib_phonenumber;

OUTPUT(lib_phonenumber.phonenumber.isValidNumber('+1 555 012 3456', 'US'));
// Returns: FALSE
```

#### getType
Returns the type of phone number (mobile, fixed line, etc.).

```ecl
INTEGER getType(CONST STRING phonenumber, CONST STRING countryCode)
```

**Parameters:**
- `phonenumber`: The phone number to analyze
- `countryCode`: Two-letter country code

**Returns:** Phone number type as Integer:
- `0 = FIXED_LINE` - Fixed line number
- `1 = MOBILE` - Mobile number
- `2 = FIXED_LINE_OR_MOBILE` - Could be either
- `3 = TOLL_FREE` - Toll-free number
- `4 = PREMIUM_RATE` - Premium rate number
- `5 = SHARED_COST` - Shared cost number
- `6 = VOIP` - VoIP number
- `7 = PERSONAL_NUMBER` - Personal number
- `8 = PAGER` - Pager number
- `9 = UAN` - Universal Access Number
- `10 = VOICEMAIL` - Voicemail number
- `11 = UNKNOWN` - Unknown type

**Example:**
<!--#synthpii-->
```ecl
IMPORT lib_phonenumber;

OUTPUT(lib_phonenumber.phonenumber.getType('+1 555 012 3456', 'US'));
// Returns 11 (UNKNOWN)
```

#### getRegionCode
Returns the region code for the phone number.

```ecl
STRING getRegionCode(CONST STRING phonenumber, CONST STRING countryCode)
```

**Parameters:**
- `phonenumber`: The phone number to analyze
- `countryCode`: Two-letter country code for parsing context

**Returns:** Two-letter region code (e.g., 'US', 'GB', 'DE')

**Example:**
<!--#synthpii-->
```ecl
IMPORT lib_phonenumber;

OUTPUT(lib_phonenumber.phonenumber.getRegionCode('+44 20 7946 0958', 'GB'));
// Returns: "GB"
```

#### getCountryCode
Returns the numeric country code for the phone number.

```ecl
INTEGER getCountryCode(CONST STRING phonenumber, CONST STRING countryCode)
```

**Parameters:**
- `phonenumber`: The phone number to analyze
- `countryCode`: Two-letter country code for parsing context

**Returns:** Numeric country code (e.g., 1 for US/Canada, 44 for UK)

**Example:**
<!--#synthpii-->
```ecl
IMPORT lib_phonenumber;

OUTPUT(lib_phonenumber.phonenumber.getCountryCode('+44 20 7946 0958', 'GB'));
// Returns: 44
```

## Examples

### Basic Validation
<!--#synthpii-->
```ecl
IMPORT lib_phonenumber;

// Validate various phone number formats
validUS := lib_phonenumber.phonenumber.isValidNumber('555-123-4567', 'US');
validUK := lib_phonenumber.phonenumber.isValidNumber('+44 20 7946 0958', 'GB');
validInvalid := lib_phonenumber.phonenumber.isValidNumber('123', 'US');

OUTPUT(validUS);     // FALSE
OUTPUT(validUK);     // TRUE
OUTPUT(validInvalid); // FALSE
```

### Phone Number Analysis
<!--#synthpii-->
```ecl
IMPORT lib_phonenumber;

phoneNumber := '+1 800 555 1212';
country := 'US';

phoneType := lib_phonenumber.phonenumber.getType(phoneNumber, country);
regionCode := lib_phonenumber.phonenumber.getRegionCode(phoneNumber, country);
countryCode := lib_phonenumber.phonenumber.getCountryCode(phoneNumber, country);

OUTPUT(phoneType);   // "TOLL_FREE"
OUTPUT(regionCode);  // "US"
OUTPUT(countryCode); // 1
```

### Batch Processing
<!--#synthpii-->
```ecl
IMPORT lib_phonenumber;

PhoneRecord := RECORD
    STRING phone;
    STRING country;
END;

phones := DATASET([
    {'+1 555 123 4567', 'US'},
    {'+44 20 7946 0958', 'GB'},
    {'+49 30 123456', 'DE'},
    {'555-123-4567', 'US'}
], PhoneRecord);

results := PROJECT(phones, TRANSFORM({
    PhoneRecord,
    BOOLEAN isValidNumber,
    lib_phonenumber.phonenumber_type phoneType,
    STRING regionCode,
    INTEGER countryCode
},
    SELF.isValidNumber := lib_phonenumber.phonenumber.isValidNumber(LEFT.phone, LEFT.country),
    SELF.phoneType := lib_phonenumber.phonenumber.getType(LEFT.phone, LEFT.country),
    SELF.regionCode := lib_phonenumber.phonenumber.getRegionCode(LEFT.phone, LEFT.country),
    SELF.countryCode := lib_phonenumber.phonenumber.getCountryCode(LEFT.phone, LEFT.country),
    SELF := LEFT
));

OUTPUT(results);
```

## Testing

Comprehensive tests are available in [`testing/regress/ecl/phonenumber.ecl`](testing/regress/ecl/phonenumber.ecl). Run tests with:

```bash
# Run the test
ecl run thor testing/regress/ecl/phonenumber.ecl
```

## Error Handling

All functions handle errors gracefully:
- Invalid phone numbers return `FALSE` for `isValidNumber`
- Invalid numbers return `"INVALID"` for `getType`
- Parse errors return empty strings for `getRegionCode`
- Parse errors return `0` for `getCountryCode`

## Performance Notes

- The plugin uses Google's libphonenumber library for accurate, up-to-date phone number handling

## Country Codes

Common country codes supported:
- `US` - United States
- `GB` - United Kingdom  
- `DE` - Germany
- `FR` - France
- `IT` - Italy
- `JP` - Japan
- `CN` - China
- `CA` - Canada
- `AU` - Australia
- `IN` - India

For a complete list, refer to the ISO 3166-1 alpha-2 standard.

## Contributing

When contributing to this plugin:
1. Follow the HPCC Platform coding standards
2. Add appropriate tests to [`testing/regress/ecl/phonenumber.ecl`](testing/regress/ecl/phonenumber.ecl)
3. Update this README if adding new functionality
4. Ensure all phone number formats and regions are properly tested

## License

Licensed under the Apache License, Version 2.0. See the HPCCSystems Platform LICENSE.txt file for details.