//class=codesigning

IMPORT STD, $.signing;

// Signing test for embedded code in functions and macros
// All tests should compile with the --allowsigned=cpp option

signing.Outer_Nested_Macro_Signed(4);

// ============================================================

signing.Outer_Macro_Signed(4);

// ============================================================

signing.Outer_Function_Signed(4);

// ============================================================

unsigned_macro(num) := FUNCTIONMACRO
    RETURN signing.Outer_Macro_Signed(num);
ENDMACRO;

unsigned_macro(4);

// ============================================================

ds1 := DATASET([{1}], {UNSIGNED1 n});
profileResults := STD.DataPatterns.Profile(ds1);
OUTPUT(profileResults, ALL, NAMED('profileResults'));

// ============================================================

ds2 := DATASET([{1}], {UNSIGNED1 n});

benfordResult := Std.DataPatterns.Benford(ds2, 'n');

OUTPUT(benfordResult, NAMED('benfordResult'), ALL);

// ============================================================

STD.Date.SecondsToString(100000);
ds3 := DATASET
     (
         [
             {120000, 'CT'},
             {120000, 'ET'}
         ],
         {Std.Date.Time_t time, STRING tz}
     );
 
utcOffsetDS := Std.Date.TimeZone.AppendTZOffset(ds3, tz, seconds_to_utc);
OUTPUT(utcOffsetDS, NAMED('offset_to_utc_result'));
 
ptOffsetDS := Std.Date.TimeZone.AppendTZOffset
     (
         ds3,
         tz,
         seconds_to_pacific_time,
         toTimeZoneAbbrev := 'PT',
         toLocation := 'NORTH AMERICA'
     );
OUTPUT(ptOffsetDS, NAMED('offset_to_pacific_time_result'));
