RCCodes(STRING5 Code) := MODULE
EXPORT Dct := DICTIONARY([
        ],{ STRING5 Code => STRING GroupType, STRING Description }) : ONCE;
EXPORT GroupType := Dct[Code].GroupType;
EXPORT Description := Dct[Code].Description;
END;

Description := RCCodes('23456').Description;
output (Description);
