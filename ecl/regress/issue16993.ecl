Put(filePath, rec) := FUNCTIONMACRO
    IMPORT Std;
    LOCAL recStr := '{' + (STRING)TOJSON(rec) + '}';
    RETURN Std.File.SetFileDescription(filePath, recStr);
ENDMACRO;

Get_Bad(filePath, recLayout) := FUNCTIONMACRO
    IMPORT Std;
    LOCAL desc := Std.File.GetFileDescription(filePath);
    RETURN FROMJSON
        (
            recLayout,
            desc,
            ONFAIL(TRANSFORM(recLayout, SELF := []))
        );
ENDMACRO;

Get_Good(filePath, recLayout) := FUNCTIONMACRO
    IMPORT Std;
    LOCAL desc := Std.File.GetFileDescription(filePath);
    LOCAL Convert(STRING j) := FROMJSON
        (
            recLayout,
            j,
            ONFAIL(TRANSFORM(recLayout, SELF := []))
        );
    RETURN Convert(desc);
ENDMACRO;

//------------------------------------------------------

PATH := '~some_existing_file_path';
DataLayout := {UNSIGNED4 n};

// Insert rec into description
Put(PATH, ROW({1000}, DataLayout));

// Extract rec
// OUTPUT(Get_Good(PATH, DataLayout));
OUTPUT(Get_Bad(PATH, DataLayout));
