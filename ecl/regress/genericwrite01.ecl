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

// The option genericDiskReadWrites enables generic writes, but so does using
// TYPE() arguments in DATASET and OUTPUT statements.
//
// TYPE(<FileFormat> [: option [, option]+])

LAYOUT := {UNSIGNED2 n};
PATH := '~generic_write_test_01';

CreateFile() := FUNCTION
    ds := DATASET(1000, TRANSFORM(LAYOUT, SELF.n := COUNTER));
    RETURN OUTPUT(ds, {ds}, PATH, TYPE(FLAT : REVERSE(TRUE)), OVERWRITE, COMPRESSED);
END;

ReadFile() := FUNCTION
    ds := DATASET(PATH, LAYOUT, TYPE(FLAT : REVERSE(TRUE)), OPT);
    RETURN OUTPUT(ds, NAMED('ds'), ALL);
END;

SEQUENTIAL
    (
        CreateFile(),
        ReadFile()
    );
