/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

import Std.System.Thorlib;
import Std.File AS FileServices;
import Std.Str;
import $.setup;

prefix := setup.Files(false, false).QueryFilePrefix;

filename := prefix+'empty';
r := { string x};

ds := DATASET(0, transform(r, SELF := []));

iclData0 := DATASET(filename, r, flat);

ordered(
    output(ds,,filename, overwrite, compressed);
    output(iclData0);
    FileServices.DeleteLogicalFile(filename,true),
);
