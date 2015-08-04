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

export htf_keys:= macro

htf_flat1 := RECORD
  //unsigned integer4 zip;
  string2 zip;
END;

rawfile := dataset('~.::htf_flat', { htf_flat1, unsigned8 __filepos { virtual(fileposition)}}, THOR);

zip_index := index(rawfile (NOT ISNULL(ZIP)), {ZIP, __filepos}, '~thor::htf::key.zip.tftotal.lname');

set of string5 _ZIP := [] : stored('zip');

zip_lookup := table(zip_index (ZIP in _ZIP), {__filepos});

output(zip_lookup)

endmacro;


htf_keys();

