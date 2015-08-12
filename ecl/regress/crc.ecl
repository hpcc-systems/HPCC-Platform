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

export DataLib := SERVICE
  string    PreferredFirst(const string scr)  : c, pure, entrypoint='dataCalcPreferredFirst';
END;

qstring20 lnameField := '' : stored('lnameField');
qstring20 fnameField := '' : stored('fnameField');

#uniquename(tf) // true first
%tf%(STRING fname) := DataLib.PreferredFirst(fname);

#uniquename(MatchingDistribute)
x := HASH((%tf%(fnameField))) : persist('gchtestb2');

output(x);
