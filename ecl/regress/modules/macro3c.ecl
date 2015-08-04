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

RETURN MODULE

import macro3a,macro3b;

foob := record
string20 fname;
string20 mname;
string20 lname;
end;


df := dataset([{'FRED','P','FOOBAR'},{'MARY','Q','PUBLIC'},
{'ALFRED','D','BUTLER'}],foob);

outfile := df;
output(outfile);

//macro3b.macro2(df,outfile)

export foo_attr1 := outfile;

END;
