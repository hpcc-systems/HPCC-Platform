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

#option ('globalFold', false);
#option ('optimizeGraph', false);

r := record
   string9 ssn;
end;

stTrue := true : stored('stTrue');

em := dataset('email',r,flat);
emx := dataset('email',r,flat);

new_em := if(stTrue, em, emx);

rec := record
    string9 ssnx := new_em.ssn;
end;

rec_em := table(new_em, rec);

output(rec_em);

//This code does, and ...

new_em2 := em;

rec2 := record
    string9 ssn := em.ssn;
end;

rec_em2 := table(new_em2, rec);

//This code does.

new_em3 := if(stTrue, em, em);

output(new_em3);
