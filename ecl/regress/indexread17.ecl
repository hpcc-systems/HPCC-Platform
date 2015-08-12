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

datalibx := service
    string141 addressclean(const string src, const string filter) : eclrtl,library='dab',entrypoint='rtlStringFilter';
end;


Address1 := '41 S TRINGO DRIVE #104';
Address2 := 'DELRAY BEACH FL 33445';

Clean_Address := datalibx.addressclean(Address1,Address2);

d := dataset('~local::rkc::person', { varstring15 name, varunicode10 id, unsigned8 filepos{virtual(fileposition)} }, flat);
i := index(d, {d}, '~key.person');

a1 := i((name in [V'RICHARD',(varstring)(Clean_Address[1..10])]) and id = (varunicode)U'ABC');
output(a1);

a2 := i((name in [(varstring)(Clean_Address[1..10])]));
output(a2);
a3 := i((name = ((varstring)Clean_Address)[1..10]));
output(a3);

