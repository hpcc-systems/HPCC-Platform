/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

//class=file
//class=index
//version multiPart=false
//version multiPart=true
//version multiPart=true,useLocal=true
//version multiPart=true,useTranslation=true,nothor

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);

//--- end of version configuration ---

#option ('layoutTranslationEnabled', useTranslation);
#onwarning (4523, ignore);
#onwarning (5402, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

//Substring tests....

integer minus1 := -1 : stored('minus1');

string40 const_blank := '';
string40 const_a := 'A';
string40 const_anderson := 'Anderson';
string40 const_anderson_xxx := 'Anderson                  Rubbish!';

string40 search_blank := '' : stored('blank');
string40 search_a := 'A' : stored('a');
string40 search_anderson := 'Anderson' : stored('anderson');
string40 search_anderson_xxx := 'Anderson                  Rubbish!' : stored('anderson_xxx');

o(dataset(recordof(Files.DG_FetchIndex)) ds) := 
    output(sort(table(ds, { lname, fname }), lname, fname));
    
o0 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(search_blank))] = search_blank)));
o1 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(search_a))] = search_a)));
o2 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(search_anderson))] = search_anderson)));
o3 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(search_anderson_xxx))] = search_anderson_xxx)));

on0 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..1] = search_blank)));
on1 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..1] = search_a)));
on2 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..8] = search_anderson)));
on3 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..8] = search_anderson_xxx)));

//8
ot0 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(search_blank))] = trim(search_blank))));
ot1 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(search_a))] = trim(search_a))));
ot2 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(search_anderson))] = trim(search_anderson))));
ot3 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(search_anderson_xxx))] = trim(search_anderson_xxx))));

//12
ont0 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..1] = trim(search_blank))));
ont1 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..1] = trim(search_a))));
ont2 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..8] = trim(search_anderson))));
ont3 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..8] = trim(search_anderson_xxx))));

//16
ox0 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..minus1] = search_blank)));
ox1 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..minus1] = search_a)));
ox2 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..minus1] = search_anderson)));
ox3 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..minus1] = search_anderson_xxx)));

//20
co0 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(const_blank))] = const_blank)));
co1 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(const_a))] = const_a)));
co2 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(const_anderson))] = const_anderson)));
//co3 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(const_anderson_xxx))] = const_anderson_xxx)));
co3 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(const_anderson))] = const_anderson_xxx)));

//24
con0 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..1] = const_blank)));
con1 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..1] = const_a)));
con2 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..8] = const_anderson)));
con3 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..8] = const_anderson_xxx)));

//28
cot0 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(const_blank))] = trim(const_blank))));
cot1 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(const_a))] = trim(const_a))));
cot2 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(const_anderson))] = trim(const_anderson))));
//cot3 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(const_anderson_xxx))] = trim(const_anderson_xxx))));
cot3 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..length(trim(const_anderson))] = trim(const_anderson_xxx))));

//32
cont0 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..1] = trim(const_blank))));
cont1 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..1] = trim(const_a))));
cont2 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..8] = trim(const_anderson))));
cont3 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..8] = trim(const_anderson_xxx))));

//36
cox0 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..-1] = const_blank)));
cox1 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..-1] = const_a)));
cox2 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..-1] = const_anderson)));
cox3 := o(Files.DG_FetchIndex(wild(fname), keyed(Lname[1..-1] = const_anderson_xxx)));

oz := o(Files.DG_FetchIndex(wild(fname), keyed(Lname between 'z' and 'a')));

//Condition here to make it simple to test a single variant.
if (true,
        sequential(
            o0, o1, o2, o3,
            on0, on1, on2, on3,
            ot0, ot1, ot2, ot3,
            ont0, ont1, ont2, ont3,
            ox0, ox1, ox2, ox3,
            co0, co1, co2, co3,
            con0, con1, con2, con3,
            cot0, cot1, cot2, cot3,
            cont0, cont1, cont2, cont3,
            cox0, cox1, cox2, cox3,
            oz,
            output('done')
        ),
        ox0
);
        
