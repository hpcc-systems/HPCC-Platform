/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

//version optRemoteRead=false
//version optRemoteRead=true

#onwarning (4523, ignore);

optRemoteRead := #IFDEFINED(root.optRemoteRead, false);
#option('forceRemoteRead', optRemoteRead);

IMPORT STD;
//import $.setup;
prefix := '~test::';//setup.Files(false, false).QueryFilePrefix;


addrRec := RECORD
 string addr;
END;

rec := RECORD
 string15 fname;
 string15 lname;
 unsigned age;
 DATASET(addrRec) addrs;
END;

thefname := prefix + 'indexlimits_file';
thefname_index := prefix + 'indexlimits_index';
inds := DATASET([{ 'Aaron', 'Jones', 100, [{'somewhere'}]}, {'Adam', 'Smith', 90, [{'somewhere'}]}, {'Bob', 'Brown', 80, [{'somewhere'}]}, {'Brian', 'Brown', 70, [{'somewhere'}]}, {'Charles', 'Dance', 60, [{'somewhere'}]}, {'Charles', 'Gould', 50, [{'somewhere'}]},  {'David', 'Brokenshire', 40, [{'somewhere'}]}, {'Edward', 'Green', 30, [{'somewhere'}]}, {'Egbert', 'Sillyname', 20, [{'somewhere'}]}, {'Freddy', 'Peters', 10, [{'somewhere'}]} ], rec, DISTRIBUTED);
i := INDEX(inds, { fname }, { inds }, thefname_index);

indexReadComboHitRowLimit := LIMIT(LIMIT(i(age >= 50), 1, SKIP)(fname = 'Charles'), 2, SKIP, KEYED); // keyed limit should not be triggered, but row limit should
//indexReadComboHitRowLimit := LIMIT(i(age >= 50), 1, SKIP)(fname = 'Charles'); // keyed limit should not be triggered, but row limit should
//indexReadComboHitRowLimit := LIMIT(LIMIT(i(age >= 50), 2, SKIP, KEYED), 1, SKIP)(fname = 'Charles'); // keyed limit should not be triggered, but row limit should

SEQUENTIAL(
 OUTPUT(inds, , thefname, OVERWRITE);
 BUILDINDEX(i, SORTED, OVERWRITE);

 OUTPUT(indexReadComboHitRowLimit, NAMED('indexReadComboHitRowLimit'));
);
