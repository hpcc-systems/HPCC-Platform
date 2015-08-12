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
//d := dataset('~local::rkc::person', { string15 f1, qstring15 f2, data15 f3, unicode15 f4, unsigned8 filepos{virtual(fileposition)} }, flat);
d := dataset('~local::rkc::person', { string15 f1, qstring15 f2, data15 f3, varstring15 f4, unsigned8 filepos{virtual(fileposition)} }, flat);

i := index(d, { f1, f2 }, { d } ,'\\home\\person.name_first.key');


string15 searchStrInStr := 'Gavin' : stored('searchStrInStr');
string15 searchStrInQStr := 'Gavin' : stored('searchStrInQStr');
string15 searchQStrInQStr := 'Gavin' : stored('searchQStrInQStr');
string15 searchStrInData := 'Gavin' : stored('searchStrInData');
string15 searchQStrInStr := 'Gavin' : stored('searchQStrInStr');

count(i(f1[1..length(trim(searchStrInStr))] = trim(searchStrInStr)));
count(i(f2[1..length(trim(searchStrInQStr))] = trim(searchStrInQStr)));
//count(i(f3 = search));
//count(i(f4 = search));
count(i((qstring)f1[1..length(trim(searchQStrInQStr))] = (qstring)trim(searchQStrInStr)));      // should not be keyed!
count(i(f2[1..length(trim(searchQStrInQStr))] = (qstring)trim(searchQStrInQStr)));
count(i(f3[1..length(trim(searchStrInData))] = (data)trim(searchStrInData)));
//count(i(f4 = (unicode)search));
count(i((qstring)(f1[1..length(trim(searchQStrInQStr))]) <> (qstring)trim(searchQStrInStr)));       // perverse, but can be keyed!
                                                                                                    // actually you can't because it is effectively a range compare
count(i(f1[1..length(trim(searchStrInStr))] = trim(searchStrInQStr)));  // typo - add a length test

count(i(f1[1..12] = trim(searchStrInStr)));
count(i(f1[1..12] <> trim(searchStrInStr)));
