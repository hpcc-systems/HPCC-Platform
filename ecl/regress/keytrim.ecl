/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#option ('globalFold', false);
//d := dataset('~local::rkc::person', { string15 f1, qstring15 f2, data15 f3, unicode15 f4, unsigned8 filepos{virtual(fileposition)} }, flat);
d := dataset('~local::rkc::person', { string15 f1, qstring15 f2, data15 f3, varstring15 f4, unsigned8 filepos{virtual(fileposition)} }, flat);

i := index(d, { d } ,'\\seisint\\person.name_first.key');


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
