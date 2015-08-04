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

testrec := RECORD
                          string26 date;
                          unsigned4 n;
                   END;

inds := DATASET('testin',testrec,thor);

set of unsigned4 DaysSinceFirstMonday := [ // NB only 2007-2010 for this benchmark
  0, 31, 61, 92, 120, 151, 181, 212, 243, 273, 304, 334,
  1, 32, 62, 93, 122, 153, 183, 214, 245, 275, 306, 336,
  3, 34, 64, 95, 123, 154, 184, 215, 246, 276, 307, 337 ] ;


unsigned4 WEEKNUMBER(const string26 date) := FUNCTION // NB only for benchmark
  unsigned4 year := (unsigned4)(>string4<)date;
  unsigned4 month := (unsigned4)(>string2<)date[5..6];
  unsigned4 day := (unsigned4)(>string2<)date[8..9];
  unsigned4 totalday := DaysSinceFirstMonday[NOBOUNDCHECK ((year - 2007)*12+month)] + day;
  unsigned4 w1 := totalday DIV 7;
  unsigned4 w2 := IF(totalday%7!=0,w1+1,w1);
  return IF(w2>52,year+1,year)*100+IF(w2>52,1,w2);
end;


testrec T1(testrec r) := TRANSFORM
  //self.n := x[r.n];
  self.n := WEEKNUMBER(r.date);
  self := r;
END;

dsout := PROJECT(inds,T1(LEFT));

OUTPUT(dsout,,'testout');

