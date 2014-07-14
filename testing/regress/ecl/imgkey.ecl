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

//noRoxie

vstring(integer i) := TYPE
    export integer physicallength(string s) := i;
    export string load(string s) := s[1..i];
    export string store(string s) := s;
    END;


vdata(integer i) := TYPE
    export integer physicallength(data s) := i;
    export data load(data s) := s[1..i];
    export data store(data s) := s;
    END;

rawLayout := record
    string20 dl;
    string8 date;
    unsigned2 imgLength;
    vdata(SELF.imgLength) jpg;
end;

rawLayout1 := record
// MORE - should not have to copy rawLayout fields in here
// Should just say rawLayout;
    string20 dl;
    string8 date;
    unsigned2 imgLength;
    vdata(SELF.imgLength) jpg;
    unsigned8 _fpos{virtual(fileposition)}
end;

keylayout := record
    string20 dl;
    unsigned2 seq;
    string8 date;
    unsigned2 num;
    unsigned2 imgLength;
    unsigned8 _fpos;
end;

d1 := dataset([
    {'id1', '20030911', 5, x'3132333435'}, 
    {'id2', '20030910', 3, x'313233'}, 
    {'id2', '20030905', 3, x'333231'}], 
    rawLayout);
output(d1,,'~REGRESS::imgfile', overwrite);

d := dataset('~REGRESS::imgfile', rawLayout1, FLAT);
i := index(d, keylayout, 'imgindex');

rawtrim := table(d, { dl, date, unsigned2 seq:=0, unsigned2 num := 0, imgLength, _fpos});
sortedDls := sort(rawtrim, dl, -date);

rawtrim addSequence(rawtrim L, rawtrim R, unsigned c) := TRANSFORM
    SELF.seq := c-1;
    SELF := R;
END;

rawtrim copySequence(rawtrim L, rawtrim R, unsigned c) := TRANSFORM
    SELF.num := IF (c=1, R.seq+1, L.num);
    SELF := R;
END;

groupedDLs := group(sortedDls, dl); // NOTE - GROUP cannot be local
sequenced := iterate(groupedDLs, addSequence(LEFT, RIGHT, counter)); // a grouped iterate to add sequence numbers
sequence_projected := iterate(sort(sequenced, dl, date), copySequence(LEFT, RIGHT, COUNTER));
resorted := SORT(sequence_projected, {dl, seq, date, num, imgLength, _fpos}); // a grouped sort to get into key order

buildindex(resorted, {dl, seq, date, num, imgLength, _fpos} ,'imgindex', overwrite, local, sorted);

string20 _dl := 'id2' : stored('dl');
unsigned _idx := 2 : stored('idx');

// ImgServer query 1
//output(fetch(d, i(dl=_dl/*, seq=1*/), RIGHT._fpos));

// ImgServer query 2
//output(fetch(d, i(dl=_dl, seq=_idx), RIGHT._fpos));


output(i(dl=_dl/*, seq=num*/));
output(i(dl=_dl, seq=0));
output(i(dl=_dl, seq=_idx));
