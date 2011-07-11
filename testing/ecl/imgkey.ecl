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
output(d1,,'imgfile', overwrite);

d := dataset('imgfile', rawLayout1, FLAT);
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
