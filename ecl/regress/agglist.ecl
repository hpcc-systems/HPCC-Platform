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

one := 1 : stored('one');
two := 2 : stored('two');
three := 3 : stored('three');
four := 4 : stored('four');
five := 5 : stored('five');

lci := [1,4,2,5,3];
lsi := [one, four, two, five, three];
sli := lsi : stored('sli');

count(lci);
exists(lci);
sum(lci);
max(lci);
min(lci);
ave(lci);

count(lsi);
exists(lsi);
sum(lsi);
max(lsi);
min(lsi);
ave(lsi);

count(sli);
exists(sli);
sum(sli);
max(sli);
min(sli);
ave(sli);

set of integer lsi0 := [];

sum(lsi0);
max([lsi0, lsi0]);
min(lsi0);

lsi1 := [one];

sum(lsi1);
max(lsi1);
min(lsi1);

lsi2 := [four, two];

sum(lsi2);
max(lsi2);
min(lsi2);

lsi3 := [one, five, three];

sum(lsi3);
max(lsi3);
min(lsi3);

//----------------------------------------------------------------------------

a := 'a' : stored('a');
b := 'b' : stored('b');
c := 'c' : stored('c');
d := 'd' : stored('d');
e := 'e' : stored('e');


lcs := ['e', 'cde', 'a', 'b', 'd'];
lss := [e, c, a, b, d];
sls := lss : stored('sls');

count(lcs);
exists(lcs);
max(lcs);
min(lcs);

count(lss);
exists(lss);
max(lss);
min(lss);

count(sls);
exists(sls);
max(sls);
min(sls);

//----------------------------------------------------------------------------

data da := D'a' : stored('da');
data db := D'bcd' : stored('db');
data dc := D'c' : stored('dc');
data dd := D'def' : stored('dd');
data de := D'egh' : stored('de');


lsda := [de, dc, da, db, dd];
slda := lsda : stored('slda');


count(slda);
exists(slda);
max(slda);
min(slda);

//----------------------------------------------------------------------------

decimal8_2 decone := 1.2;
decimal8_2 dectwo := 2.3;
decimal8_2 decthree := 3.0;
decimal8_2 decfour := 4.5;
decimal8_2 decfive := 5;

decimal8_2 sdecone := decone : stored('decone');
decimal8_2 sdectwo := dectwo : stored('dectwo');
decimal8_2 sdecthree := decthree : stored('decthree');
decimal8_2 sdecfour := decfour : stored('decfour');
decimal8_2 sdecfive := decfive : stored('decfive');

lcd := [decone, decfour, dectwo, decfive, decthree];
lsd := [sdecone, sdecfour, sdectwo, sdecfive, sdecthree];
sld := lsd : stored('sld');

count(lcd);
exists(lcd);
sum(lcd);
max(lcd);
min(lcd);
ave(lcd);

count(lsd);
exists(lsd);
sum(lsd);
max(lsd);
min(lsd);
ave(lsd);

count(sld);
exists(sld);
sum(sld);
max(sld);
min(sld);
ave(sld);

//----------------------------------------------------------------------------

real8 realone := 1.2;
real8 realtwo := 2.3;
real8 realthree := 3.0;
real8 realfour := 4.5;
real8 realfive := 5;

real8 srealone := realone : stored('realone');
real8 srealtwo := realtwo : stored('realtwo');
real8 srealthree := realthree : stored('realthree');
real8 srealfour := realfour : stored('realfour');
real8 srealfive := realfive : stored('realfive');

lcr := [realone, realfour, realtwo, realfive, realthree];
lsr := [srealone, srealfour, srealtwo, srealfive, srealthree];
slr := lsr : stored('slr');

count(lcr);
exists(lcr);
sum(lcr);
max(lcr);
min(lcr);
ave(lcr);

count(lsr);
exists(lsr);
sum(lsr);
max(lsr);
min(lsr);
ave(lsr);

count(slr);
exists(slr);
sum(slr);
max(slr);
min(slr);
ave(slr);
