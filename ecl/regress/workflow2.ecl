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

#option ('targetClusterType', 'hthor');

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        seq := 25;
unsigned6       did;
unsigned4       score;
            END;

recs := dataset('out1.d00',namesRecord,thor);

recs1 := recs(seq%4=0);
recs2 := recs(seq%4=1);
recs3 := recs(seq%4=2);
recs4 := recs(seq%4=3);

do_recs(infile, outfile) := macro
outfile := sort(infile, seq);
  endmacro;

do_recs(recs1,rlp1)
do_recs(recs2,rlp2)
do_recs(recs3,rlp3)
do_recs(recs4,rlp4)

rlp := rlp1+rlp2+rlp3+rlp4;

count(rlp(did > 0));
count(rlp(did <> 0,score=100));
count(rlp(did=0,score>0));
count(rlp(did<>0,score=0));
output(rlp,,'doxie_did_regression')

