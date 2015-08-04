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

