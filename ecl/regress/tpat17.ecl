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


r := record
  string line;
  end;

d := dataset(['one','two','THREE','four','Gavin','Hawthorn','123AGE','WHEN123']
,r);


token x := pattern('[a-zA-Z]+');
rule xupper := x in pattern('[A-Z]+');
rule xlower := x in pattern('[a-z]+');
rule xgavin := validate(x, matchtext = 'Gavin');
rule xhalliday := validate(x, matchtext = 'Hawthorn');
rule x4     := x length(4);
rule x5_7   := x length(5..7);
rule xfirst := first x;
rule xlast  := x last;
rule xbefore := x before pattern('[0-9]+');
rule xafter := x after pattern('[0-9]+');


rule s := xupper | xlower | xgavin | xhalliday | x4 | x5_7 | xfirst | xlast | xbefore | xafter;

results :=
    record
        MATCHTEXT;
        matched(xupper);
        matched(xlower);
        matched(xgavin);
        matched(xhalliday);
        matched(x4);
        matched(x5_7);
        matched(xfirst);
        matched(xlast);
        matched(xbefore);
        matched(xafter);
    end;

output(PARSE(d,line,s,results));
//output(PARSE(d,line,s,results,parse));
