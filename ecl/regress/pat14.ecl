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

infile := dataset([
        {'RTC AS RECEIVERCONSERVATOR OF CARTERET SAVINGS BANK FA SUCCESSOR IN INTEREST TO OR FORMERLY KNOWN AS WESTERN FEDERA'},
        {''}
        ], { string line });

PATTERN ws := ' ';
PATTERN word_as := ws 'AS' ws;
PATTERN name := min(ANY+);
PATTERN before_as := name word_as;
pattern sentance := before_as ;


results :=
    record
//      MATCHTEXT;
        MATCHTEXT(before_as);
    end;

//Return first matching sentance that we can find
output(PARSE(infile,line,sentance,results));
