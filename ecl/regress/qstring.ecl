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

export rtl := SERVICE
    string expandQString(const string physical) : eclrtl,library='eclrtl',entrypoint='rtlExpandQString';
    string compressQString(const string logical, unsigned4 maxLen) : eclrtl,library='eclrtl',entrypoint='rtlCompressQString';
    integer4 compareQString(const string l, const string r) : eclrtl,library='eclrtl',entrypoint='rtlCompareQString';
END;

export display := SERVICE
    echo(const string src) : eclrtl,library='eclrtl',entrypoint='rtlEcho';
END;




ghString(unsigned4 len) :=
    TYPE
        export integer physicalLength := (((len+1)*3) DIV 4);
        export string load(string physical) := rtl.expandQString(physical[1..physicalLength]);
        export string store(string logical) := rtl.compressQString(logical, len);
        export integer4 compare(string l, string r) := rtl.compareQString(l[1..physicalLength], r[1..physicalLength]);
//      export bool equal(string l, string r) := rtl.equalQString(l[1..physicalLength], r[1..physicalLength]);
    END;

rec1 :=     RECORD
ghString(20)    forename;
ghString(20)    surname;
            END;


test := nofold(dataset([
                {'Gavin','Hawthorn'},
                {'Richard','Drimbad'},
                {'David','Bayliss'}
                ], rec1));


test2 := test(surname <> 'BAYLISS');

test3 := sort(test2, surname);

output(test3,,'out.d00');


display.echo('!'+rtl.ExpandQString(rtl.CompressQString('Gavin', 10)) + '!');
display.echo('!'+rtl.ExpandQString(rtl.CompressQString('{\nW()!"', 12)) + '!');

display.echo('!'+rtl.ExpandQString(rtl.CompressQString('Gavin', 1)) + '!');
display.echo('!'+rtl.ExpandQString(rtl.CompressQString('Gavin', 2)) + '!');
display.echo('!'+rtl.ExpandQString(rtl.CompressQString('Gavin', 3)) + '!');
display.echo('!'+rtl.ExpandQString(rtl.CompressQString('Gavin', 4)) + '!');
display.echo('!'+rtl.ExpandQString(rtl.CompressQString('Gavin', 5)) + '!');
