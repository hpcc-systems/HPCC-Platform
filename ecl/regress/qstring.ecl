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
