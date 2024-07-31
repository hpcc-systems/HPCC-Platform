/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.
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

#OPTION('globalFold', FALSE);

//------------------------------------------

inDS := DATASET([{'Daniel', 'Daniel'}], {STRING s1, STRING s2});

// UTF-8 not included because the concept of a fixed-length
// UTF-8 string does not make sense

// buffer_x:    replacement occurs entirely within target buffer
// alloc_x:     replacement requires extra temp buffer

ResLayout := RECORD
    STRING10    buffer_s;
    STRING3     alloc_s;
    UNICODE10   buffer_u;
    UNICODE3    alloc_u;
END;

//------------------------------------------

STRING del_few_chars_ps := '[le]' : STORED('del_few_chars_ps');
UNICODE del_few_chars_pu := u'[le]' : STORED('del_few_chars_pu');

remove_some_chars := PROJECT
    (
        NOFOLD(inDS),
        TRANSFORM
            (
                ResLayout,
                SELF.buffer_s := NOFOLD(REGEXREPLACE(del_few_chars_ps, (STRING)LEFT.s1, '')),
                SELF.alloc_s := NOFOLD(REGEXREPLACE(del_few_chars_ps, (STRING)LEFT.s2, '')),
                SELF.buffer_u := NOFOLD(REGEXREPLACE(del_few_chars_pu, (UNICODE)LEFT.s1, u'')),
                SELF.alloc_u := NOFOLD(REGEXREPLACE(del_few_chars_pu, (UNICODE)LEFT.s2, u''))
            )
    );
OUTPUT(remove_some_chars, NAMED('remove_some_chars'));

//------------------------------------------

STRING del_no_chars_ps := '[[:punct:]]' : STORED('del_no_chars_ps');
UNICODE del_no_chars_pu := u'[[:punct:]]' : STORED('del_no_chars_pu');

remove_zero_chars := PROJECT
    (
        NOFOLD(inDS),
        TRANSFORM
            (
                ResLayout,
                SELF.buffer_s := NOFOLD(REGEXREPLACE(del_no_chars_ps, (STRING)LEFT.s1, '')),
                SELF.alloc_s := NOFOLD(REGEXREPLACE(del_no_chars_ps, (STRING)LEFT.s2, '')),
                SELF.buffer_u := NOFOLD(REGEXREPLACE(del_no_chars_pu, (UNICODE)LEFT.s1, u'')),
                SELF.alloc_u := NOFOLD(REGEXREPLACE(del_no_chars_pu, (UNICODE)LEFT.s2, u''))
            )
    );
OUTPUT(remove_zero_chars, NAMED('remove_zero_chars'));

//------------------------------------------

STRING del_all_chars_ps := '\\w' : STORED('del_all_chars_ps');
UNICODE del_all_chars_pu := u'\\w' : STORED('del_all_chars_pu');

remove_all_chars := PROJECT
    (
        NOFOLD(inDS),
        TRANSFORM
            (
                ResLayout,
                SELF.buffer_s := NOFOLD(REGEXREPLACE(del_all_chars_ps, (STRING)LEFT.s1, '')),
                SELF.alloc_s := NOFOLD(REGEXREPLACE(del_all_chars_ps, (STRING)LEFT.s2, '')),
                SELF.buffer_u := NOFOLD(REGEXREPLACE(del_all_chars_pu, (UNICODE)LEFT.s1, u'')),
                SELF.alloc_u := NOFOLD(REGEXREPLACE(del_all_chars_pu, (UNICODE)LEFT.s2, u''))
            )
    );
OUTPUT(remove_all_chars, NAMED('remove_all_chars'));

//------------------------------------------

STRING double_all_chars_ps := '(\\w)' : STORED('double_all_chars_ps');
UNICODE double_all_chars_pu := u'(\\w)' : STORED('double_all_chars_pu');

double_all_chars := PROJECT
    (
        NOFOLD(inDS),
        TRANSFORM
            (
                ResLayout,
                SELF.buffer_s := NOFOLD(REGEXREPLACE(double_all_chars_ps, (STRING)LEFT.s1, '$1$1')),
                SELF.alloc_s := NOFOLD(REGEXREPLACE(double_all_chars_ps, (STRING)LEFT.s2, '$1$1')),
                SELF.buffer_u := NOFOLD(REGEXREPLACE(double_all_chars_pu, (UNICODE)LEFT.s1, u'$1$1')),
                SELF.alloc_u := NOFOLD(REGEXREPLACE(double_all_chars_pu, (UNICODE)LEFT.s2, u'$1$1'))
            )
    );
OUTPUT(double_all_chars, NAMED('double_all_chars'));
