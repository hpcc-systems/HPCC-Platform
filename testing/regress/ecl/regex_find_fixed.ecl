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

inDS := DATASET(['Colorless green ideas sleep furiously.'], {STRING s});

// UTF-8 not included because the concept of a fixed-length
// UTF-8 string does not make sense

// buffer_x:    replacement occurs entirely within target buffer
// alloc_x:     replacement requires extra temp buffer

ResLayout := RECORD
    STRING10    bounded_s;
    UNICODE10   bounded_u;
END;

//------------------------------------------

// Search for a word at beginning of string, return entire match
STRING first_word_0_ps := '^\\w+' : STORED('first_word_0_ps');
UNICODE first_word_0_pu := u'^\\p{L}+' : STORED('first_word_0_pu');

first_word_0 := PROJECT
    (
        NOFOLD(inDS),
        TRANSFORM
            (
                ResLayout,
                SELF.bounded_s := NOFOLD(REGEXFIND(first_word_0_ps, (STRING)LEFT.s, 0)),
                SELF.bounded_u := NOFOLD(REGEXFIND(first_word_0_pu, (UNICODE)LEFT.s, 0))
            )
    );
OUTPUT(first_word_0, NAMED('first_word_0'));

//------------------------------------------

// Search for two words at beginning of string, return only first word
STRING two_words_1_ps := '^(\\w+) (\\w+)' : STORED('two_words_1_ps');
UNICODE two_words_1_pu := u'^(\\p{L}+) (\\p{L}+)' : STORED('two_words_1_pu');

two_words_1 := PROJECT
    (
        NOFOLD(inDS),
        TRANSFORM
            (
                ResLayout,
                SELF.bounded_s := NOFOLD(REGEXFIND(two_words_1_ps, (STRING)LEFT.s, 1)),
                SELF.bounded_u := NOFOLD(REGEXFIND(two_words_1_pu, (UNICODE)LEFT.s, 1))
            )
    );
OUTPUT(two_words_1, NAMED('two_words_1'));

//------------------------------------------

// Search for two words at beginning of string, return only second word
STRING two_words_2_ps := '^(\\w+) (\\w+)' : STORED('two_words_2_ps');
UNICODE two_words_2_pu := u'^(\\p{L}+) (\\p{L}+)' : STORED('two_words_2_pu');

two_words_2 := PROJECT
    (
        NOFOLD(inDS),
        TRANSFORM
            (
                ResLayout,
                SELF.bounded_s := NOFOLD(REGEXFIND(two_words_2_ps, (STRING)LEFT.s, 2)),
                SELF.bounded_u := NOFOLD(REGEXFIND(two_words_2_pu, (UNICODE)LEFT.s, 2))
            )
    );
OUTPUT(two_words_2, NAMED('two_words_2'));

//------------------------------------------

// Search for two words at beginning of string, return only third word (which does not exist)
STRING two_words_3_ps := '^(\\w+) (\\w+)' : STORED('two_words_3_ps');
UNICODE two_words_3_pu := u'^(\\p{L}+) (\\p{L}+)' : STORED('two_words_3_pu');

two_words_3 := PROJECT
    (
        NOFOLD(inDS),
        TRANSFORM
            (
                ResLayout,
                SELF.bounded_s := NOFOLD(REGEXFIND(two_words_3_ps, (STRING)LEFT.s, 3)),
                SELF.bounded_u := NOFOLD(REGEXFIND(two_words_3_pu, (UNICODE)LEFT.s, 3))
            )
    );
OUTPUT(two_words_3, NAMED('two_words_3'));
