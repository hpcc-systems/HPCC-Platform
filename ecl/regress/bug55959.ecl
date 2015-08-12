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

mylayout := RECORD
    UNICODE term {MAXLENGTH(256)};
    BOOLEAN noise_1;
    BOOLEAN noise_2;
    BOOLEAN noise_3;
END;

noise_words_1 := [U'OF',U'XXX',U'OR'];  // doesn't work
noise_words_2 := [U'OF',U'XXX'];        // works
noise_words_3 := [U'OF',U'XX',U'OR'];   // works

mylayout make_noise(mylayout L) := TRANSFORM
    self.term := l.term;
    self.noise_1 := l.term in noise_words_1;
    self.noise_2 := l.term in noise_words_2;
    self.noise_3 := l.term in noise_words_3; END;

ds := DATASET([{U'OF', false, false, false},{'OF    ',false, false,false}], mylayout);

output(PROJECT(nofold(ds), make_noise(LEFT)));
