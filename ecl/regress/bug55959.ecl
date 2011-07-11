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
