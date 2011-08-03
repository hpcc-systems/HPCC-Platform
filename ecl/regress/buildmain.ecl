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

namesRecord :=
        RECORD
string20            forename;
string20            surname;
string2             nl{virtual(fileposition)};
        END;

mainRecord :=
        RECORD
integer8            sequence;
string20            forename;
string20            surname;
        END;

d := PIPE('pipeRead 500000 20', namesRecord);

mainRecord t(namesRecord l, unsigned4 c) :=
    TRANSFORM
        SELF.sequence := c;
        SELF := l;
    END;

seqd := project(d, t(left, counter));

output(seqd,,'~keyed.d00',overwrite);

