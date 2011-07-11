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

OUTPUT(HASH(0));
OUTPUT(HASH(1234567890));
OUTPUT(HASH(0987654321));
OUTPUT(HASH('abcdefghijklmnopqrstuvwxyz'));
OUTPUT(HASH('!@#$%^&*--_+=-0987654321~`'));
OUTPUT(HASH64(0));
OUTPUT(HASH64(1234567890));
OUTPUT(HASH64(0987654321));
OUTPUT(HASH64('abcdefghijklmnopqrstuvwxyz'));
OUTPUT(HASH64('@#$%^&*---+=-0987654321~`'));
OUTPUT(HASHCRC(0));
OUTPUT(HASHCRC(1234567890));
OUTPUT(HASHCRC(0987654321));
OUTPUT(HASHCRC('abcdefghijklmnopqrstuvwxyz'));
OUTPUT(HASHCRC('!@#$%^&*--_+=-0987654321~`'));

OUTPUT(ATAN2(0.0,1.0));
OUTPUT(ATAN2(0.0,-1.0));
OUTPUT(ATAN2(-1.0,0.0));
OUTPUT(ATAN2(1.0,0.0));
OUTPUT(ATAN2(1.0,1.0));
OUTPUT(ATAN2(2.0,2.0));
OUTPUT(ATAN2(3.0,3.0));
OUTPUT(ATAN2(4.0,4.0));
OUTPUT(ATAN2(-1.0,1.0));
OUTPUT(ATAN2(-2.0,2.0));
OUTPUT(ATAN2(-3.0,3.0));
OUTPUT(ATAN2(-4.0,4.0));
OUTPUT(ATAN2(-1.0,-1.0));
OUTPUT(ATAN2(-2.0,-2.0));
OUTPUT(ATAN2(-3.0,-3.0));
OUTPUT(ATAN2(-4.0,-4.0));
OUTPUT(ATAN2(1.0,-1.0));
OUTPUT(ATAN2(2.0,-2.0));
OUTPUT(ATAN2(3.0,-3.0));
OUTPUT(ATAN2(4.0,-4.0));
