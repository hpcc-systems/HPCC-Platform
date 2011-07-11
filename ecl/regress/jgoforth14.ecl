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

String20 var1 := '' : stored('watchtype');

string_rec := { string100 line; unsigned8 filepos{virtual(fileposition)}};

t := dataset(map(
                var1='nonglb'=>'~thor::BASE::Watchdog_Moxie_nonglb',
                var1='nonutility'=>'~thor::BASE::Watchdog_Moxie_nonutility',
                var1='nonglb_nonutility'=>'~thor::BASE::Watchdog_Moxie_nonglb_nonutility',
                '~thor::BASE::Watchdog_Moxie'),string_rec,flat);

b := BUILDINDEX(t,,
                map(var1='nonglb'=>'~thor::key::Watchdog_Moxie_nonglb.did',
                    var1='nonutility'=>'~thor::key::Watchdog_Moxie_nonutility.did',
                    var1='nonglb_nonutility'=>'~thor::key::Watchdog_Moxie_nonglb_nonutility.did',
                    '~thor::key::Watchdog_Moxie.did'),overwrite);

b;
