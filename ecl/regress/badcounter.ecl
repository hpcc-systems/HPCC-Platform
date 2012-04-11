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

rec := {STRING s};
DS1:=DATASET([{'One'},{'Two'},{'Three'},{'Four'}], rec);
DS2:=DATASET([{'A'},{'B'},{'C'},{'D'}], rec);

rec getMessage(DS1 L, unsigned cnt):= TRANSFORM
    Self.S:=L.s+DS2[Cnt].s;
End;
DS3 := Project(DS1, getMessage(LEFT, COUNTER));

OUTPUT(DS3);
