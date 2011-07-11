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





favorites(string name) := 
    CASE(name,
        'Gavin'=>['Olives','Goats cheese'],
        'Liz'=>['Pinapple','Cheese'],
        'Nathan'=>['Mushroom'],
        'Robert'=>['Cheese','Yoghurt','Apple','Mango'],
        []);


favoritePudding(string name) := 
    MAP(name='Gavin'=>['Christmas','Suet'],
        name='Liz'=>['Ice cream','Chocolate'],
        name='Nathan'=>['Fruit'],
        name='Robert'=>ALL,
        []);



names := dataset(['Gavin','Liz','Nathan','Robert'], { string name });


output(names, { name, 'Cheese' in favorites(name)});

output(names, { name, 'Ice cream' in favoritePudding(name)});
