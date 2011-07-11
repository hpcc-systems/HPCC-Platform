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

#option ('globalFold', false);
getVarColor(string3 abrv) := case(abrv,
'AME' =>    V'AMETHYST',
'BGE' =>    V'BEIGE',
'BLK' =>    V'BLACK',
'BLU' =>    V'BLUE',
'BRO' =>    V'BROWN',
'BRZ' =>    V'BRONZE',
'CAM' =>    V'CAMOUFLAGE',
'COM' =>    V'CHROME', 
'CPR' =>    V'COPPER',
'CRM' =>    V'CREAM',
'DBL' =>    V'Dark Blue',
'DGR' =>    V'Dark Green',
'GLD' =>    V'GOLD',
'GRN' =>    V'GREEN',
'GRY' =>    V'GRAY',
'LAV' =>    V'LAVENDER',
'LBL' =>    V'Light Blue',
'LGR' =>    V'Light Green',
'MAR' =>    V'BURGUNDY', 
'MUL' =>    V'MULTICOLORED',
'MVE' =>    V'MAUVE',
'ONG' =>    V'ORANGE',
'PLE' =>    V'PURPLE',
'PNK' =>    V'PINK',
'RED' =>    V'RED',
'SIL' =>    V'Silver', 
'TAN' =>    V'TAN',
'TEA' =>    V'TEAL',
'TPE' =>    V'TAUPE',
'TRQ' =>    V'TURQUOISE',
'UNK' =>    V'',
'WHI' =>    V'WHITE',
'YEL' =>    V'YELLOW',(varstring)'');

getVarColor('YEL');


getColor(string3 abrv) := case(abrv,
'AME' =>    'AMETHYST',
'BGE' =>    'BEIGE',
'BLK' =>    'BLACK',
'BLU' =>    'BLUE',
'BRO' =>    'BROWN',
'BRZ' =>    'BRONZE',
'CAM' =>    'CAMOUFLAGE',
'COM' =>    'CHROME', 
'CPR' =>    'COPPER',
'CRM' =>    'CREAM',
'DBL' =>    'Dark Blue',
'DGR' =>    'Dark Green',
'GLD' =>    'GOLD',
'GRN' =>    'GREEN',
'GRY' =>    'GRAY',
'LAV' =>    'LAVENDER',
'LBL' =>    'Light Blue',
'LGR' =>    'Light Green',
'MAR' =>    'BURGUNDY', 
'MUL' =>    'MULTICOLORED',
'MVE' =>    'MAUVE',
'ONG' =>    'ORANGE',
'PLE' =>    'PURPLE',
'PNK' =>    'PINK',
'RED' =>    'RED',
'SIL' =>    'Silver', 
'TAN' =>    'TAN',
'TEA' =>    'TEAL',
'TPE' =>    'TAUPE',
'TRQ' =>    'TURQUOISE',
'UNK' =>    '',
'WHI' =>    'WHITE',
'YEL' =>    'YELLOW','');

getColor('YEL')  //returns 'WHITE'.  all codes return wrong color
