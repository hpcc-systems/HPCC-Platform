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
