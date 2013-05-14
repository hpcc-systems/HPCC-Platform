/*##############################################################################

    Copyright (C) <2010>  <LexisNexis Risk Data Management Inc.>

    All rights reserved. This program is NOT PRESENTLY free software: you can NOT redistribute it and/or modify
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

//UseStandardFiles
//UseTextSearch
//tidyoutput
//nothor
//nothorlcr
//DoesntReallyUseIndexes
//xxvarskip type==roxie && setuptype==thor && !local

#option ('checkAsserts',false)


q1 := dataset([
            '"increase"',
            'CAPS("increase")',
            'AND(CAPS("increase"),NOCAPS("the"))',
            'AND(CAPS("increase":1),NOCAPS("the":1000))',           // forces priorities of the terms - if remote, will force seeks.
            'AND(CAPS("increase":1000),NOCAPS("the":1))',           // forces priorities of the terms - but the wrong way around

            //Use a set to ensure that the remote read covers more than one part
            'AND(SET(CAPS("zacharia","Japheth","Absalom")),NOCAPS("ark"))',

            'AND(SET(CAPS("zacharia","Japheth","Absalom":1)),NOCAPS("ark":1000))',
            'AND(SET(CAPS("zacharia","Japheth","Absalom":1000)),NOCAPS("ark":1))',

            'AND("Melchisedech","rahab")',
            'AND("sisters","brothers")',

//MORE:
// STEPPED flag on merge to give an error if input doesn't support stepping.
// What about the duplicates that can come out of the proximity operators?
// where the next on the rhs is at a compatible position, but in a different document
// What about inverse of proximity x not w/n y
// Can inverse proximity be used for sentance/paragraph.  Can we combine them so short circuited before temporaries created.
//MORE: What other boundary conditions can we think of.

                ''
            ], queryInputRecord);

p := project(q1, doBatchExecute(TS_searchIndex, LEFT, 0x00000200));
output(p);
