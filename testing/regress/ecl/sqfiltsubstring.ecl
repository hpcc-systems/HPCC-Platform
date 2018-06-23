/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

//version multiPart=false

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);

//--- end of version configuration ---

import $.setup;
sq := setup.sq(multiPart);

// Test filtering at different levels, making sure parent fields are available in the child query.
// Also tests scoping of sub expressions using within.

zero := 0;
one := 1 : stored('one');
two := 2 : stored('two');
three := 3 : stored('three');
four := 4 : stored('four');
thirty := 30 : stored('thirty');

xkeyed(boolean x) := keyed(x);
unkeyed(boolean x) := (x);

//Fixed length substring
sequential(
output(sq.houseDs(xkeyed(postcode[1..0] = 'X')), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..0] = '    ')), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..1] = 'S')), { postcode} );
output(sq.houseDs(xkeyed(postcode[1] = 'S')), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..1] = 'A')), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..2] = 'SW')), { postcode} );
output(sq.houseDs(xkeyed(postcode[..2] = 'SW')), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..2] IN ['SA', 'SB', 'SW', 'SG'])), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..2] IN ['SA', 'SB', 'SW', 'SG'])), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..2] > 'SA')), { postcode} );
//Check behaviour of out of range truncation.
output(sq.houseDs(xkeyed(postcode[1..2] > 'SG8' and postcode[1..2] < 'WC1')), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..2] >= 'SG8' and postcode[1..2] <= 'WC1')), { postcode} );

//Check that the code to dynamically restrict the datasets works as expected.
output(sq.houseDs(xkeyed(postcode[1..zero] = 'X')), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..zero] = '    ')), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..one] = 'S')), { postcode} );
output(sq.houseDs(xkeyed(postcode[1] = 'S')), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..one] = 'A')), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..two] = 'SW')), { postcode} );
output(sq.houseDs(xkeyed(postcode[..two] = 'SW')), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..two] IN ['SA', 'SB', 'SW', 'SG'])), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..two] IN ['SA', 'SB', 'SW', 'SG'])), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..two] > 'SA')), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..two] > 'SG8' and postcode[1..two] < 'WC1')), { postcode} );
output(sq.houseDs(xkeyed(postcode[1..two] >= 'SG8' and postcode[1..two] <= 'WC1')), { postcode} );

//Variable length substring
output(sq.personDs(xkeyed(forename[1..0] = '   ')), { forename });
output(sq.personDs(xkeyed(forename[1..0] = 'x  ')), { forename });
output(sq.personDs(xkeyed(forename[1..1] = 'J')), { forename });
output(sq.personDs(xkeyed(forename[1] = 'J')), { forename });
output(sq.personDs(xkeyed(forename[1..4] = 'Wilm')), { forename });
output(sq.personDs(xkeyed(forename[1..4] = 'Liz ')), { forename });
output(sq.personDs(xkeyed(forename[..4] = 'Liz ')), { forename });
output(sq.personDs(xkeyed(forename[1..30] = 'Liz ')), { forename });
output(sq.personDs(xkeyed(forename[1..3] between 'Fre' and 'Joh')), { forename });
output(sq.personDs(xkeyed(forename[..3] between 'Fre' and 'Joh')), { forename });
output(sq.personDs(xkeyed(forename[..3] between 'Fred' and 'John')), { forename });
output(sq.personDs(xkeyed(forename[1..3] between 'Fred' and 'John')), { forename });

output(sq.personDs(xkeyed(forename[1..zero] = '   ')), { forename });
output(sq.personDs(xkeyed(forename[1..zero] = 'x  ')), { forename });
output(sq.personDs(xkeyed(forename[1..one] = 'J')), { forename });
output(sq.personDs(xkeyed(forename[1] = 'J')), { forename });
output(sq.personDs(xkeyed(forename[1..four] = 'Wilm')), { forename });
output(sq.personDs(xkeyed(forename[1..four] = 'Liz ')), { forename });
output(sq.personDs(xkeyed(forename[..four] = 'Liz ')), { forename });
output(sq.personDs(xkeyed(forename[1..thirty] = 'Liz ')), { forename });
output(sq.personDs(xkeyed(forename[1..three] between 'Fre' and 'Joh')), { forename });
output(sq.personDs(xkeyed(forename[..three] between 'Fre' and 'Joh')), { forename });
output(sq.personDs(xkeyed(forename[..three] between 'Fred' and 'John')), { forename });
output(sq.personDs(xkeyed(forename[1..three] between 'Fred' and 'John')), { forename });

output('Done')
);
