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

export system := 
    SERVICE
unsigned integer4 node() : library='graph', ctxmethod, entrypoint='getNodeNum';
    END;


arec := 
            RECORD
unsigned1       a;
unsigned1       b;
unsigned4       sequence := 0;
unsigned4       node := 0;
            END;

alpha := dataset([
        {1,1}, 
        {1,2},
        {2,1},
        {2,2},
        {0,0},
        {3,1},
        {1,1}, 
        {1,2},
        {2,1},
        {2,2},
        {0,0},
        {3,1},
        {1,1}, 
        {1,2},
        {2,1},
        {2,2},
        {0,0},
        {3,1},
        {1,1}, 
        {1,2},
        {2,1},
        {2,2},
        {0,0},
        {3,1},
        {1,1}, 
        {1,2},
        {2,1},
        {2,2},
        {0,0},
        {3,1},
        {1,1}, 
        {1,2},
        {2,1},
        {2,2},
        {0,0},
        {3,1},
        {1,1}, 
        {1,2},
        {2,1},
        {2,2},
        {0,0},
        {3,1},
        {1,1}, 
        {1,2},
        {2,1},
        {2,2},
        {0,0},
        {3,1},
        {1,1}, 
        {1,2},
        {2,1},
        {2,2},
        {0,0},
        {3,1},
        {1,1}, 
        {1,2},
        {2,1},
        {2,2},
        {0,0},
        {3,1},
        {1,1}, 
        {1,2},
        {2,1},
        {2,2},
        {0,0},
        {3,1}
        ], arec);

arec t(arec l, unsigned4 c) := 
    TRANSFORM
        SELF.sequence := c;
        SELF.node := system.node();
        SELF := l;
    END;

bravo := project(alpha, t(left, counter));

charlie := choosesets(bravo, a=1 and b=1=>100,a=1 and b=2=>2,a=2 and b=1=>3,a=2 and b=2=>4,ENTH);
output(charlie,,'out1.d00');
output(table(charlie,{a,b,count(group)},a,b),,'out2.d00');
