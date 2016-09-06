/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

rtl := SERVICE
 unsigned4 rtlRandom() : eclrtl,volatile,library='eclrtl',entrypoint='rtlRandom';
END;

// The following assume RANDOM() aren't strictly correct, but assume it is unlikely for RANDOM() to return the
// same number twice in succession.

//Random should be re-evaluated for each unique instance.
output(IF(rtl.rtlRandom() != rtl.rtlRandom(), 'Pass', 'Fail'));

//There is only a single instance of this variable - it should not be re-evaluated
volatile1 := rtl.rtlRandom();
output(IF(volatile1 = volatile1, 'Pass', 'Fail'));

//Again, re-evaluating the function should not create a new value
volatile2() := rtl.rtlRandom();
output(IF(volatile2() = volatile2(), 'Pass', 'Fail'));

//Again, no reason for the random in the function to be re-evaluated.
volatile3(integer x) := rtl.rtlRandom() % x;
output(IF(volatile3(100) % 50 = volatile3(50), 'Pass', 'Fail'));

//Explicitly create a unique volatile instance for each call instance - even if the same parameters
volatile4(integer n) volatile := rtl.rtlRandom();
output(IF(volatile4(100) != volatile4(100), 'Pass', 'Fail'));
output(IF(volatile4(100) != volatile4(99), 'Pass', 'Fail'));

//Create a unique instance for each value of n
volatile5(integer n) := volatile4(n);
output(IF(volatile5(1) != volatile5(2), 'Pass', 'Fail'));
output(IF(volatile5(5) = volatile5(5), 'Pass', 'Fail'));

//Create a unique volatile instance each time the function is called.
volatile6() volatile := rtl.rtlRandom() % 100;
output(IF(volatile6() != volatile6(), 'Pass', 'Fail'));
