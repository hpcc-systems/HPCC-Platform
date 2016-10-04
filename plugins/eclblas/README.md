ECL BLAS Plugin
================

This is the ECL plugin to utilize the Basic Linear Algebra Sysem (BLAS).
It utilizes the C wrappers for BLAS

Installation and Dependencies
----------------------------

To build the <plugin name> plugin with the HPCC-Platform, <dependency> is required.
```
sudo apt-get install atlas-dev
```

Getting started
---------------

The libcblas.so library can be found in several packages including ATLAS
and LAPACK.

The Actual Plugin
-----------------

The eclblas plugin features are exposed via the ECL Standard library
Std.BLAS module.


###An ECL Example
```c
IMPORT Std.BLAS AS BLAS;
IMPORT BLAS.Types AS Types;
Types.matrix_t init1 := [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
Types.matrix_t init2 := [9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0];
Types.matrix_t init3 := [1.0, 2.0, 3.0];
Types.matrix_t init4 := [2.0, 2.0, 2.0];

Test1_mat := BLAS.dgemm(FALSE, TRUE, 3, 3, 1, 1.0, init3, init4);
Test2_mat := BLAS.dgemm(TRUE, FALSE, 1, 1, 3, 1.0, init3, init4);
Test3_mat := BLAS.dgemm(FALSE, TRUE, 3, 1, 3, -1.0, init1, init4);
Test4_mat := BLAS.dgemm(FALSE, TRUE, 3, 1, 3, 1.0, init1, init4, 5, init3);
OUTPUT(Test1_mat);
OUTPUT(Test2_mat);
OUTPUT(Test3_mat);
OUTPUT(test4_mat);
```
Yields the various matrix products.

Behavior and Implementation Details
------------------------------------

All of the arrays are column major.  The underlying Fortran code
expects column major, so we do not need the library to interpret
the data as row major (usual C convention).
