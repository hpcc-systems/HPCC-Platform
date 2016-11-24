/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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
//DGETF2 computes the LU factorization of a matrix A.  Similar to LAPACK routine
//of same name.  Result matrix holds both Upper and Lower triangular matrix, with
//lower matrix diagonal implied since it is a unit triangular matrix.
//This version does not permute the rows.
//This version does not support a sub-matrix, hence no LDA argument.
//This routine would be better if dlamch were available to determine safe min

#include <math.h>
#include "eclblas.hpp"

namespace eclblas {

ECLBLAS_CALL void dgetf2(bool & __isAllResult, size32_t & __lenResult,
                         void * & __result, uint32_t m, uint32_t n,
                         bool isAllA, size32_t lenA, const void* a) {
  //double sfmin = dlamch('S');   // get safe minimum
  unsigned int cells = m*n;
  __isAllResult = false;
  __lenResult = cells * sizeof(double);
  double *new_a = (double*) rtlMalloc(__lenResult);
  memcpy(new_a, a, __lenResult);
  double akk;
  unsigned int i, k;
  unsigned int diag, vpos, wpos, mpos;
  unsigned int sq_dim = (m < n) ? m  : n;
  for (k=0; k<sq_dim; k++) {
    diag = (k*m) + k;     // diag cell
    vpos = diag + 1;      // top cell of v vector
    wpos = diag + m;      // left cell of w vector
    mpos = diag + m + 1;  //upper left of sub-matrix to update
    akk = new_a[diag];
    if (akk == 0.0) {
      rtlFree(new_a);
      rtlFail(0, "Permute required"); // need to permute
    }
    //Ideally, akk should be tested against sfmin, and dscal used
    // to update the vector for the L cells.
    for (i=vpos; i<vpos+m-k-1; i++) new_a[i] = new_a[i]/akk;
    //Update sub-matrix
    if (k < sq_dim - 1) {
      cblas_dger(CblasColMajor,
                 m-k-1, n-k-1, -1.0,  // sub-matrix dimensions
                 (new_a+vpos), 1, (new_a+wpos), m, (new_a+mpos), m);
    }
  }
  __result = (void*) new_a;
}

}
