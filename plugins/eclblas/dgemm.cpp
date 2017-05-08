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


// matrix-matrix multiply.  alpha A B + beta C with flags to
//transpose A and B.  beta defaults to zero, and C to empty
#include "eclblas.hpp"

namespace eclblas {

ECLBLAS_CALL void dgemm(bool & __isAllResult, size32_t & __lenResult,
                        void * & __result, bool transposeA, bool transposeB,
                        uint32_t m, uint32_t n, uint32_t k,
                        double alpha, bool isAllA, size32_t lenA, const void* A,
                        bool isAllB, size32_t lenB, const void* B, double beta,
                        bool isAllC, size32_t lenC, const void* C) {
  typedef double __attribute__((aligned(1))) misaligned_double; // prevent gcc from assuming the data is correctly aligned.
  unsigned int lda = transposeA==0 ? m  : k;
  unsigned int ldb = transposeB==0 ? k  : n;
  unsigned int ldc = m;
  __isAllResult = false;
  __lenResult = m * n * sizeof(double);
  double *result = (double*) rtlMalloc(__lenResult);
  // populate if provided
  for(uint32_t i=0; i<m*n; i++) result[i] = (__lenResult==lenC) ?((misaligned_double*)C)[i] :0.0;
  cblas_dgemm(CblasColMajor,
              transposeA ? CblasTrans : CblasNoTrans,
              transposeB ? CblasTrans : CblasNoTrans,
              m, n, k, alpha,
              (const double *) A, lda,
              (const double *) B, ldb,
              beta, result, ldc);
  __result = (void *) result;
}

}
