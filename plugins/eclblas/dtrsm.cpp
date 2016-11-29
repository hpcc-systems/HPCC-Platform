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
// Triangular matrix solver.
//  op( A )*X = alpha*B,   or   X*op( A ) = alpha*B; B is m x n
//
#include "eclblas.hpp"

namespace eclblas {

ECLBLAS_CALL void dtrsm(bool & __isAllResult, size32_t & __lenResult,
                        void * & __result, uint8_t side, uint8_t tri,
                        bool transposeA, uint8_t diag, uint32_t m,
                        uint32_t n, uint32_t lda, double alpha, bool isAllA,
                        size32_t lenA, const void * a, bool isAllB, size32_t lenB,
                        const void * b) {
  unsigned int ldb = m;
  __isAllResult = false;
  __lenResult = lenB;
  double *new_b = (double*) rtlMalloc(lenB);
  memcpy(new_b, b, __lenResult);
  cblas_dtrsm(CblasColMajor,
              side==AX ?  CblasLeft  : CblasRight,
              tri==UPPER  ? CblasUpper  : CblasLower,
              transposeA ? CblasTrans : CblasNoTrans,
              diag==UNIT ? CblasUnit : CblasNonUnit,
              m, n, alpha, (const double *)a, lda, new_b, ldb);
  __result = (void*) new_b;
}

}
