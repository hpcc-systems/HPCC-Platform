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
// Implements symmetric rank update.  C <- alpha A**T * A  + beta C
//or C <- alpha A * A**T  + beta C.  Triangular parameters says whether
//the update is upper or lower.  C is N by N
#include "eclblas.hpp"

namespace eclblas {

ECLBLAS_CALL void dsyrk(bool & __isAllResult, size32_t & __lenResult,
                        void * &__result, uint8_t tri, bool transposeA,
                        uint32_t n, uint32_t k, double alpha, bool isAllA,
                        size32_t lenA, const void * a, double beta,
                        bool isAllC, size32_t lenC, const void * c,
                        bool clear) {
  __isAllResult = false;
  __lenResult = lenC;
  double *new_c = (double*) rtlMalloc(lenC);
  if (clear) {
    unsigned int pos = 0;
    for(unsigned int i=0; i<n; i++) {
      pos = i*n;  // pos is head of column
      for (unsigned int j=0; j<n; j++) {
        new_c[pos+j] = tri==UPPER ? i>=j ? ((double*)c)[pos+j]  : 0.0
                                  : i<=j ? ((double*)c)[pos+j]  : 0.0;
      }
    }
  } else memcpy(new_c, c, __lenResult);
  unsigned int lda = (transposeA)  ? k  : n;
  cblas_dsyrk(CblasColMajor,
              tri==UPPER  ? CblasUpper  : CblasLower,
              transposeA ? CblasTrans : CblasNoTrans,
              n, k, alpha, (const double *)a, lda, beta, new_c, n);
  __result = (void*) new_c;
}

}
