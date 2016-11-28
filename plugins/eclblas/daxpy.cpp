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

// Vector add, alpha X  +  Y
#include "eclblas.hpp"

namespace eclblas {

ECLBLAS_CALL void daxpy(bool & __isAllResult, size32_t & __lenResult,
                        void * & __result, uint32_t n, double alpha,
                        bool isAllX, size32_t lenX, const void * x, uint32_t incx,
                        bool isAllY, size32_t lenY, const void* y, uint32_t incy,
                        uint32_t x_skipped, uint32_t y_skipped) {
  __isAllResult = false;
  __lenResult = lenY;
  const double* X = ((const double*)x) + x_skipped;
  double *result = (double*) rtlMalloc(__lenResult);
  memcpy(result, y,lenY);
  double* Y = result + y_skipped;
  cblas_daxpy(n, alpha, X, incx, Y, incy);
  __result = (void*) result;
}

}
