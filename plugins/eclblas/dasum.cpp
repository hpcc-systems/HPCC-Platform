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


// Absolute sum.  the L1-norm of a vector

#include "eclblas.hpp"

namespace eclblas {

ECLBLAS_CALL double dasum(uint32_t m, bool isAllX, size32_t lenX, const void * x,
                          uint32_t incx, uint32_t skipped) {
  const double* X = ((const double*)x) + skipped;
  double rslt = cblas_dasum(m, X, incx);
  return rslt;
}

}
