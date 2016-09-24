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
// scale a vector

#include "eclblas.hpp"

ECLBLAS_CALL void dscal(uint32_t n, double alpha, bool isAllX,
                        size32_t lenX, const void * x, uint32_t incx,
                        uint32_t skipped, bool & __isAllResult,
                        size32_t & __lenResult, void * & __result) {
  __isAllResult = false;
  __lenResult = lenX;
  double *result = (double*) rtlMalloc(lenX);
  memcpy(result, x, __lenResult);
  cblas_dscal(n, alpha, result+skipped, incx);
  __result = (void*) result;
}
