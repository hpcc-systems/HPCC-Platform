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
//Extract the upper triangular matrix or the lower triangular matrix
//from a composite.  Composites are produced by some factorizations.
//
#include "eclblas.hpp"

namespace eclblas {

ECLBLAS_CALL void extract_tri(bool & __isAllResult, size32_t & __lenResult,
                              void * & __result, uint32_t m, uint32_t n, uint8_t tri,
                              uint8_t dt, bool isAllA, size32_t lenA, const void * a){
  int cells = m * n;
  __isAllResult = false;
  __lenResult = lenA;
  double *new_a = (double*) rtlMalloc(lenA);
  unsigned int r=0;    //row
  unsigned int c=0;    //column
  for (int i=0; i<cells; i++) {
    double a_element = ((const double*)a)[i];
    if (r==c) new_a[i] = (dt==UNIT_TRI) ? 1.0  : a_element;
    else if (r > c) { // lower part
      new_a[i] = (tri==UPPER) ? 0.0  : a_element;
    } else {          // upper part
      new_a[i] = (tri==UPPER) ? a_element  : 0.0;
    }
    r++;
    if (r==m) {
      r=0;
      c++;
    }
  }
  __result = (void*) new_a;
}

}
