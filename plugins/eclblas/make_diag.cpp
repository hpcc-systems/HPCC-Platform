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
//Make a diagonal matrix from a vector or a single value
//If the vector is present the diagonal is the product
ECLBLAS_CALL void make_diag(size32_t m, double v, bool isAllX,
                            size32_t lenX, const void * x,
                            bool & __isAllResult, size32_t & __lenResult,
                            void * & __result) {
  int cells = m * m;
  __isAllResult = false;
  __lenResult = cells * sizeof(double);
  double *diag = (double*) rtlMalloc(__lenResult);
  const double *in_x = (double*)x;
  unsigned int r, c;    //row and column
  for (int i=0; i<cells; i++) {
    r = i % m;
    c = i / m;
    diag[i] = (r==c)?  (lenX!=0 ? v*in_x[r] : v) : 0.0;
  }
  __result = (void*) diag;
}
