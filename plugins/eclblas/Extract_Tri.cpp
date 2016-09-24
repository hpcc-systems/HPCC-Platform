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
ECLBLAS_CALL void Extract_Tri(uint32_t m, uint32_t n, uint8_t tri,
                              uint8_t dt, bool isAllA, size32_t lenA,
                              const void * a, bool & __isAllResult,
                              size32_t & __lenResult, void * & __result){
  int cells = m * n;
  __isAllResult = false;
  __lenResult = lenA;
  double *new_a = (double*) rtlMalloc(lenA);
  unsigned int r, c;    //row and column
  for (int i=0; i<cells; i++) {
    r = i % m;
    c = i / m;
    if (r==c) new_a[i] = (dt==UNIT_TRI) ? 1.0  : ((double*)a)[i];
    else if (r > c) { // lower part
      new_a[i] = (tri==UPPER) ? 0.0  : ((double*)a)[i];
    } else {          // upper part
      new_a[i] = (tri==UPPER) ? ((double*)a)[i]  : 0.0;
    }
  }
  __result = (void*) new_a;
}
