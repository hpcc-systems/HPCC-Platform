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

#ifndef ECLBLAS_INCL
#define ECLBLAS_INCL

#ifdef ECLBLAS_EXPORTS
#define ECLBLAS_PLUGIN_API DECL_EXPORT
#define ECLBLAS_CALL DECL_EXPORT
#else
#define ECLBLAS_PLUGIN_API DECL_IMPORT
#define ECLBLAS_CALL DECL_IMPORT
#endif

#include "platform.h"
#include "hqlplugins.hpp"
#include "eclinclude4.hpp"
#include "eclrtl.hpp"
#include "eclhelper.hpp"


// Defines for triangle, diagonal, Side
#define UPPER_TRIANGLE 1
#define UPPER 1
#define AX 1
#define UNIT 1
#define UNIT_TRI 1


extern "C" {
#include <cblas.h>
}

extern "C" ECLBLAS_PLUGIN_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);

namespace eclblas {

ECLBLAS_CALL double dasum(uint32_t m, bool isAllX, size32_t lenX, const void * x,
                          uint32_t incx, uint32_t skipped);
ECLBLAS_CALL void daxpy(bool & __isAllResult, size32_t & __lenResult,
                        void * & __result, uint32_t n, double alpha,
                        bool isAllX, size32_t lenX, const void * x, uint32_t incx,
                        bool isAllY, size32_t lenY, const void * y, uint32_t incy,
                        uint32_t x_skipped, uint32_t y_skipped);
ECLBLAS_CALL void dgemm(bool & __isAll_Result, size32_t & __lenResult,
                        void * & __result, bool transposeA, bool transposeB,
                        uint32_t m, uint32_t n, uint32_t k,
                        double alpha, bool isAllA, size32_t lenA, const void* A,
                        bool isAllB, size32_t lenB, const void* B, double beta,
                        bool isAllC, size32_t lenC, const void* C);
ECLBLAS_CALL void dgetf2(bool & __isAllResult, size32_t & __lenResult,
                         void * & result, uint32_t m, uint32_t n,
                         bool isAllA, size32_t lenA, const void* a);
ECLBLAS_CALL void dpotf2(bool & __isAllResult, size32_t & __lenResult,
                         void * & __result, uint8_t tri, uint32_t r,
                         bool isAllA, size32_t lenA, const void * A,
                         bool clear);
ECLBLAS_CALL void dscal(bool & __isAllResult, size32_t & __lenResult,
                        void * & __result, uint32_t n, double alpha,
                        bool isAllX, size32_t lenX, const void * X,
                        uint32_t incx, uint32_t skipped);
ECLBLAS_CALL void dsyrk(bool & __isAllResult, size32_t & __lenResult,
                        void * &__result, uint8_t tri, bool transposeA,
                        uint32_t N, uint32_t k, double alpha, bool isAllA,
                        size32_t lenA, const void * a, double beta,
                        bool isAllC, size32_t lenC, const void * c,
                        bool clear);
ECLBLAS_CALL void dtrsm(bool & __isAllResult, size32_t & __lenResult,
                        void * & __result, uint8_t side, uint8_t tri,
                        bool transposeA, uint8_t diag, uint32_t m,
                        uint32_t n, uint32_t lda, double alpha, bool isAllA,
                        size32_t lenA, const void * a, bool isAllB, size32_t lenB,
                        const void * b);
ECLBLAS_CALL void extract_tri(bool & __isAllResult, size32_t & __lenResult,
                              void * & __result, uint32_t m, uint32_t n, uint8_t tri,
                              uint8_t dt, bool isAllA, size32_t lenA,
                              const void * a);
ECLBLAS_CALL void make_diag(bool & __isAllResult, size32_t & __lenResult,
                            void * & __result, size32_t m, double v,
                            bool isAllX, size32_t lenX, const void * x);

} // namespace

#endif
