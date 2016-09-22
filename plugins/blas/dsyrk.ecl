// Implements symmetric rank update.  C <- alpha A**T * A  + beta C
//or C <- alpha A * A**T  + beta C.  Triangular parameters says whether
//the update is upper or lower.  C is N by N
IMPORT $.Types AS Types;
dimension_t := Types.dimension_t;
Triangle    := Types.Triangle;
value_t     := Types.value_t;
matrix_t    := Types.matrix_t;

/**
 * Implements symmetric rank update C <- alpha A**T A + beta C or
 * c <- alpha A A**T + beta C.  C is N x N.
 * @param tri update upper or lower triangle
 * @param transposeA Transpose the A matrix to be NxK
 * @param N number of rows
 * @param K number of columns in the update matrix or transpose
 * @param alpha the alpha scalar
 * @param A the update matrix, either NxK or KxN
 * @param beta the beta scalar
 * @param C the matrix to update
 * @param clear clear the triangle that is not updated.  BLAS assumes
 * that symmetric matrices have only one of the triangles and this
 * option lets you make that true.
 */
EXPORT matrix_t dsyrk(Triangle tri, BOOLEAN transposeA,
            dimension_t N, dimension_t K,
            value_t alpha, matrix_t A,
            value_t beta, matrix_t C, BOOLEAN clear=FALSE) := BEGINC++
  #ifndef STD_BLAS_ENUM
  #define STD_BLAS_ENUM
  enum CBLAS_ORDER {CblasRowMajor=101, CblasColMajor=102};
  enum CBLAS_TRANSPOSE {CblasNoTrans=111, CblasTrans=112, CblasConjTrans=113};
  enum CBLAS_UPLO {CblasUpper=121, CblasLower=122};
  enum CBLAS_DIAG {CblasNonUnit=131, CblasUnit=132};
  enum CBLAS_SIDE {CblasLeft=141, CblasRight=142};
  #endif
  #ifndef STD_BLAS_DSYRK
  #define STD_BLAS_DSYRK
  extern "C" {
    void cblas_dsyrk(const enum CBLAS_ORDER Order, const enum CBLAS_UPLO Uplo,
                     const enum CBLAS_TRANSPOSE Trans, const int N, const int K,
                     const double alpha, const double *A, const int lda,
                     const double beta, double *C, const int ldc);
  }
  #endif
  #define UPPER 1
  #option library cblas
  #body
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
  unsigned int lda = (transposea)  ? k  : n;
  cblas_dsyrk(CblasColMajor,
              tri==UPPER  ? CblasUpper  : CblasLower,
              transposea ? CblasTrans : CblasNoTrans,
              n, k, alpha, (const double *)a, lda, beta, new_c, n);
  __result = (void*) new_c;
ENDC++;
