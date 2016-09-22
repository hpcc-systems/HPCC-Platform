IMPORT $.Types AS Types;
dimension_t := Types.dimension_t;
value_t     := Types.value_t;
matrix_t    := Types.matrix_t;
/**
 * alpha*op(A) op(B) + beta*C where op() is transpose
 * @param transposeA true when transpose of A is used
 * @param transposeB true when transpose of B is used
 * @param M number of rows in product
 * @param N number of columns in product
 * @param K number of columns/rows for the multiplier/multiplicand
 * @param alpha scalar used on A
 * @param A matrix A
 * @param B matrix B
 * @param beta scalar for matrix C
 * @param C matrix C or empty
 */
EXPORT matrix_t dgemm(BOOLEAN transposeA, BOOLEAN transposeB,
                      dimension_t M, dimension_t N, dimension_t K,
                      value_t alpha, matrix_t A, matrix_t B,
                      value_t beta=0.0, matrix_t C=[]) := BEGINC++
  #ifndef STD_BLAS_ENUM
  #define STD_BLAS_ENUM
    enum CBLAS_ORDER {CblasRowMajor=101, CblasColMajor=102};
    enum CBLAS_TRANSPOSE {CblasNoTrans=111, CblasTrans=112, CblasConjTrans=113};
    enum CBLAS_UPLO {CblasUpper=121, CblasLower=122};
    enum CBLAS_DIAG {CblasNonUnit=131, CblasUnit=132};
    enum CBLAS_SIDE {CblasLeft=141, CblasRight=142};
  #endif
  #ifndef STD_BLAS_DGEMM
  #define STD_BLAS_DGEMM
  extern "C" {
    void cblas_dgemm(const enum CBLAS_ORDER Order, const enum CBLAS_TRANSPOSE TransA,
                     const enum CBLAS_TRANSPOSE TransB, const int M, const int N,
                     const int K, const double alpha, const double *A,
                     const int lda, const double *B, const int ldb,
                     const double beta, double *C, const int ldc);
  }
  #endif
  #option library cblas
  #body
   unsigned int lda = transposea==0 ? m  : k;
   unsigned int ldb = transposeb==0 ? k  : n;
   unsigned int ldc = m;
   __isAllResult = false;
   __lenResult = m * n * sizeof(double);
   double *result = new double[m * n];
   // populate if provided
   for(uint32_t i=0; i<m*n; i++) result[i] = (__lenResult==lenC) ?((double*)c)[i] :0.0;
   cblas_dgemm(CblasColMajor,
               transposea ? CblasTrans : CblasNoTrans,
               transposeb ? CblasTrans : CblasNoTrans,
               m, n, k, alpha,
               (const double *) a, lda,
               (const double *) b, ldb,
               beta, result, ldc);
   __result = (void *) result;
ENDC++;
