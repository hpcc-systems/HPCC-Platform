// Triangular matrix solver.
//  op( A )*X = alpha*B,   or   X*op( A ) = alpha*B; B is m x n
//
IMPORT $.Types;
dimension_t := Types.dimension_t;
Triangle    := Types.Triangle;
Diagonal    := Types.Diagonal;
Side        := Types.Side;
value_t     := Types.value_t;
matrix_t    := Types.matrix_t;

/**
 * Triangular matrix solver.  op(A) X = alpha B or X op(A) = alpha B
 * where op is Transpose, X and B is MxN
 * @param side side for A, Side.Ax is op(A) X = alpha B
 * @param tri Says whether A is Upper or Lower triangle
 * @param transposeA is op(A) the transpose of A
 * @param diag is the diagonal an implied unit diagonal or supplied
 * @param M number of rows
 * @param N number of columns
 * @param lda the leading dimension of the A matrix, either M or N
 * @param alpha the scalar multiplier for B
 * @param A a triangular matrix
 * @param B the matrix of values for the solve
 * @return the matrix of coefficients to get B.
 */
EXPORT matrix_t dtrsm(Side side, Triangle tri,
                      BOOLEAN transposeA, Diagonal diag,
                      dimension_t M, dimension_t N,  dimension_t lda,
                      value_t alpha, matrix_t A, matrix_t B) := BEGINC++
  #ifndef STD_BLAS_ENUM
  #define STD_BLAS_ENUM
  enum CBLAS_ORDER {CblasRowMajor=101, CblasColMajor=102};
  enum CBLAS_TRANSPOSE {CblasNoTrans=111, CblasTrans=112, CblasConjTrans=113};
  enum CBLAS_UPLO {CblasUpper=121, CblasLower=122};
  enum CBLAS_DIAG {CblasNonUnit=131, CblasUnit=132};
  enum CBLAS_SIDE {CblasLeft=141, CblasRight=142};
  #endif
  #ifndef STD_BLAS_DTRSM
  #define STD_BLAS_DTRSM
  extern "C" {
    void cblas_dtrsm(const enum CBLAS_ORDER Order, const enum CBLAS_SIDE Side,
                     const enum CBLAS_UPLO Uplo, const enum CBLAS_TRANSPOSE TransA,
                     const enum CBLAS_DIAG Diag, const int M, const int N,
                     const double alpha, const double *A, const int lda,
                     double *B, const int ldb);
  }
  #endif
  #define UPPER 1
  #define AX 1
  #define UNIT 1
  #option library cblas
  #body
  unsigned int ldb = m;
  __isAllResult = false;
  __lenResult = lenB;
  double *new_b = (double*) rtlMalloc(lenB);
  memcpy(new_b, b, __lenResult);
  cblas_dtrsm(CblasColMajor,
              side==AX ?  CblasLeft  : CblasRight,
              tri==UPPER  ? CblasUpper  : CblasLower,
              transposea ? CblasTrans : CblasNoTrans,
              diag==UNIT ? CblasUnit : CblasNonUnit,
              m, n, alpha, (const double *)a, lda, new_b, ldb);
  __result = (void*) new_b;
ENDC++;
