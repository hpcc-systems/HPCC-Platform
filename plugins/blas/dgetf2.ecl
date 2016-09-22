//DGETF2 computes the LU factorization of a matrix A.  Similar to LAPACK routine
//of same name.  Result matrix holds both Upper and Lower triangular matrix, with
//lower matrix diagonal implied since it is a unit triangular matrix.
//This version does not permute the rows.
//This version does not support a sub-matrix, hence no LDA argument.
//
//This routine would be better if dlamch were available to determine safe min
//
IMPORT $.Types AS Types;
dimension_t := Types.dimension_t;
Triangle    := Types.Triangle;
matrix_t    := Types.matrix_t;

/**
 * Compute LU Factorization of matrix A.
 * @param m number of rows of A
 * @param n number of columns of A
 * @return composite matrix of factors, lower triangle has an
 *         implied diagonal of ones.  Upper triangle has the diagonal of the
 *         composite.
 */
EXPORT matrix_t dgetf2(dimension_t m, dimension_t n, matrix_t a) := BEGINC++
  #ifndef STD_BLAS_ENUM
  #define STD_BLAS_ENUM
  enum CBLAS_ORDER {CblasRowMajor=101, CblasColMajor=102};
  enum CBLAS_TRANSPOSE {CblasNoTrans=111, CblasTrans=112, CblasConjTrans=113};
  enum CBLAS_UPLO {CblasUpper=121, CblasLower=122};
  enum CBLAS_DIAG {CblasNonUnit=131, CblasUnit=132};
  enum CBLAS_SIDE {CblasLeft=141, CblasRight=142};
  #endif
  #ifndef STD_BLAS_DGER
  #define STD_BLAS_DGER
  extern "C" {
    void cblas_dger(const enum CBLAS_ORDER order, const int M, const int N,
                const double alpha, const double *X, const int incX,
                const double *Y, const int incY, double *A, const int lda);
  }
  #endif
  #include <math.h>
  #body
  //double sfmin = dlamch('S');   // get safe minimum
  unsigned int cells = m*n;
  __isAllResult = false;
  __lenResult = cells * sizeof(double);
  double *new_a = (double*) rtlMalloc(__lenResult);
  memcpy(new_a, a, __lenResult);
  double akk;
  unsigned int i, k;
  unsigned int diag, vpos, wpos, mpos;
  unsigned int sq_dim = (m < n) ? m  : n;
  for (k=0; k<sq_dim; k++) {
    diag = (k*m) + k;     // diag cell
    vpos = diag + 1;      // top cell of v vector
    wpos = diag + m;      // left cell of w vector
    mpos = diag + m + 1;  //upper left of sub-matrix to update
    akk = new_a[diag];
    if (akk == 0.0) rtlFail(0, "Permute required"); // need to permute
    //Ideally, akk should be tested against sfmin, and dscal used
    // to update the vector for the L cells.
    for (i=vpos; i<vpos+m-k-1; i++) new_a[i] = new_a[i]/akk;
    //Update sub-matrix
    if (k < sq_dim - 1) {
      cblas_dger(CblasColMajor,
                 m-k-1, n-k-1, -1.0,  // sub-matrix dimensions
                 (new_a+vpos), 1, (new_a+wpos), m, (new_a+mpos), m);
    }
  }
  __result = (void*) new_a;
ENDC++;
