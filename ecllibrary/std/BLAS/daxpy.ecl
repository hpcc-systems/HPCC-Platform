//void cblas_daxpy(const int N, const double alpha, const double *X,
//                 const int incX, double *Y, const int incY);
IMPORT $ AS BLAS;
IMPORT $.Types AS Types;
dimension_t := Types.dimension_t;
value_t     := Types.value_t;
matrix_t    := Types.matrix_t;

/**
 * alpha*X + Y
 * @param N number of elements in vector
 * @param alpha the scalar multiplier
 * @param X the column major matrix holding the vector X
 * @param incX the increment or stride for the vector
 * @param Y the column major matrix holding the vector Y
 * @param incY the increment or stride of Y
 * @param x_skipped number of entries skipped to get to the first X
 * @param y_skipped number of entries skipped to get to the first Y
 * @return the updated matrix
 */
EXPORT matrix_t daxpy(dimension_t N, value_t alpha, matrix_t X,
                      dimension_t incX, matrix_t Y, dimension_t incY,
                      dimension_t x_skipped=0, dimension_t y_skipped=0)
      := BEGINC++
  #ifndef STD_BLAS_DAXPY
  #define STD_BLAS_DAXPY
  extern "C" {
    void cblas_daxpy(const int N, const double alpha, const double *X,
                     const int incX, double *Y, const int incY);
  }
  #endif
  #option library cblas
  #body
  __isAllResult = false;
  __lenResult = (lenX>lenY) ? lenX  : lenY;
  const double* X = ((double*)x) + x_skipped;
  double *result = (double*) rtlMalloc(__lenResult);
  memcpy(result, y,lenY);
  double* Y = result + y_skipped;
  cblas_daxpy(n, alpha, X, incx, Y, incy);
  __result = (void*) result;
ENDC++;
