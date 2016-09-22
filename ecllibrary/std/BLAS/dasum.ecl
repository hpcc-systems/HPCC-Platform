// Absolute sum.  the 1-norm of a vector
IMPORT $ AS Matrix;
IMPORT $.Types AS Types;;
value_t := Types.value_t;
dimension_t := Types.dimension_t;
matrix_t := Types.matrix_t;

/**
 * Absolute sum, the 1 norm of a vector.
 *@param m the number of entries
 *@param x the column major matrix holding the vector
 *@param incx the increment for x, 1 in the case of an actual vector
 *@param skipped default is zero, the number of entries stepped over
 * to get to the first entry
 *@return the sum of the absolute values
 */
EXPORT value_t dasum(dimension_t m, matrix_t x, dimension_t incx,
                     dimension_t skipped=0) := BEGINC++
  #ifndef STD_BLAS_DASUM
  #define STD_BLAS_DASUM
  extern "C" {
    double cblas_dasum(const int N, const double *X, const int incX);
  }
  #endif
  #option library cblas
  #body
  const double* X = ((const double*)x) + skipped;
  double rslt = cblas_dasum(m, X, incx);
  return rslt;
ENDC++;