// scale a vector
//void cblas_dscal(const int N, const double alpha, double *X, const int incX);
IMPORT $.Types AS Types;
dimension_t := Types.dimension_t;
value_t     := Types.value_t;
matrix_t    := Types.matrix_t;

/**
 * Scale a vector alpha
 * @param N number of elements in the vector
 * @param alpha the scaling factor
 * @param X the column major matrix holding the vector
 * @param incX the stride to get to the next element in the vector
 * @param skipped the number of elements skipped to get to the first element
 * @return the updated matrix
 */
EXPORT matrix_t dscal(dimension_t N, value_t alpha, matrix_t X,
                      dimension_t incX, dimension_t skipped=0) := BEGINC++
  #ifndef STD_BLAS_DSCAL
  #define STD_BLAS_DSCAL
  extern "C" {
    void cblas_dscal(const int N, const double alpha, double *X, const int incX);
  }
  #endif
  #option library cblas
  #body
  __isAllResult = false;
  __lenResult = lenX;
  double *result = (double*) rtlMalloc(lenX);
  memcpy(result, x, __lenResult);
  cblas_dscal(n, alpha, result+skipped, incx);;
  __result = (void*) result;
ENDC++;
