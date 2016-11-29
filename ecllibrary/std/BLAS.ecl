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
IMPORT lib_eclblas AS LIB_ECLBLAS;

EXPORT BLAS := MODULE
  // Types for the Block Basic Linear Algebra Sub-programs support
  EXPORT Types := MODULE
    EXPORT value_t      := LIB_ECLBLAS.blas_value_t;     //REAL8;
    EXPORT dimension_t  := LIB_ECLBLAS.blas_dimension_t; //UNSIGNED4;
    EXPORT matrix_t     := LIB_ECLBLAS.blas_matrix_t;    //SET OF REAL8
    EXPORT Triangle     := LIB_ECLBLAS.blas_Triangle;    //ENUM(UNSIGNED1, Upper=1, Lower=2)
    EXPORT Diagonal     := LIB_ECLBLAS.blas_Diagonal;    //ENUM(UNSIGNED1, UnitTri=1, NotUnitTri=2)
    EXPORT Side         := LIB_ECLBLAS.blas_Side;        //ENUM(UNSIGNED1, Ax=1, xA=2)
  END;

  /**
   * Function prototype for Apply2Cell.
   * @param v the value
   * @param r the row ordinal
   * @param c the column ordinal
   * @return the updated value
   */
  EXPORT Types.value_t ICellFunc(Types.value_t v,
                                 Types.dimension_t r,
                                 Types.dimension_t c) := v;

  /**
   * Iterate matrix and apply function to each cell
   *@param m number of rows
   *@param n number of columns
   *@param x matrix
   *@param f function to apply
   *@return updated matrix
   */
  EXPORT Types.matrix_t Apply2Cells(Types.dimension_t m,
                                    Types.dimension_t n,
                                    Types.matrix_t x,
                                    ICellFunc f) := FUNCTION
    Cell := {Types.value_t v};
    Cell applyFunc(Cell v, UNSIGNED pos) := TRANSFORM
      r := ((pos-1)  %  m) + 1;
      c := ((pos-1) DIV m) + 1;
      SELF.v := f(v.v, r, c);
    END;
    dIn := DATASET(x, Cell);
    dOut := PROJECT(dIn, applyFunc(LEFT, COUNTER));
    RETURN SET(dOut, v);
  END;

  /**
   * Absolute sum, the 1 norm of a vector.
   *@param m the number of entries
   *@param x the column major matrix holding the vector
   *@param incx the increment for x, 1 in the case of an actual vector
   *@param skipped default is zero, the number of entries stepped over
   * to get to the first entry
   *@return the sum of the absolute values
   */
  EXPORT Types.value_t
      dasum(Types.dimension_t m, Types.matrix_t x,
            Types.dimension_t incx, Types.dimension_t skipped=0)
      := LIB_ECLBLAS.ECLBLAS.dasum(m, x, incx, skipped);

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
EXPORT Types.matrix_t
       daxpy(Types.dimension_t N, Types.value_t alpha, Types.matrix_t X,
             Types.dimension_t incX, Types.matrix_t Y, Types.dimension_t incY,
             Types.dimension_t x_skipped=0, Types.dimension_t y_skipped=0)
      := LIB_ECLBLAS.ECLBLAS.daxpy(N, alpha, X, incX, Y, incY, x_skipped, y_skipped);

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
  EXPORT Types.matrix_t
         dgemm(BOOLEAN transposeA, BOOLEAN transposeB,
               Types.dimension_t M, Types.dimension_t N, Types.dimension_t K,
               Types.value_t alpha, Types.matrix_t A, Types.matrix_t B,
               Types.value_t beta=0.0, Types.matrix_t C=[])
     := LIB_ECLBLAS.ECLBLAS.dgemm(transposeA, transposeB, M, N, K, alpha, A, B, beta, C);

  /**
   * Compute LU Factorization of matrix A.
   * @param m number of rows of A
   * @param n number of columns of A
   * @return composite matrix of factors, lower triangle has an
   *         implied diagonal of ones.  Upper triangle has the diagonal of the
   *         composite.
   */
  EXPORT Types.matrix_t
         dgetf2(Types.dimension_t m, Types.dimension_t n, Types.matrix_t a)
      := LIB_ECLBLAS.ECLBLAS.dgetf2(m, n, a);

  /**
   * DPOTF2 computes the Cholesky factorization of a real symmetric
   * positive definite matrix A.
   *The factorization has the form
   * A = U**T * U ,  if UPLO = 'U', or
   * A = L  * L**T,  if UPLO = 'L',
   * where U is an upper triangular matrix and L is lower triangular.
   * This is the unblocked version of the algorithm, calling Level 2 BLAS.
   * @param tri indicate whether upper or lower triangle is used
   * @param r number of rows/columns in the square matrix
   * @param A the square matrix
   * @param clear clears the unused triangle
   * @return the triangular matrix requested.
   */

  EXPORT Types.matrix_t
         dpotf2(Types.Triangle tri, Types.dimension_t r, Types.matrix_t A,
                         BOOLEAN clear=TRUE)
      := LIB_ECLBLAS.ECLBLAS.dpotf2(tri, r, A, clear);

  /**
   * Scale a vector alpha
   * @param N number of elements in the vector
   * @param alpha the scaling factor
   * @param X the column major matrix holding the vector
   * @param incX the stride to get to the next element in the vector
   * @param skipped the number of elements skipped to get to the first element
   * @return the updated matrix
   */
  EXPORT Types.matrix_t
         dscal(Types.dimension_t N, Types.value_t alpha, Types.matrix_t X,
               Types.dimension_t incX, Types.dimension_t skipped=0)
      := LIB_ECLBLAS.ECLBLAS.dscal(N, alpha, X, incX, skipped);

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
  EXPORT Types.matrix_t
         dsyrk(Types.Triangle tri, BOOLEAN transposeA,
               Types.dimension_t N, Types.dimension_t K,
               Types.value_t alpha, Types.matrix_t A,
               Types.value_t beta, Types.matrix_t C, BOOLEAN clear=FALSE)
     := LIB_ECLBLAS.ECLBLAS.dsyrk(tri, transposeA, N, K, alpha, A, beta, C, clear);

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
  EXPORT Types.matrix_t
         dtrsm(Types.Side side, Types.Triangle tri,
               BOOLEAN transposeA, Types.Diagonal diag,
               Types.dimension_t M, Types.dimension_t N, Types.dimension_t lda,
               Types.value_t alpha, Types.matrix_t A, Types.matrix_t B)
      := LIB_ECLBLAS.ECLBLAS.dtrsm(side, tri, transposeA, diag, M, N, lda, alpha, A, B);

  /**
   * Extract the diagonal of he matrix
   * @param m number of rows
   * @param n number of columns
   * @param x matrix from which to extract the diagonal
   * @return diagonal matrix
   */
  EXPORT Types.matrix_t extract_diag(Types.dimension_t m,
                                     Types.dimension_t n,
                                     Types.matrix_t x) := FUNCTION
    cell := {Types.value_t v};
    cell ext(cell v, UNSIGNED pos) := TRANSFORM
      r := ((pos-1) % m) + 1;
      c := ((pos-1) DIV m) + 1;
      SELF.v := IF(r=c AND r<=m AND c<=n, v.v, SKIP);
    END;
    diag := SET(PROJECT(DATASET(x, cell), ext(LEFT, COUNTER)), v);
    RETURN diag;
  END;

  /**
   * Extract the upper or lower triangle.  Diagonal can be actual or implied
   * unit diagonal.
   * @param m number of rows
   * @param n number of columns
   * @param tri Upper or Lower specifier, Triangle.Lower or Triangle.Upper
   * @param dt Use Diagonal.NotUnitTri or Diagonal.UnitTri
   * @param a Matrix, usually a composite from factoring
   * @return the triangle
   */
  EXPORT Types.matrix_t
         extract_tri(Types.dimension_t m, Types.dimension_t n,
                     Types.Triangle tri, Types.Diagonal dt,
                     Types.matrix_t a)
       := LIB_ECLBLAS.ECLBLAS.extract_tri(m, n, tri, dt, a);

  /**
   * Generate a diagonal matrix.
   * @param m number of diagonal entries
   * @param v option value, defaults to 1
   * @param X optional input of diagonal values, multiplied by v.
   * @return a diagonal matrix
   */
  EXPORT Types.matrix_t
         make_diag(Types.dimension_t m, Types.value_t v=1.0,
                   Types.matrix_t X=[]) := LIB_ECLBLAS.ECLBLAS.make_diag(m, v, X);

  // make_vec helpers
  Cell := RECORD
    Types.value_t v;
  END;
  Cell makeCell(Types.value_t v) := TRANSFORM
    SELF.v := v;
  END;
  vec_dataset(Types.dimension_t m, Types.value_t v) := DATASET(m, makeCell(v));

  /**
   * Make a vector of dimension m
   * @param m number of elements
   * @param v the values, defaults to 1
   * @return the vector
   */
  EXPORT Types.matrix_t
      make_vector(Types.dimension_t m,
                  Types.value_t v=1.0) := SET(vec_dataset(m, v), v);

  /**
   * The trace of the input matrix
   * @param m number of rows
   * @param n number of columns
   * @param x the matrix
   * @return the trace (sum of the diagonal entries)
   */
  EXPORT Types.value_t
      trace(Types.dimension_t m,
            Types.dimension_t n,
            Types.matrix_t x)
      := SUM(extract_diag(m,n,x));
END;
