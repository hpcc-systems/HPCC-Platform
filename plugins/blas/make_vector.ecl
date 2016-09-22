//Make a vector
IMPORT $.Types AS Types;
matrix_t := Types.matrix_t;
dimension_t := Types.dimension_t;
value_t := Types.value_t;

Cell := RECORD
  value_t v;
END;
Cell makeCell(value_t v) := TRANSFORM
  SELF.v := v;
END;
vec_dataset(dimension_t m, value_t v) := DATASET(m, makeCell(v));

/**
 * Make a vector of dimension m
 * @param m number of elements
 * @param v the values, defaults to 1
 * @return the vector
 */
EXPORT matrix_t make_vector(dimension_t m, value_t v=1.0) := SET(vec_dataset(m, v), v);
