//Function prototype for a function to apply to each cell
IMPORT $ AS BLAS;
IMPORT $.Types AS Types;
value_t := Types.value_t;
dimension_t := Types.dimension_t;
/**
 * Function prototype for Apply2Cell.
 * @param v the value
 * @param r the row ordinal
 * @param c the column ordinal
 * @return the updated value
 */
EXPORT value_t ICellFunc(value_t v, dimension_t r, dimension_t c) := v;
