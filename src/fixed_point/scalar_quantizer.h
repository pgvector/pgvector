#ifndef PGVECTOR_SRC_FIXED_POINT_SCALAR_QUANTIZER_H_
#define PGVECTOR_SRC_FIXED_POINT_SCALAR_QUANTIZER_H_


#include "postgres.h"

/* PG C headers */
#include "access/tupdesc.h"
#include "c.h"
#include "storage/bufpage.h"

/* PGVector C headers */
#include "../vector.h"

/*
 * This function quantizes the `in` vector with FP8 scalar quantization and
 * returns the result, which is written into `quantized_storage`. Note that the
 * caller is responsible for allocating memory for `quantized_storage` - since
 * this function is typically called in a tight loop, the caller should make
 * `quantized_storage` reusable across calls.
 * RETURNS: a datum pointer that points to `quantized_storage`.
 */
Datum ScalarQuantizeVector(const Vector* in, const Vector* multipliers,
                           bytea* quantized_storage);

/*
 * This function computes similar scores of `preprocessed_query` vector
 * against all FP8 vectors in the `page`. Note that the caller is responsible
 * for pre-processing the input query.
 * INPUTS:
 *   `preprocessed_query`: the `float` vector scaled by the inverse of fp8
 *   multipliers.
 *   `tuple_desc`: the schema of tuples in `page`.
 *   `page`: the pointer to the page in buffer pool where all fp8 vectors are
 *   stored.
 * OUTPUT:
 *   `result`: distances between `preprocessed_query` and all quantized vectors
 *   in the page.
 */
void ComputeOneToManyDotProductDistance(
    const Vector* preprocessed_query,
    const TupleDesc tuple_desc, const Page page, Vector* result);

/*
 * Similar to the above but computes the squared l2 distance. Note that `query`
 * is does not need to be pre-processed.
 */ 
void ComputeOneToManySquaredL2Distance(
    const Vector* query, const Vector* inv_multipliers,
    const TupleDesc tuple_desc, const Page page, Vector* result);

#endif  /* PGVECTOR_SRC_FIXED_POINT_SCALAR_QUANTIZER_H_ */
