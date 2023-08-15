/*-------------------------------------------------------------------------
 *
 * scalar_quantizer.c
 *	  
 *     Implements 8-bit scalar quantizer and scoring functionalities.
 *
 * NOTE: the algorithms implemented in this file was heavily influenced by
 * Google's ScaNN scalar quantization library:
 *
 *   https://github.com/google-research/google-research/tree/master/scann
 * 
 *-------------------------------------------------------------------------
 */

#include <stddef.h>
#include <limits.h>
#include <math.h>

#include "scalar_quantizer.h"

#include "postgres.h"

/* PG C headers */
#include "c.h"
#include "fmgr.h"
#include "access/itup.h"

/* pg vector C headers */
#include "../vector.h"

Datum ScalarQuantizeVector(const Vector *in, const Vector *multipliers,
                           bytea *quantized_storage)
{
    /* NOTE: use `VARDATA_ANY` since `quantized_storage` can be packed with 1B header */
    int8_t  *ptr = (int8_t*) VARDATA_ANY(quantized_storage);

    Assert(in->dim == multipliers->dim);

    for (int i = 0; i < in->dim; ++i)
    {
        float fp_val = round(in->x[i] * multipliers->x[i]);
        if (unlikely(fp_val > SCHAR_MAX))
            ptr[i] = SCHAR_MAX;
        else if (unlikely(fp_val < SCHAR_MIN))
            ptr[i] = SCHAR_MIN;
        else
            ptr[i] = (int8_t) fp_val;
    }

    PG_RETURN_BYTEA_P(quantized_storage);
}

void ComputeOneToManyDotProductDistance(const Vector *preprocessed_query,
                                        const TupleDesc tuple_desc,
                                        const Page page, Vector *result)
{
    bool    is_null;
    int8_t  *ptr;
    float   distance;
    OffsetNumber max_offno = PageGetMaxOffsetNumber(page);

    Assert(max_offno == result->dim);
    Assert(preprocessed_query->dim == inv_multipliers->dim);

    for (int i = 0; i < result->dim; ++i)
    {
        IndexTuple tuple = (IndexTuple) PageGetItem(page, PageGetItemId(page, i + FirstOffsetNumber));
        bytea* data = DatumGetByteaPP(index_getattr(tuple, 1, tuple_desc, &is_null));
        /* IVF index build should skip null vectors (refer to `ivfbuild.c`). */
        if (unlikely(is_null))
            continue;

        ptr = (int8_t*) VARDATA_ANY(data);
        distance = 0.0f;
        for (size_t j = 0; j < preprocessed_query->dim; ++j)
            distance += preprocessed_query->x[j] * ((float) ptr[j]);
        /* Negative dot product */
        result->x[i] = -distance;
    }
}

void ComputeOneToManySquaredL2Distance(const Vector *query,
                                       const Vector *inv_multipliers,
                                       const TupleDesc tuple_desc,
                                       const Page page, Vector *result)
{
    bool    is_null;
    int8_t  *ptr;
    float   distance;
    float   scaled_val;
    float   diff;
    OffsetNumber max_offno = PageGetMaxOffsetNumber(page);

    Assert(max_offno == result->dim);
    Assert(query->dim == inv_multipliers->dim);

    for (int i = 0; i < result->dim; ++i)
    {
        IndexTuple tuple = (IndexTuple) PageGetItem(page, PageGetItemId(page, i + FirstOffsetNumber));
        bytea* data = DatumGetByteaPP(index_getattr(tuple, 1, tuple_desc, &is_null));
        /* IVF index build should skip null vectors (refer to `ivfbuild.c`). */
        if (unlikely(is_null))
            continue;
        
        ptr = (int8_t*) VARDATA_ANY(data);
        distance = 0.0f;
        for (size_t j = 0; j < query->dim; ++j)
        {
            scaled_val = ((float) ptr[j]) * inv_multipliers->x[j];
            diff = query->x[j] - scaled_val;
            distance += (diff * diff);
        }
        result->x[i] = distance;
    }
}
