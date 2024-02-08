#ifndef VECTOR_H
#define VECTOR_H

#define VECTOR_MAX_N 16000

#define VECTOR_SIZE(_n_elem)		(offsetof(Vector, x) + sizeof(VecEl)*(_n_elem))
#define DatumGetVector(x)		((Vector *) PG_DETOAST_DATUM(x))
#define PG_GETARG_VECTOR_P(x)	DatumGetVector(PG_GETARG_DATUM(x))
#define PG_RETURN_VECTOR_P(x)	PG_RETURN_POINTER(x)

typedef struct VecEl
{
	int32 index;
	float value;
} VecEl;

typedef struct Vector
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		n_elem;			/* number of non-zero elements */
	int32       dim; 			/* the actual dimension of the svector */
	int16		unused;
	VecEl		x[FLEXIBLE_ARRAY_MEMBER];
}			Vector;

// Vector	   *InitVector(int dim);
Vector     *InitVector(int32 n_elem, int dim);
void		PrintVector(char *msg, Vector * svector);
int			svector_cmp_internal(Vector * a, Vector * b);

#endif
