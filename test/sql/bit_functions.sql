SELECT hamming_distance(B'111', B'111');
SELECT hamming_distance(B'111', B'110');
SELECT hamming_distance(B'111', B'100');
SELECT hamming_distance(B'111', B'000');
SELECT hamming_distance(B'111', B'00');
SELECT hamming_distance(B'111', B'000'::varbit(4));

SELECT jaccard_distance(B'1111', B'1111');
SELECT jaccard_distance(B'1111', B'1110');
SELECT jaccard_distance(B'1111', B'1100');
SELECT jaccard_distance(B'1111', B'1000');
SELECT jaccard_distance(B'1111', B'0000');
SELECT jaccard_distance(B'1100', B'1000');
SELECT jaccard_distance(B'1111', B'000');
SELECT jaccard_distance(B'1111', B'0000'::varbit(5));
