SELECT '{0:1.5,2:3.5}/5'::sparsevec;
SELECT '{0:1.5,2:3.5}/5'::sparsevec::vector;
SELECT '{0:1.5,2:3.5}/5'::sparsevec::vector(5);
SELECT '{0:1.5,2:3.5}/5'::sparsevec::vector(4);
SELECT '[0,1.5,0,3.5,0]'::vector::sparsevec;

SELECT '{0:0,1:1,2:0}/3'::sparsevec;

SELECT '{1:1,0:1}/2'::sparsevec;

SELECT '{}/5'::sparsevec;
SELECT '{}/-1'::sparsevec;
SELECT '{}/100001'::sparsevec;
SELECT '{}/16001'::sparsevec::vector;

SELECT '{-1:1}/1'::sparsevec;
SELECT '{1:1}/1'::sparsevec;

SELECT '{}/1'::sparsevec(2);
