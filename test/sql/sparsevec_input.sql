SELECT '{0:1.5,2:3.5}'::sparsevec;
SELECT '{0:1.5,2:3.5}'::sparsevec::vector;
SELECT '{0:1.5,2:3.5}'::sparsevec::vector(5);
SELECT '{0:1.5,2:3.5}'::sparsevec::vector(4);
SELECT '{0:1.5,2:3.5}'::sparsevec::vector(2);
SELECT '[0,1.5,0,3.5,0]'::vector::sparsevec;

SELECT '{0:0,1:1,2:0}'::sparsevec;

SELECT '{1:1,0:1}'::sparsevec;

SELECT '{}'::sparsevec;
SELECT '{}'::sparsevec::vector;

SELECT '{-1:1}'::sparsevec;
SELECT '{1:1}'::sparsevec;

SELECT '{}'::sparsevec(2);
