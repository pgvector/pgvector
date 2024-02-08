SELECT ARRAY[0,1,2,3]::svector;
SELECT ARRAY[0.0,1.0,2.0,3.0]::svector;
SELECT ARRAY[0,1,2,3]::float4[]::svector;
SELECT ARRAY[0,1,2,3]::float8[]::svector;
SELECT ARRAY[0,1,2,3]::numeric[]::svector;
SELECT '{NULL}'::real[]::svector;
SELECT '{NaN}'::real[]::svector;
SELECT '{Infinity}'::real[]::svector;
SELECT '{-Infinity}'::real[]::svector;
SELECT '{}'::real[]::svector;
SELECT '{{1}}'::real[]::svector;
SELECT '[1,2,3]'::svector::real[];

-- ensure no error
SELECT ARRAY[1,2,3] = ARRAY[1,2,3];
