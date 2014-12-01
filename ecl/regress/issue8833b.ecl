#option('showMetaInGraph', true);
layout := {unsigned u1};
ds := dataset([{3},{2},{1}], layout);
key := index(ds, {u1},{}, '~dskaggs::delete::key');

dsSort := sort(ds, u1);
dsJoin := join(dsSort, key,
               keyed(left.u1 = right.u1),
               transform(left), keyed, unordered);
dsSort2 := sort(dsJoin, u1);

sequential(
    build(key, overwrite),
    output(dsSort2,, '~dskaggs::delete::dsSort2', overwrite),
);