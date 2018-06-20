# Grace_Hash_Join_Hadoop
Distributed implementation of Grace Hash Join algorithm on Hadoop

The Grace Hash join Algorithm works as follows: For two relations R and S, we partition both R
and S and write these partitions to disk. Then, the algorithm loads pairs of partitions into
memory, builds a hash table for the smaller relation and probes the other relation for matches
with the hash table. The hash function must be the same in both relations and must concern the
join attribute which is present in both relations. The general idea is that becaus e we use the
same hash function to form the aforementioned partitions, an R-tuple (e.g. belonging to R1
partition) will find its match in its counterpart S-partition (in this case S1). The advantage of this
algorithm is that unlike other hash join algorithms, we don't have to scan the S relation more
times than needed.
