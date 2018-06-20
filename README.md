# Grace_Hash_Join_Hadoop
Distributed implementation of Grace Hash Join algorithm on Hadoop

The Grace Hash join Algorithm works as follows: For two relations R and S, we partition both R and S and write these partitions to disk. Then, the algorithm loads pairs of partitions into memory, builds a hash table for the smaller relation and probes the other relation for matches with the hash table. The hash function must be the same in both relations and must concern the join attribute which is present in both relations. The general idea is that becaus e we use the same hash function to form the aforementioned partitions, an R-tuple (e.g. belonging to R1 partition) will find its match in its counterpart S-partition (in this case S1). The advantage of this algorithm is that unlike other hash join algorithms, we don't have to scan the S relation more times than needed.

# Requirements
Java 6 or higher  
Hadoop 1.x  
Maven 3.0.5  

# Input
The program takes 2 csv files as input, which represent the R and S relational tables. The first line is expected to contain the column field names, which are also comma-separated.

# Run
After compiling the project and producing the GraceHashJoinCluster6.jar file, execute:
```Java
hadoop jar GraceHashJoinCluster6.jar com.proxwvaseis.gracehashjoin.Main path_to_left_table path_to_right_table
name_of_join_column #num_of_machines 


