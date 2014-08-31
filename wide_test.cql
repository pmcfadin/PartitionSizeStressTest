create KEYSPACE wide_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

use wide_test;

create table wide1 (
    partition_name text,
    partition_cell_number bigint,     // Maps to long in Java
    random_data int,
    PRIMARY KEY(partition_name, partition_cell_number)
);


// Turn key cache off