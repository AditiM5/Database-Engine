# README

### Running tests :

#### 1. SETTINGS: 
The test driver reads the locations of location variables (dbfile_dir, tpch_dir) from the file 'test.cat' 
* The first line describes the catalog location => *catalog_path
* The second line tells where to store created dbfiles  => *dbfile_dir
* The third line is the location where tpch tables can be found => *tpch_dir

All additional files that are created (metadata and other things) should be stored in the location specified by dbfile_dir.

```make test.out```

To run the driver, type

```test.out```

#### The output screenshots are stored in the folder A3_Output


### Running gtest :
The gtests are being run on 1-MB data
Test cases being handled:
* select * from partsupp where ps_supplycost < 500
* select p_partkey, p_name, p_retailprice from part where p_retailprice < 100;
* select sum (s_acctbal + (s_acctbal * .05)) from supplier;
* select sum (ps_supplycost) from supplier, partsupp where s_suppkey = ps_suppkey;
* select sum (ps_supplycost) from supplier, partsupp where s_suppkey = ps_suppkey groupby s_nationkey;

```make gtest```

To run the driver, type

```./RelOpTest```

and then follow the on-screen instructions.

### Running docker:

#### To run gtest
```./docker.sh gtest```



### Authors

* **Aditi Malladi UFID: 9828-6321**
* **Suraj Kumar Reddy Thanugundla UFID: 3100-9916**
