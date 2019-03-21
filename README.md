# README

### Running test :

```make test.out```

To run the driver, type

```test.out```

and then follow the on-screen instructions.

### Running gtest :
Test cases being handled:
* Simple create a sorted file and load records
* Sequential Scan of DBFile (sorted)
* Scanning the sorted DBFile and filtering out using a CNF
* Interleave, by first creating a new file, adding records, scanning DBFile, and repeating the same
* Writing records to a sorted DBFile

```make gtest```

To run the driver, type

```./DBFileTest```

and then follow the on-screen instructions.

### Running docker:

#### To run the main file:
```./docker.sh main```

#### To run gtest
```./docker.sh gtest```



### Authors

* **Aditi Malladi UFID: 9828-6321**
* **Suraj Kumar Reddy Thanugundla UFID: 3100-9916**
