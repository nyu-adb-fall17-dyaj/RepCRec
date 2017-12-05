# Replicated Concurrency Control and Recovery

A distributed database simulator, complete with multiversion concurrency control, deadlock detection, replication, and
failure recovery.

## Getting Started

Download or clone the repo.

### Prerequisites

```
python 3.6.3
```

### Running

At the root of the project

```
$ python -m src.ddbms (--cmd | -file <FILE>)
```

Usage: 
```
python -m src.ddbms -h | -help
python -m src.ddbms --cmd
python -m src.ddbms -file <FILE>
``` 

Options:
```
-h --help   Show help message.
--cmd       Enter input via command line.
-file FILE  Run input file.
```

Valid Inputs:

```
begin(trx)
beginRO(trx)
R(trx,var)
W(trx,var,val)
dump()
dump(site)
dump(var)
end(trx)
fail(site)
recover(site)
```

## Running the tests

At the root of the project

```
$ pip install nose
$ nosetests
```

## Authors

* **Da Ying** - dy877@nyu.edu
* **Ardi Jusufi** - aj2223877@nyu.edu
