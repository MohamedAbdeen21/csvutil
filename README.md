# CSVutil

# Usage

Currently CSVutil provides three main subcommands:
- `select`
- `stat`
- `count`

Each command can be used to read data from Stdin.

Number of Goroutines can be specified using the `-t` global flag.

Delimiter can be specified using `-d` global flag.

# Examples

## stat
```
>>> ./csvutil stat test.csv Temperature
min     : -89.00
max     : 196.00
nulls   : 69274.00
sum     : 171543113.20
mean    : 60.29
std_dev : 2.67
```

```
>>> head -n 10000 test.csv | ./csvutil stat Humidity -s max,min
min     : 0.00
max     : 100.00
```

## count
Use `-g` or `--group` flag to count column frequency
```
>>> ./csvutil count -t 12 -g Severity -f State=FL test.csv 
3: 11478
2: 377529
4: 9581
1: 2800
```

```
>>> ./csvutil count test.csv 
total: 2845343
```

## select
Can be used to reorder columns or extract a subset of columns. Setting `-t` flag will not maintain order of rows due to the concurrent nature of execution. Set `-t 1` to maintain order of rows.
```
>>> ./csvutil select test.csv -c Severity,Temperature,Humidity -f State=WI > subset.csv
```