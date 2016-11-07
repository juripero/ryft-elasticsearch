#!/bin/bash
show_help () {
    echo "Benchmark util"
    echo "Usage: $0 <n requests> <URL>"
    echo "example $0 10 http://127.0.0.1:9200/shakespeare/_search"
    exit 1
}

if [ "$#" -ne 2 ]; then
    echo "Illegal number of parameters"
    show_help
fi

N=1
cat query-list.txt | while read QUERY; do
    QUERY_FILENAME="query$N.tmp"
    echo $QUERY > $QUERY_FILENAME
    ab -p $QUERY_FILENAME -n $1 -c 1 -g $N.tsv $2 > $N.log && rm $QUERY_FILENAME &
    ((N++))
done
