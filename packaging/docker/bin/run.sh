#!/bin/bash
TAOS_RUN_TAOSBENCHMARK_TEST_ONCE=0
while ((1))
do
    # echo "outer loop: $a"
    sleep 1
    output=`taos -k`
    status=${output:0:1}
    # echo $output
    # echo $status
    if [ "$status"x = "0"x ]
    then
        taosd &
    fi
    # echo "$status"x "$TAOS_RUN_TAOSBENCHMARK_TEST"x "$TAOS_RUN_TAOSBENCHMARK_TEST_ONCE"x
    if [ "$status"x = "2"x ] && [ "$TAOS_RUN_TAOSBENCHMARK_TEST"x = "1"x ] && [ "$TAOS_RUN_TAOSBENCHMARK_TEST_ONCE"x = "0"x ]
    then
        TAOS_RUN_TAOSBENCHMARK_TEST_ONCE=1
        result=`taos -s "show databases;" | grep " test "`
        if [ "${result:0:5}"x != " test"x ]
        then
            taosBenchmark -y -t 1000 -n 1000 -S 900000
        fi
    fi
done
