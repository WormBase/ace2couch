#!/bin/bash

while :; do
    date
    curl -s localhost:5984/_active_tasks |
    perl -MJSON -MYAML -e 'print Dump(decode_json(<>))'
    sleep 5
done

