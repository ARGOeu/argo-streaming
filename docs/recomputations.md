# Python script for retrieving recomputations

The specific script reads recomputatation information from mongodb for a specific tenant, report and date.
Each relevant recomputation retrieved is compiled in a json list and stored as a file in hdfs

To retrieve recomputations for tenant FOO report Critical and date 2018-09-10 issue:
`./bin/utils/recomputations.py -t FOO -r Critical -d 2018-09-10 -c /path/to/argo-streaming.conf`

Recomputation retrieval is also automatically called during batch job submission