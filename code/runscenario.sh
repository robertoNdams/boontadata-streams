#!/bin/bash

if test $# -eq 0
then
    command=$0
    echo "usage: $command <scenario>"
    echo "<scenario> can be flink, truncate_cassandra_data, ..."
    return 0
fi

scenario=$1
echo "will start scenario $scenario"

tellandwaitnsecs()
{
    nbofseconds=$1
    echo "will wait for $nbofseconds seconds"
    sleep $nbofseconds
}

scenario_flink()
{
    timeType=$1
    echo "starting Flink scenario with $timeType"

    echo "Initial content in the Cassandra database"
    docker exec -ti cassandra3 cqlsh --execute "use boontadata; select count(*) as nb_debug from debug; select count(*) as nb_rawevents from raw_events; select count(*) as nb_aggevents from agg_events;"

    echo "start Flink job"
    docker exec -ti flink-master flink run -d -c io.boontadata.flink1.StreamingJob /workdir/flink1-0.1.jar $timeType
    tellandwaitnsecs 10
    docker exec -ti flink-master flink list

    echo "inject data"
    docker exec -ti client1 python /workdir/ingest.py

    echo "wait for Flink to finish ingesting"
    tellandwaitnsecs 10

    echo "get the result"
    docker exec -ti client1 python /workdir/compare.py

    echo "kill the Flink job"
    flinkjobid=`docker exec -ti flink-master flink list | grep io.boontadata.flink1.StreamingJob | awk '{print $4}'`
    echo "Flink job id is $flinkjobid"
    docker exec -ti flink-master flink cancel $flinkjobid
    docker exec -ti flink-master flink list
}

scenario_spark()
{
    timeType=$1
    echo "starting Flink scenario with $timeType"

    echo "Initial content in the Cassandra database"
    docker exec -ti cassandra3 cqlsh --execute "use boontadata; select count(*) as nb_debug from debug; select count(*) as nb_rawevents from raw_events; select count(*) as nb_aggevents from agg_events;"

    echo "start Spark job"
    docker exec -ti sparkm1 bash -c ". start-job.sh" | tee /tmp/spark-submission.txt
    sparksubmissionid=`grep submissionId /tmp/spark-submission.txt | awk '{print $3}' | cut -d'"' -f 2`
    tellandwaitnsecs 15
    
    echo "inject data"
    docker exec -ti client1 python /workdir/ingest.py

    echo "wait for Spark to finish ingesting"
    tellandwaitnsecs 10

    echo "get the result"
    docker exec -ti client1 python /workdir/compare.py

    echo "kill the Spark job"
    echo "Spark submission id is $sparksubmissionid"
    docker exec -ti sparkm1 bash -c "spark-submit --kill $sparksubmissionid --master spark://sparkm1:6066"
}

scenario_sstream()
{
    timeType=$1
    echo "starting Structured Streaming scenario with $timeType"

    echo "Initial content in the Cassandra database"    docker exec -ti cassandra3 cqlsh --execute "use boontadata; select count(*) as nb_debug from debug; select count(*) as nb_rawevents from raw_events; select count(*) as nb_aggevents from agg_events;"

    echo "start sstream job"
    docker exec -ti sstreamm1 bash -c ". start-job.sh" | tee /tmp/sstream-submission.txt
    sparksubmissionid=`grep submissionId /tmp/spark-submission.txt | awk '{print $3}' | cut -d'"' -f 2`
    tellandwaitnsecs 15

    echo "inject data"
    docker exec -ti client1 python /workdir/ingest.py

    echo "wait for sstream to finish ingesting"
    tellandwaitnsecs 10

    echo "get the result"
    docker exec -ti client1 python /workdir/compare.py

    echo "kill the sstream job"
    echo "sstream submission id is $sparksubmissionid"
    docker exec -ti sstreamm1 bash -c "spark-submit --kill $sparksubmissionid --master spark://sstreamm1:6066"
}

scenario_truncate()
{
    echo "Initial content in the Cassandra database"
    docker exec -ti cassandra3 cqlsh --execute "use boontadata; select count(*) as nb_debug from debug; select count(*) as nb_rawevents from raw_events; select count(*) as nb_aggevents from agg_events;"

    echo "truncate"
    docker exec -ti cassandra3 cqlsh --execute "use boontadata; truncate table debug; truncate table raw_events; truncate table agg_events;"

    echo "new content in the Cassandra database"
    docker exec -ti cassandra3 cqlsh --execute "use boontadata; select count(*) as nb_debug from debug; select count(*) as nb_rawevents from raw_events; select count(*) as nb_aggevents from agg_events;"
    
}

scenario_test_cassandra()
{
    echo "get the first lines in Cassandra's debug table"
    docker exec -ti cassandra3 cqlsh --execute "select * from boontadata.debug limit 5;"
}

case $scenario in
    flink1)
        scenario_truncate
        scenario_flink ProcessingTime
        ;;
    flink2)
        scenario_truncate
        scenario_flink EventTime
        ;;
    spark1)
        scenario_truncate
        scenario_spark ProcessingTime
        ;;
     sstream1)
        scenario_truncate
        scenario_sstream ProcessingTime
        ;;
    truncate)
        scenario_truncate
        ;;
    test_cassandra)
        scenario_test_cassandra
        ;;
    *)
        echo "scenario $scenario is not implemented (yet?)."
esac

return 0
