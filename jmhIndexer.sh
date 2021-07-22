SECONDS_PER_RUN=5
RUNS="-wi 2 -w $SECONDS_PER_RUN -i 5 -r $SECONDS_PER_RUN"

./gradlew jmhJar || exit 1

java -jar build/libs/*-jmh.jar \
  $RUNS -f 1 -t 1 -bm avgt \
  Indexer \
#  -p _indexerType=LOG_LOOKUP
