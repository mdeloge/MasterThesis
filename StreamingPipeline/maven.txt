
mvn compile exec:java -Dexec.mainClass=be.thinkcore.dataflow.thesis.PubSubEnergyID -Dexec.args="--runner=DataflowRunner --project=lkn-muntstraat --streaming=true --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=1 --maxNumWorkers=2 --workerMachineType=n1-standard-1 --gcpTempLocation=gs://jouleboulevard2/tmp --zone=europe-west1-d" -Pdataflow-runner
