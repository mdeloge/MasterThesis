Running the Pipeline

mvn compile exec:java -Dexec.mainClass=be.thinkcore.dataflow.thesis.PubSubEnergyID \
     -Dexec.args="--runner=DataflowRunner --project=lkn-muntstraat \
                  --gcpTempLocation=gs://jouleboulevard2/tmp" \
     -Pdataflow-runner
     
     
mvn compile exec:java -Dexec.mainClass=be.thinkcore.dataflow.thesis.PubSubEnergyID -Dexec.args="--runner=DataflowRunner --project=lkn-muntstraat --streaming=true --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=1 --maxNumWorkers=2 --workerMachineType=n1-standard-1 --gcpTempLocation=gs://jouleboulevard2/tmp --zone=europe-west1-d" -Pdataflow-runner
     
     
 * <h3>Writing</h3>
 *
 * <p>To write to a BigQuery table, apply a {@link BigQueryIO.Write} transformation.
 * This consumes either a {@link PCollection} of {@link TableRow TableRows} as input when using
 * {@link BigQueryIO#writeTableRows()} or of a user-defined type when using
 * {@link BigQueryIO#write()}. When using a user-defined type, a function must be provided to
 * turn this type into a {@link TableRow} using
 * {@link BigQueryIO.Write#withFormatFunction(SerializableFunction)}.
 * <pre>{@code
 * PCollection<TableRow> quotes = ...
 *
 * List<TableFieldSchema> fields = new ArrayList<>();
 * fields.add(new TableFieldSchema().setName("source").setType("STRING"));
 * fields.add(new TableFieldSchema().setName("quote").setType("STRING"));
 * TableSchema schema = new TableSchema().setFields(fields);
 *
 * quotes.apply(BigQueryIO.writeTableRows()
 *     .to("my-project:output.output_table")
 *     .withSchema(schema)
 *     .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
 * }</pre>
 
   
<p>A common use case is to dynamically generate BigQuery table names based on
 * the current window or the current value. To support this,
 * {@link BigQueryIO.Write#to(SerializableFunction)}
 * accepts a function mapping the current element to a tablespec. For example,
 * here's code that outputs daily tables to BigQuery:
 * <pre>{@code
 * PCollection<TableRow> quotes = ...
 * quotes.apply(Window.<TableRow>into(CalendarWindows.days(1)))
 *       .apply(BigQueryIO.writeTableRows()
 *         .withSchema(schema)
 *         .to(new SerializableFunction<ValueInSingleWindow, String>() {
 *           public String apply(ValueInSingleWindow value) {
 *             // The cast below is safe because CalendarWindows.days(1) produces IntervalWindows.
 *             String dayString = DateTimeFormat.forPattern("yyyy_MM_dd")
 *                  .withZone(DateTimeZone.UTC)
 *                  .print(((IntervalWindow) value.getWindow()).start());
 *             return "my-project:output.output_table_" + dayString;
 *           }
 *         }));
 * }</pre>
 
 