package be.thinkcore.backbone.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

public class PubSubPipeline {

	private static final Logger LOG = LoggerFactory.getLogger(BatchPipeline.class);

	public static interface MyOptions extends DataflowPipelineOptions {
		@Description("Output BigQuery table <project_id>:<dataset_id>.<table_id>")
		@Default.String("lkn-muntstraat:Jouleboulevard")
		String getOutput();

		void setOutput(String s);

		@Description("Input topic")
		@Default.String("projects/lkn-muntstraat/topics/dataflow_data")
		String getInput();

		void setInput(String s);
	}


	public static void main(String[] args) {	
		/*
		 * gs://temp-data-flukso/test_data/*.csv
		 * gs://muntstraat/csvs/*.csv
		 */
		String destination = "lkn-muntstraat:Jouleboulevard.pubsub_testing";
		String sideSource = "gs://muntstraat/MetadataFlukso.csv";
		String rawDataDestination = "lkn-muntstraat:Jouleboulevard.raw_data";

		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
		options.setStreaming(true);
		Pipeline pipe = Pipeline.create(options);


		PCollection<String> line = pipe.apply(PubsubIO.Read.named("PubSubStreamRead")
				.subscription("projects/lkn-muntstraat/subscriptions/dataflow_subscription"));

		PCollection<String> metaLine = pipe.apply(TextIO.Read.named("FetchMetadata")
				.from(sideSource));

		final PCollectionView<List<String>> metaList = metaLine.apply(View.<String>asList());

		@SuppressWarnings("serial")
		PCollection<TableRow> parsedData = line.apply(ParDo.named("ParsingCombinedData").withSideInputs(metaList).of(new DoFn<String,TableRow>(){
			@Override
			public void processElement(ProcessContext c){
				String[] streamData = c.element().split(",");	
				if(checkSplitter(streamData, 3)){
					for(int i = 0; i < c.sideInput(metaList).size(); i++){
						String [] sideData = c.sideInput(metaList).get(i).split(";");
						if(checkSplitter(sideData, 5) && sideData[0].equals(streamData[2])){
							TableRow row = new TableRow();
							row.put("datetime", streamData[0]);
							row.put("consumption", streamData[1]);
							row.put("meterID", streamData[2]);
							row.put("sensorToken", sideData[1]);
							row.put("fluksoID", sideData[2]);
							row.put("sensorType", sideData[3]);
							row.put("buildingName", sideData[4]);
							c.output(row);
							break;
						}
					}	
				} else {
					LOG.debug("A faulty row was encountered: \t" + c.element() + "\n");
				}				
			}

			//Validates the processed String element before an attempt will be made to create a TableRow object
			private boolean checkSplitter(String[] split, int wantedLength){
				if(split.length == wantedLength){
					for(int i = 0; i < wantedLength; i++){
						if(split[i].equals("") || split[i].equals("Meter ID") || split[i].equals("consumption")){
							return false;
						}
					}					
					return true;
				} else {				
					return false;
				}
			}
		}));

		//Defining the BigQuery table scheme
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("datetime").setType("TIMESTAMP").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("consumption").setType("FLOAT").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("meterID").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("sensorToken").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("fluksoID").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("sensorType").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("buildingName").setType("STRING").setMode("REQUIRED"));
		TableSchema schema = new TableSchema().setFields(fields);


		//Takes the parsed data and writes it to the destination BigQuery table, truncate will empty the table and refill it
		parsedData.apply(BigQueryIO.Write
				.named("BigQueryWrite")
				.to(destination)
				.withSchema(schema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withoutValidation());


		/*
		 ***********************************
		 *       Raw Data Processing
		 *********************************** 
		 */

		//Parsing Raw data for insertion into BigQuery		
		PCollection<TableRow> parsedRawData = line.apply(ParDo.named("ParsingRawData").of(new DoFn<String,TableRow>(){
			@Override
			public void processElement(ProcessContext c){
				String[] streamData = c.element().split(",");	
				if(streamData.length == 3 && streamData[0] != "" && streamData[1] != "" && streamData[2] != ""){
					TableRow row = new TableRow();
					row.put("datetime", streamData[0]);
					row.put("consumption", streamData[1]);
					row.put("meterID", streamData[2]);
					c.output(row);					

				} else {
					LOG.debug("A faulty row was encountered: \t" + c.element() + "\n");
				}				
			}
		}));

		//We will store the raw data in a separate bigquery table as backup
		List<TableFieldSchema> rawFields = new ArrayList<>();
		rawFields.add(new TableFieldSchema().setName("datetime").setType("TIMESTAMP").setMode("REQUIRED"));
		rawFields.add(new TableFieldSchema().setName("consumption").setType("FLOAT").setMode("REQUIRED"));
		rawFields.add(new TableFieldSchema().setName("meterID").setType("STRING").setMode("REQUIRED"));
		TableSchema rawSchema = new TableSchema().setFields(rawFields);

		//Insertion into bigquery
		parsedRawData.apply(BigQueryIO.Write.named("BQ Raw Data Write")
				.to(rawDataDestination)
				.withSchema(rawSchema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withoutValidation());




		//Runs the pipeline
		pipe.run();


	}
}
