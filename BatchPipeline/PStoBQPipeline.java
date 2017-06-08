package be.thinkcore.backbone.dataflow;

import java.util.ArrayList;
import java.util.List;
import java.util.Formatter.BigDecimalLayoutForm;

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
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import be.thinkcore.backbone.dataflow.PubSubPipeline.MyOptions;

public class PStoBQPipeline {

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
		/**************************
		 * Variables and constants
		 **************************/
		String destination = "lkn-muntstraat:Jouleboulevard.pubsub_testing";
		String sideSource = "gs://muntstraat/MetadataFlukso.csv";
		String rawDataDestination = "lkn-muntstraat:Jouleboulevard.raw_data";


		/**************************
		 * 		Pipeline setup
		 **************************/
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
		options.setStreaming(true);
		Pipeline pipe = Pipeline.create(options);


		/****************************
		 * Reading data from Pub/Sub
		 ****************************/
		PCollection<String> rawPubSub = pipe.apply(PubsubIO.Read.named("PubSubStreamRead")
				.subscription("projects/lkn-muntstraat/subscriptions/dataflow_subscription"));

		/**************************
		 * 	Parsing Pub/Sub data
		 **************************/
		@SuppressWarnings("serial")
		PCollection<TableRow> parsedPubSub = rawPubSub.apply(ParDo.named("Parsing raw PubSub data").of(new DoFn<String, TableRow>(){
			@Override
			public void processElement(ProcessContext c){
				String input = c.element();
				String delimiter = ",";
				String[] tokens = input.split(delimiter);				

				if(tokens.length == 3 && !tokens[0].equals("") && !tokens[1].equals("") && !tokens[2].equals("")){
					TableRow row = new TableRow();
					row.put("datetime", tokens[0]);
					row.put("consumption", tokens[1]);
					row.put("meterID", tokens[2]);
					c.output(row);
				} else {
					LOG.debug("A faulty row was encountered: \t" + c.element() + "\n");
				}
			}
		}));


		/*******************************
		 * Writing Raw Data to BigQuery
		 *******************************/
		//Defining the BigQuery table scheme
		List<TableFieldSchema> rawFields = new ArrayList<>();
		rawFields.add(new TableFieldSchema().setName("datetime").setType("TIMESTAMP").setMode("REQUIRED"));
		rawFields.add(new TableFieldSchema().setName("consumption").setType("FLOAT").setMode("REQUIRED"));
		rawFields.add(new TableFieldSchema().setName("meterID").setType("STRING").setMode("REQUIRED"));
		TableSchema rawSchema = new TableSchema().setFields(rawFields);

		parsedPubSub.apply(BigQueryIO.Write.named("Writing raw data to BQ")
				.to(rawDataDestination)
				.withSchema(rawSchema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withoutValidation());		 
		
		
		/*******************************
		 * Reading metadata from storage
		 *******************************/
		PCollection<String> metadata = pipe.apply(TextIO.Read.named("Read metadata from storage")
				.from(sideSource));
		
		/*********************************
		 * PreProc metadata for sideinput
		 *********************************/
		final PCollectionView<List<String>> metaList = metadata.apply(View.<String>asList());
		
		/***********************************
		 * Combining raw data and metadata
		 ***********************************/
		PCollection<TableRow> combinedData = parsedPubSub.apply(ParDo.named("Combining raw and metadata")
				.withSideInputs(metaList).of(new DoFn<TableRow, TableRow>(){
					@Override
					public void processElement(ProcessContext c){
						//TODO add sideoutput for rejected rows???
						TableRow currentElement = c.element();
						for(int i = 0; i < c.sideInput(metaList).size(); i++){
							String [] sideData = c.sideInput(metaList).get(i).split(",");
							if(!sideData[0].equals("Meter ID") && currentElement.get("meterID").equals(sideData[0])){
								currentElement.put("sensorToken", sideData[1]);
								currentElement.put("fluksoID", sideData[2]);
								currentElement.put("sensorType", sideData[3]);
								currentElement.put("buildingName", sideData[4]);
								c.output(currentElement);
								break;
							}
						}
					}
				}));
		
		/***********************************
		 *   Writing combined data to BQ
		 ***********************************/
		List<TableFieldSchema> combinedFields = new ArrayList<>();
		combinedFields.add(new TableFieldSchema().setName("datetime").setType("TIMESTAMP").setMode("REQUIRED"));
		combinedFields.add(new TableFieldSchema().setName("consumption").setType("FLOAT").setMode("REQUIRED"));
		combinedFields.add(new TableFieldSchema().setName("meterID").setType("STRING").setMode("REQUIRED"));
		combinedFields.add(new TableFieldSchema().setName("sensorToken").setType("STRING").setMode("REQUIRED"));
		combinedFields.add(new TableFieldSchema().setName("fluksoID").setType("STRING").setMode("REQUIRED"));
		combinedFields.add(new TableFieldSchema().setName("sensorType").setType("STRING").setMode("REQUIRED"));
		combinedFields.add(new TableFieldSchema().setName("buildingName").setType("STRING").setMode("REQUIRED"));
		TableSchema combinedSchema = new TableSchema().setFields(combinedFields);
		
		combinedData.apply(BigQueryIO.Write.named("Write combined data to BQ")
				.withSchema(combinedSchema)
				.to(destination)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withoutValidation());

		pipe.run();
	}

}
