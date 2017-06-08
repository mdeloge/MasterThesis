package be.thinkcore.backbone.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.Pipeline.PipelineExecutionException;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineWorkerPoolOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
//import com.google.cloud.dataflow.sdk.transforms.Partition;
import com.google.cloud.dataflow.sdk.values.PCollection;
//import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionView;


public class SideInputPipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(BatchPipeline.class);

	/*
	 * https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/io/BigQueryIO
	 */
	public static void main(String[] args) {	
		/*
		 * VARIABLES
		 * "gs://muntstraat/csvs_15min/*.csv"
		 * "gs://temp-data-flukso/test_data/*.csv"
		 */
		String source = "gs://temp-data-flukso/test_data/*.csv";
		String destination = "lkn-muntstraat:Jouleboulevard.sideinputtest";
		String sideInputLocation = "gs://muntstraat/MetadataFlukso.csv";
		
		
		PipelineOptions myOptions = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		//Creation of the pipeline with default arguments
		Pipeline p = Pipeline.create(myOptions);
		PCollection<String> line = p.apply(TextIO.Read.named("ReadDataFromStorage")
				.from(source));

		
		//creating the sideinput Pipeline with metadata
		Pipeline sideinput = Pipeline.create(myOptions);
		PCollection<String> metadata = sideinput.apply(TextIO.Read.named("ReadMetadataFromStorage")
											.from(sideInputLocation));
		
		@SuppressWarnings("serial")
		PCollection<TableRow> metadataProcessed = metadata.apply(ParDo.named("ProcessingMetadata")
														.of(new DoFn<String, TableRow>(){
			@Override
	    	public void processElement(ProcessContext c){
				String input = c.element();
				String delimiter = ",";
				String[] tokens = input.split(delimiter);				
				
				if(tokens.length == 5 && !tokens[0].equals("") 
						&& !tokens[1].equals("") && !tokens[2].equals("")
						&& !tokens[3].equals("") && !tokens[4].equals("")){
					TableRow row = new TableRow();
					row.put("meterID", tokens[0]);
					row.put("sensorToken", tokens[1]);
					row.put("fluksoID", tokens[2]);
					row.put("sensorType", tokens[3]);
					row.put("buildingName", tokens[4]);
					c.output(row);
				} else {
					LOG.debug("A faulty row was encountered: \t" + c.element() + "\n");
				}
			}
		}));
		
		final PCollectionView<List<TableRow>> metadataProcList = metadataProcessed.apply(View.<TableRow>asList());
		
		/*
		 * Parsing all individual String lines into TableRow objects that can be written to BigQuery
		 * If the data is not complete no TableRow object is returned and a log message is printed to the console
		 * 
		 * ParDo.named("ParsingCSVLines").withSideInputs(sideInputs).of(new DoFn<String, TableRow>()
		 */
		@SuppressWarnings("serial")
		PCollection<TableRow> tablerows = line.apply(ParDo.withSideInputs(metadataProcList)
											  .named("ParsingCSVLines")
											  .of(new DoFn<String, TableRow>(){
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
		
		
		//Defining the BigQuery table scheme
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("datetime").setType("TIMESTAMP").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("consumption").setType("FLOAT").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("meterID").setType("STRING").setMode("REQUIRED"));
		TableSchema schema = new TableSchema().setFields(fields);
		String table = destination;
		
		
		TimePartitioning timePart = new TimePartitioning();
		timePart.setType("DAY");
		//TODO add code here to link timepartitioning to PCollection
		
		
		
		/*
		 * Writing the TableRow object to BigQuery
		 * WriteDisposition.WRITE_APPEND: the data will be added to the existing table, already stored data remains
		 * CreateDisposition.CREATE_IF_NEEDED: if the table doesn't exist it will be created with the provided scheme
		 */
		tablerows.apply(BigQueryIO.Write
				.named("BigQueryWrite")
				.to(table)
				.withSchema(schema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withoutValidation());

		//Runs the pipeline
		p.run();
		
		//Listing staged files to debug deploy issues
		List<String> stagedFiles = myOptions.as(DataflowPipelineWorkerPoolOptions.class).getFilesToStage();
		for(String stagedFile : stagedFiles){
			System.out.println(stagedFile);
		}
	}
}

