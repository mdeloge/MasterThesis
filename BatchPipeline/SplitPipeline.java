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
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineWorkerPoolOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
//import com.google.cloud.dataflow.sdk.transforms.Partition;
import com.google.cloud.dataflow.sdk.values.PCollection;
//import com.google.cloud.dataflow.sdk.values.PCollectionList;


public class SplitPipeline {
	
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
		String destination = "lkn-muntstraat:Jouleboulevard.partitioned_minute";
		
		
		PipelineOptions myOptions = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		//Creation of the pipeline with default arguments
		Pipeline p = Pipeline.create(myOptions);
			
		//New pipeline as a side input, to check which sensors will need to be synced
		//Pipeline pallMeters = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
		//PCollection<String> pallMetersColl = pallMeters.apply(biqgueryio.read);
		//PCollection view van maken om ze te wrappen
	
		/*
		 * Reading all the .csv files from Google Cloud Storage 
		 * Returns PCollection of type String
		 */
		PCollection<String> line = p.apply(TextIO.Read.named("ReadFromCloudStorage")
				.from(source));
		
		
		
		//Defining the BigQuery table scheme
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("datetime").setType("TIMESTAMP").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("consumption").setType("FLOAT").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("meterID").setType("STRING").setMode("REQUIRED"));
		TableSchema schema = new TableSchema().setFields(fields);
		//String table = destination;
		
		
		
		//Splitting up the data
		String [] meterIDs = {"00c273a1bb36e5eba3a93d4e3065ec91", "06686f304bd7c72a86c3810d2965f4a2"};
		
		for(int i = 0; i < meterIDs.length; i++){
			String nameParse = "ParsingCSVlines|" + meterIDs[i];
			String nameWrite = "BigQueryWrite|" + meterIDs[i];
			String table = "lkn-muntstraat:Jouleboulevard.sensor" + meterIDs[i];
			final int index = i;
			
			@SuppressWarnings("serial")
			PCollection<TableRow> tablerows = line.apply(ParDo.named(nameParse).of(new DoFn<String, TableRow>(){
				@Override
		    	public void processElement(ProcessContext c){
					String input = c.element();
					String delimiter = ",";
					String[] tokens = input.split(delimiter);
					
					final String id = meterIDs[index];
					
					if(tokens.length == 3 && !tokens[0].equals("") && !tokens[1].equals("") && tokens[2].equals(id)){
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
			
			/*
			 * Writing the TableRow object to BigQuery
			 * WriteDisposition.WRITE_APPEND: the data will be added to the existing table, already stored data remains
			 * CreateDisposition.CREATE_IF_NEEDED: if the table doesn't exist it will be created with the provided scheme
			 */
			tablerows.apply(BigQueryIO.Write
					.named(nameWrite)
					.to(table)
					.withSchema(schema)
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
					.withoutValidation());
			
		}

		//Runs the pipeline
		p.run();
		
		//Listing staged files to debug deploy issues
		List<String> stagedFiles = myOptions.as(DataflowPipelineWorkerPoolOptions.class).getFilesToStage();
		for(String stagedFile : stagedFiles){
			System.out.println(stagedFile);
		}
	}
}

