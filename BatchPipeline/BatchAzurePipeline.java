package be.thinkcore.backbone.dataflow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
//import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineWorkerPoolOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.values.PCollection;


public class BatchAzurePipeline {

	private static final Logger LOG = LoggerFactory.getLogger(BatchAzurePipeline.class);
	private static final String metaLocation = "https://storage.googleapis.com/jouleboulevard-azure/muntstraat_testing.csv";
	//private transient StorageChecker checker = new StorageChecker();
	private transient static ArrayList<EnergyIDRow> metadata;


	public static void main(String[] args) throws IOException {	

		URL url = new URL(metaLocation);
		InputStream in = url.openStream();	    
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line;
		metadata = new ArrayList<>();
		int i = 0;
		while ((line = reader.readLine()) != null) {
			if(i != 0){
				EnergyIDRow row = new EnergyIDRow(line);
				if(row.isValidRow()){
					metadata.add(row);
				}
			}
			i++;
		}
		reader.close();

		PipelineOptions myOptions = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		//Creation of the pipeline with default arguments
		Pipeline p = Pipeline.create(myOptions);

		

		for(EnergyIDRow data: metadata){
			
			//TODO implement StorageChecker (once it works)
			String source = "gs://jouleboulevard-azure/energyiddev.blob.core.windows.net/"
					+ "jouleboulevard/EA-14105155/" + data.getMeterID() + "/15min/*.csv";

			PCollection<String> dataline = p.apply(TextIO.Read.named("ReadData | " + data.getMeterID())
					.from(source));

			@SuppressWarnings("serial")
			PCollection<TableRow> tablerows = dataline.apply(ParDo.named("Parsing | " + data.getMeterID()).of(new DoFn<String, TableRow>(){
				@Override
				public void processElement(ProcessContext c){
					String input = c.element();
					String delimiter = ",";
					String[] tokens = input.split(delimiter);				

					if(tokens.length == 2 && !tokens[0].equals("") && !tokens[1].equals("")){
						TableRow row = new TableRow();
						row.put("datetime", tokens[0]);
						row.put("consumption", tokens[1]);
						row.put("meterID", data.getDataStreamToken());
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
			String table = "lkn-muntstraat:AzureSync." + data.getMeterID();

			tablerows.apply(BigQueryIO.Write
					.named("BigQueryWrite | " + data.getMeterID())
					.to(table)
					.withSchema(schema)
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
					.withoutValidation());


		}//end of loop

		p.run();

		//Listing staged files to debug deploy issues
		List<String> stagedFiles = myOptions.as(DataflowPipelineWorkerPoolOptions.class).getFilesToStage();
		for(String stagedFile : stagedFiles){
			System.out.println(stagedFile);
		}

	}
}
