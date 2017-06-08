package be.thinkcore.backbone.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

public class SideInputTestPipeline {

	private static final Logger LOG = LoggerFactory.getLogger(BatchPipeline.class);



	public static void main(String[] args) {	
		/*
		 * gs://temp-data-flukso/test_data/*.csv
		 * gs://muntstraat/csvs/*.csv
		 */
		String source = "gs://temp-data-flukso/test_data/*.csv";
		String destination = "lkn-muntstraat:Jouleboulevard.sideinput_minute";
		String sideSource = "gs://muntstraat/MetadataFlukso.csv";

		PipelineOptions myOptions = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		Pipeline pipe = Pipeline.create(myOptions);


		PCollection<String> line = pipe.apply(TextIO.Read.named("ReadDataFromStorage")
				.from(source));

		PCollection<String> metaLine = pipe.apply(TextIO.Read.named("FetchMetadata")
				.from(sideSource));

		//PCollectionList<String> combined = PCollectionList.of(line).and(metaLine);
		final PCollectionView<List<String>> metaList = metaLine.apply(View.<String>asList());
		//int partition_amount;
		

		@SuppressWarnings("serial")
		PCollection<TableRow> parsedData = line.apply(ParDo.named("ParsingCombinedData").withSideInputs(metaList).of(new DoFn<String,TableRow>(){
			@Override
			public void processElement(ProcessContext c){
				String[] csvData = c.element().split(",");	
				if(checkSplitter(csvData, 3)){
					for(int i = 0; i < c.sideInput(metaList).size(); i++){
						String [] sideData = c.sideInput(metaList).get(i).split(",");
						if(checkSplitter(sideData, 5) && sideData[0].equals(csvData[2])){
							TableRow row = new TableRow();
							row.put("datetime", csvData[0]);
							row.put("consumption", csvData[1]);
							row.put("meterID", csvData[2]);
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
		
		
		//TODO remove loop & String
		String [] temp = {"One","Two","Three","Four"};
		for(int i = 0; i < 4; i++){
			 
			//Takes the parsed data and writes it to the destination BigQuery table, truncate will empty the table and refill it
			parsedData.apply(BigQueryIO.Write
					.named("BigQueryWrite"+temp[i])
					.to(destination+temp[i])
					.withSchema(schema)
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
					.withoutValidation());
		}

		//Runs the pipeline
		pipe.run();


	}
}
