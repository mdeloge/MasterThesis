package release;

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
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
//import com.google.cloud.dataflow.sdk.transforms.Partition;
import com.google.cloud.dataflow.sdk.values.PCollection;
//import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionView;


public class BatchEnergyID {
	
	private static final Logger LOG = LoggerFactory.getLogger(BatchEnergyID.class);

	public static void main(String[] args) {	
		
		String EANumber = "EA-1412";
		
		/*
		 * VARIABLES
		 * "lkn-muntstraat:Jouleboulevard.EnergieID_15min"
		 * "gs://jouleboulevard2/Jouleboulevard_metadata.csv"
		 */
		//"gs://jouleboulevard2/energyiddev.blob.core.windows.net/jouleboulevard2/*/*/15min/*.csv"
		//"gs://jouleboulevard/testdata/*.csv"
		String source = "gs://jouleboulevard2/energyiddev.blob.core.windows.net/jouleboulevard2/" + EANumber + "*/*/15min/*.csv";
		String destination = "lkn-muntstraat:Jouleboulevard.EnergieID_15min_4";
		String sideSource = "gs://jouleboulevard2/sync_list/Jouleboulevard_metadata.csv";
		
		
		
		PipelineOptions myOptions = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		//Creation of the pipeline with default arguments
		Pipeline p = Pipeline.create(myOptions);
	
		
		PCollection<String> line = p.apply(TextIO.Read.named(EANumber + "ReadFromGCS_" + destination.substring(destination.lastIndexOf("_") + 1))
				.from(source));
		
		PCollection<String> meta = p.apply(TextIO.Read.named("FetchMetadata")
				.from(sideSource));
		
		@SuppressWarnings("serial")
		PCollection<String[]> metaParsed = meta.apply(ParDo.named("ParsingMetaData").of(new DoFn<String,String[]>(){
			@Override
			public void processElement(ProcessContext c){
				String [] meta = c.element().split(";");
				if(meta.length == 28 && !meta[0].equals("MeterId")){
					c.output(meta);
				}
			}
		}));
		
		final PCollectionView<List<String[]>> metaList = metaParsed.apply(View.<String[]>asList());
		
		@SuppressWarnings("serial")
		PCollection<TableRow> parsedData = line.apply(ParDo.named("ParsingCombinedData").withSideInputs(metaList).of(new DoFn<String,TableRow>(){
			@Override
			public void processElement(ProcessContext c){
				String[] csvData = c.element().split(",");	
				if(checkSplitter(csvData, 3)){
					for(int i = 0; i < c.sideInput(metaList).size(); i++){
						String [] sideData = c.sideInput(metaList).get(i);
						if(sideData[0].equals(csvData[2])){
							TableRow row = new TableRow();
							row.put("datetime", csvData[0]);
							row.put("consumption", csvData[1]);
							row.put("meterID", csvData[2]);
							row.put("meterName", sideData[1]);
							row.put("sensorType", sideData[2]);
							row.put("unit", sideData[3]);
							row.put("recordNumber", sideData[4]);
							row.put("recordName", sideData[5]);
							row.put("dwellingType", sideData[6]);
							row.put("constructionYear", sideData[7]);
							row.put("renovationYear", sideData[8]);
							row.put("country", sideData[9]);
							row.put("postalCode", sideData[10]);
							row.put("floorSurface", sideData[11]);
							row.put("householdSize", sideData[12]);
							row.put("heatingOn", sideData[13]);
							row.put("cookingOn", sideData[14]);
							row.put("hotWaterOn", sideData[15]);
							row.put("energyPerformance", sideData[16]);
							row.put("energyRating", sideData[17]);
							row.put("category", sideData[18]);
							row.put("energyEfficiency", sideData[19]);
							row.put("auxiliaryHeatingOn", sideData[20]);
							row.put("installations", sideData[21]);
							row.put("streetAddress", sideData[22]);
							row.put("email", sideData[23]);
							row.put("businessName", sideData[24]);
							row.put("fullName", sideData[25]);
							row.put("multiplier", sideData[26]);
							row.put("readingType", sideData[27]);
							
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
						if(split[i].equals("") || split[i].equals("MeterId") || split[i].equals("consumption")){
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
		fields.add(new TableFieldSchema().setName("meterName").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("sensorType").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("unit").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("recordNumber").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("recordName").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("dwellingType").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("constructionYear").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("renovationYear").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("country").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("postalCode").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("floorSurface").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("householdSize").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("heatingOn").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("cookingOn").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("hotWaterOn").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("energyPerformance").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("energyRating").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("category").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("energyEfficiency").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("auxiliaryHeatingOn").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("installations").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("streetAddress").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("email").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("businessName").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("fullName").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("multiplier").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("readingType").setType("STRING").setMode("REQUIRED"));
		TableSchema schema = new TableSchema().setFields(fields);
		
		/*
		 * Writing the TableRow object to BigQuery
		 * WriteDisposition.WRITE_APPEND: the data will be added to the existing table, already stored data remains
		 * CreateDisposition.CREATE_IF_NEEDED: if the table doesn't exist it will be created with the provided scheme
		 */
		parsedData.apply(BigQueryIO.Write
				.named("BigQueryWrite_" + destination.substring(destination.lastIndexOf("_") + 1))
				.to(destination)
				.withSchema(schema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withoutValidation());

		//Runs the pipeline
		p.run();
	}
}
