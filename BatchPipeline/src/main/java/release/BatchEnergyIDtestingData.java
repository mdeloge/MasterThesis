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


public class BatchEnergyIDtestingData {
	
	private static final Logger LOG = LoggerFactory.getLogger(BatchEnergyIDtestingData.class);

	public static void main(String[] args) {	
		String dataset = "large_wide";
		
		String source = "gs://temp-jouleboulevard/test/testing_" + dataset + ".csv";
		String destination = "lkn-muntstraat:BigQueryTestingThesis." + dataset;
		
		PipelineOptions myOptions = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		//Creation of the pipeline with default arguments
		Pipeline p = Pipeline.create(myOptions);
	
		
		PCollection<String> line = p.apply(TextIO.Read.named("ReadFromCloudStorage")
				.from(source));
		
		
		@SuppressWarnings("serial")
		PCollection<TableRow> parsedData = line.apply(ParDo.named("ParsingData").of(new DoFn<String,TableRow>(){
			@Override
			public void processElement(ProcessContext c){
				String[] csvData = c.element().split(",");
				if(!csvData[0].equals("Datetime")){
					if(csvData.length == 3){
						TableRow row = new TableRow();
						row.put("Datetime", csvData[0]);
						row.put("Consumption", csvData[1]);
						row.put("MeterId", csvData[2]);
						
						c.output(row);
					} 
					else {
						TableRow row = new TableRow();
						row.put("Datetime", csvData[0]);
						row.put("Consumption", csvData[1]);
						row.put("MeterId", csvData[2]);
						row.put("MeterName", csvData[3]);
						row.put("SensorType", csvData[4]);
						row.put("Unit", csvData[5]);
						row.put("RecordNumber", csvData[6]);
						row.put("RecordName", csvData[7]);
						row.put("DwellingType", csvData[8]);
						row.put("ConstructionYear", csvData[9]);
						row.put("RenovationYear", csvData[10]);
						row.put("Country", csvData[11]);
						row.put("PostalCode", csvData[12]);
						row.put("FloorSurface", csvData[13]);
						row.put("HouseholdSize", csvData[14]);
						row.put("HeatingOn", csvData[15]);
						row.put("CookingOn", csvData[16]);
						row.put("HotWaterOn", csvData[17]);
						row.put("EnergyPerformance", csvData[18]);
						row.put("EnergyRating", csvData[19]);
						row.put("Category", csvData[20]);
						row.put("EnergyEfficiency", csvData[21]);
						row.put("AuxiliaryHeatingOn", csvData[22]);
						row.put("Installations", csvData[23]);
						row.put("StreetAddress", csvData[24]);
						row.put("Email", csvData[25]);
						row.put("BusinessName", csvData[26]);
						row.put("FullName", csvData[27]);
						row.put("Multiplier", csvData[28]);
						row.put("ReadingType", csvData[29]);
						
						c.output(row);
					}
				}
			}
		
			
		}));
		
		
		//Defining the BigQuery table scheme
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("Datetime").setType("TIMESTAMP").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("Consumption").setType("FLOAT").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("MeterId").setType("STRING").setMode("REQUIRED"));
		
		String[] test = dataset.split("_");
		if(test[1].equals("wide")){		
			fields.add(new TableFieldSchema().setName("MeterName").setType("STRING").setMode("REQUIRED"));
			fields.add(new TableFieldSchema().setName("SensorType").setType("STRING").setMode("REQUIRED"));
			fields.add(new TableFieldSchema().setName("Unit").setType("STRING").setMode("REQUIRED"));
			fields.add(new TableFieldSchema().setName("RecordNumber").setType("STRING").setMode("REQUIRED"));
			fields.add(new TableFieldSchema().setName("RecordName").setType("STRING").setMode("REQUIRED"));
			fields.add(new TableFieldSchema().setName("DwellingType").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("ConstructionYear").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("RenovationYear").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("Country").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("PostalCode").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("FloorSurface").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("HouseholdSize").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("HeatingOn").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("CookingOn").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("HotWaterOn").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("EnergyPerformance").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("EnergyRating").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("Category").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("EnergyEfficiency").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("AuxiliaryHeatingOn").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("Installations").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("StreetAddress").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("Email").setType("STRING").setMode("REQUIRED"));
			fields.add(new TableFieldSchema().setName("BusinessName").setType("STRING").setMode("REQUIRED"));
			fields.add(new TableFieldSchema().setName("FullName").setType("STRING").setMode("REQUIRED"));
			fields.add(new TableFieldSchema().setName("Multiplier").setType("STRING").setMode("REQUIRED"));
			fields.add(new TableFieldSchema().setName("ReadingType").setType("STRING").setMode("REQUIRED"));
		}
		TableSchema schema = new TableSchema().setFields(fields);
		
		/*
		 * Writing the TableRow object to BigQuery
		 * WriteDisposition.WRITE_APPEND: the data will be added to the existing table, already stored data remains
		 * CreateDisposition.CREATE_IF_NEEDED: if the table doesn't exist it will be created with the provided scheme
		 */
		parsedData.apply(BigQueryIO.Write
				.named("BigQueryWrite")
				.to(destination)
				.withSchema(schema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withoutValidation());

		//Runs the pipeline
		p.run();
	}
}
