package be.thinkcore.dataflow.thesis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TableRowJsonCoder;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;



public class PubSubEnergyID {
	/*
	 * VARIABLES "lkn-muntstraat:Jouleboulevard.EnergieID_15min"
	 * "gs://jouleboulevard2/Jouleboulevard_metadata.csv"
	 */
	// "gs://jouleboulevard2/energyiddev.blob.core.windows.net/jouleboulevard2/*/*/15min/*.csv"
	private static final Logger LOG = LoggerFactory
			.getLogger(PubSubEnergyID.class);

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		String sourceTopic = "projects/lkn-muntstraat/subscriptions/dataflow_subscription";
		
		String projectID = "lkn-muntstraat";
		String datasetID = "Jouleboulevard";
		String tableID = "pubsubfulltest";
		String destination = projectID + ":" + datasetID + "." + tableID;
		
		String sideSource = "gs://jouleboulevard2/sync_list/Jouleboulevard_metadata.csv";
		String unParseableDataDest = "lkn-muntstraat:Jouleboulevard.EnergieID_unParseable";



		PipelineOptions pipelineoptions = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		Pipeline p = Pipeline.create(pipelineoptions);


		PCollection<String> pubsubData = p.apply("Pubsub read", 
				PubsubIO.<String>read()
				.topic("projects/lkn-muntstraat/topics/dataflow_data")
				.subscription("projects/lkn-muntstraat/subscriptions/dataflow_subscription")
				.withCoder(StringUtf8Coder.of()));
		
		

		final TupleTag<TableRow> validatedData = new TupleTag<TableRow>();
		final TupleTag<TableRow> unvalidatedData = new TupleTag<TableRow>();
		final TupleTag<TableRow> unprocessableData = new TupleTag<TableRow>();

		PCollectionTuple results = pubsubData.apply("Parsing data", 
				ParDo.withOutputTags(validatedData, TupleTagList.of(unvalidatedData).and(unprocessableData)).of(new DoFn<String, TableRow>(){
					@ProcessElement
					public void processElement(ProcessContext c) {
						String[] sideinput = getMetadata();
						String[] measuredData = c.element().split(";");
						if (measuredData.length == 3) {
							boolean parseable = false;
							for (int i = 0; i < sideinput.length; i++) {
								String[] sideData = sideinput[i].split(";");
								if (sideData[0].equals(measuredData[2])) {
									TableRow row = new TableRow();
									row.put("Datetime", measuredData[0]);
									row.put("Consumption", measuredData[1]);
									row.put("MeterId", measuredData[2]);
									row.put("MeterName", sideData[1]);
									row.put("Type", sideData[2]);
									row.put("Unit", sideData[3]);
									row.put("RecordNumber", sideData[4]);
									row.put("RecordName", sideData[5]);
									row.put("DwellingType", sideData[6]);
									row.put("ConstructionYear", sideData[7]);
									row.put("RenovationYear", sideData[8]);
									row.put("Country", sideData[9]);
									row.put("PostalCode", Integer.parseInt(sideData[10]));
									try{
										row.put("FloorSurface", Integer.parseInt(sideData[11]));
									}catch (NumberFormatException e){
										row.put("FloorSurface", -1);
									}
									try{
										row.put("HouseholdSize", Integer.parseInt(sideData[12]));
									}catch (NumberFormatException e){
										row.put("HouseholdSize", -1);
									}
									row.put("HeatingOn", sideData[13]);
									row.put("CookingOn", sideData[14]);
									row.put("HotWaterOn", sideData[15]);
									try{
										row.put("EnergyPerformance", Integer.parseInt(sideData[16]));
									}catch (NumberFormatException e){
										row.put("EnergyPerformance", -1);
									}
									try{
										row.put("EnergyRating", Integer.parseInt(sideData[17]));
									}catch (NumberFormatException e){
										row.put("EnergyRating", -1);
									}
									row.put("Category", sideData[18]);
									row.put("EnergyEfficiency", sideData[19]);
									row.put("AuxiliaryHeatingOn", sideData[20]);
									row.put("Installations", sideData[21]);
									row.put("StreetAddress", sideData[22]);
									row.put("Email", sideData[23]);
									row.put("BusinessName", sideData[24]);
									row.put("FullName", sideData[25]);
									try{
										row.put("Multiplier", Integer.parseInt(sideData[26]));
									}catch (NumberFormatException e){
										row.put("Multiplier", -1);
									}
									row.put("ReadingType", sideData[27]);

									parseable = true;
									c.output(row);
									break;
								}
							}
							if (!parseable) {
								TableRow row = new TableRow();
								row.put("Datetime", measuredData[0]);
								row.put("Consumption", measuredData[1]);
								row.put("MeterID", measuredData[2]);
								c.sideOutput(unvalidatedData, row);
							}
						} else {
							TableRow row = new TableRow();
							row.put("Data", c.element());
							c.sideOutput(unprocessableData, row);
						}
					}

					private String[] getMetadata(){
						String [] output = null;
						try {
							URL url = new URL("https://storage.googleapis.com/jouleboulevard2/sync_list/Jouleboulevard_metadata.csv");
							BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
							String file = "";
							String inputLine = in.readLine();

							while ((inputLine = in.readLine()) != null)
								file += inputLine + "\n";
							in.close();
							output = file.split("\n");
						} catch (IOException e) {
							e.printStackTrace();
						}
						return output;
					}
				}));

		PCollection<TableRow> parsedData = results.get(validatedData).setCoder(TableRowJsonCoder.of());
		PCollection<TableRow> unparseableData = results.get(unvalidatedData).setCoder(TableRowJsonCoder.of());
		PCollection<TableRow> unprocessedData = results.get(unprocessableData).setCoder(TableRowJsonCoder.of());

		// Defining the BigQuery table scheme
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("Datetime").setType("TIMESTAMP").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("Consumption").setType("FLOAT").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("MeterId").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("MeterName").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("Type").setType("STRING").setMode("REQUIRED"));
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
		TableSchema schema = new TableSchema().setFields(fields);
		
		
		PCollection<TableRow> parsedWindowed = parsedData.apply("Windowed Parseable", 
				Window.<TableRow>into(FixedWindows
							.of(Duration.standardMinutes(1))));

		/*
		 * Writing the TableRow object to BigQuery
		 * WriteDisposition.WRITE_APPEND: the data will be added to the existing
		 * table, already stored data remains
		 * CreateDisposition.CREATE_IF_NEEDED: if the table doesn't exist it
		 * will be created with the provided scheme
		 */
		parsedWindowed.apply("Main BQ Write", 
				BigQueryIO.Write
					.toTableReference(TableReferencePartition.perDay(projectID, datasetID, tableID))
					.withSchema(schema)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
					.withoutValidation());
		
		

		
		// Defining the BigQuery table scheme
		List<TableFieldSchema> unparseableFields = new ArrayList<>();
		unparseableFields.add(new TableFieldSchema().setName("Datetime").setType("TIMESTAMP").setMode("REQUIRED"));
		unparseableFields.add(new TableFieldSchema().setName("Consumption").setType("FLOAT").setMode("REQUIRED"));
		unparseableFields.add(new TableFieldSchema().setName("MeterId").setType("STRING").setMode("REQUIRED"));
		TableSchema unparseableSchema = new TableSchema().setFields(unparseableFields);
		
		
		unparseableData.apply("Unparseable BQ Write", 
				BigQueryIO.Write
					.to(destination + "_unparseable")
					.withSchema(unparseableSchema)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
					.withoutValidation());
		
		
		// Defining the BigQuery table scheme
		List<TableFieldSchema> unprocessableFields = new ArrayList<>();
		unprocessableFields.add(new TableFieldSchema().setName("Data").setType("STRING").setMode("REQUIRED"));
		TableSchema unprocessableSchema = new TableSchema().setFields(unprocessableFields);
				
		unprocessedData.apply("Unprocessed BQ Write", 
				BigQueryIO.Write
					.to(destination + "_unprocessable")
					.withSchema(unprocessableSchema)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
					.withoutValidation());
		
		
		p.run();

	}

}


















