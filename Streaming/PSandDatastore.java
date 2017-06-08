package be.thinkcore.dataflow.thesis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TableRowJsonCoder;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.datastore.*;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.datastore.v1.client.DatastoreHelper.getString;
import static com.google.datastore.v1.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.datastore.v1.Key.Builder;
import com.google.cloud.datastore.Datastore;



public class PSandDatastore {
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
		String destination = "lkn-muntstraat:Jouleboulevard.pubsubfulltest";
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

		
		@SuppressWarnings("serial")
		PCollectionTuple results = pubsubData.apply("Parsing data", 
				ParDo.withOutputTags(validatedData, TupleTagList.of(unvalidatedData)).of(new DoFn<String, TableRow>(){
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
									row.put("datetime", measuredData[0]);
									row.put("consumption", measuredData[1]);
									row.put("meterID", measuredData[2]);
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

									parseable = true;
									c.output(row);
									break;
								}
							}
							if (!parseable) {
								TableRow row = new TableRow();
								row.put("datetime", measuredData[0]);
								row.put("consumption", measuredData[1]);
								row.put("meterID", measuredData[2]);
								c.sideOutput(unvalidatedData, row);
							}
						} else {
							LOG.debug("A faulty row was encountered: \t"
									+ c.element() + "\n");
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


		// Defining the BigQuery table scheme
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("datetime")
				.setType("TIMESTAMP").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("consumption")
				.setType("FLOAT").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("meterID").setType("STRING")
				.setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("meterName")
				.setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("sensorType")
				.setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("unit").setType("STRING")
				.setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("recordNumber")
				.setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("recordName")
				.setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("dwellingType")
				.setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("constructionYear")
				.setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("renovationYear")
				.setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("country").setType("STRING")
				.setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("postalCode")
				.setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("floorSurface")
				.setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("householdSize")
				.setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("heatingOn")
				.setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("cookingOn")
				.setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("hotWaterOn")
				.setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("energyPerformance")
				.setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("energyRating")
				.setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("category").setType("STRING")
				.setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("energyEfficiency")
				.setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("auxiliaryHeatingOn")
				.setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("installations")
				.setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("streetAddress")
				.setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("email").setType("STRING")
				.setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("businessName")
				.setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("fullName").setType("STRING")
				.setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("multiplier")
				.setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("readingType")
				.setType("STRING").setMode("REQUIRED"));
		TableSchema schema = new TableSchema().setFields(fields);

		/*
		 * Writing the TableRow object to BigQuery
		 * WriteDisposition.WRITE_APPEND: the data will be added to the existing
		 * table, already stored data remains
		 * CreateDisposition.CREATE_IF_NEEDED: if the table doesn't exist it
		 * will be created with the provided scheme
		 */
		parsedData.apply("BigQuery Write", 
				BigQueryIO.Write.to(destination)
					.withSchema(schema)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
					.withoutValidation());
		
//		PCollection<Entity> datastorePrep = parsedData.apply("Datastore preparation", ParDo.of(new DoFn<TableRow, Entity>(){
//			@ProcessElement
//			public void processElement(ProcessContext c) {
//				Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
//				String kind = "OverviewJouleboulevardTest";
//				String name = c.element().getF().get(2).getV().toString();
//				
//				
//				
//				c.output(ent);
//			}
//		}));
//		
//		datastorePrep.apply("Datastore update", DatastoreIO.v1().write().withProjectId("lkn-muntstraat"));

		
		// Defining the BigQuery table scheme
		List<TableFieldSchema> unparseableFields = new ArrayList<>();
		unparseableFields.add(new TableFieldSchema().setName("datetime").setType("TIMESTAMP").setMode("REQUIRED"));
		unparseableFields.add(new TableFieldSchema().setName("consumption").setType("FLOAT").setMode("REQUIRED"));
		unparseableFields.add(new TableFieldSchema().setName("meterID").setType("STRING").setMode("REQUIRED"));
		TableSchema unparseableSchema = new TableSchema().setFields(unparseableFields);
		
		unparseableData.apply("BigQuery Write unparseable data", 
				BigQueryIO.Write.to(destination + "_unparseable")
					.withSchema(unparseableSchema)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
					.withoutValidation());
		
		
		p.run();

	}

}


















