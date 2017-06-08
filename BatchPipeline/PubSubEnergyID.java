package release;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

import javax.xml.transform.TransformerFactoryConfigurationError;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
//import com.google.cloud.dataflow.sdk.transforms.Partition;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;



//import com.google.cloud.dataflow.sdk.values.PCollectionList;

public class PubSubEnergyID {

	private static final Logger LOG = LoggerFactory
			.getLogger(PubSubEnergyID.class);

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

	static InputStream openGcsFile(String bucketName, String objectId)
			throws IOException {
		try {
			Storage storage = StorageOptions.getDefaultInstance().getService();
			/*Storage storage = StorageOptions.newBuilder()
					.setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream("/home/matteus/Github/ThesisDataflow/service-account.json")))
					.build()
					.getService();*/
			BlobId blobId = BlobId.of(bucketName, objectId);
			Blob blob = storage.get(blobId);

			return Channels.newInputStream(blob.reader());
		} catch (StorageException e) {
			return null;
		}
	}
	
	private static List<String> getMetaData(String file){
		List<String> list = new ArrayList<String>();
		try{
			URL url = new URL(file);
			InputStream is = url.openStream();
			int temp;
			String text = "";
			while((temp = is.read())!=-1){
				text += (char)temp;
			}
			
			String[] meta = text.split("\n");
			for(int i = 0; i < meta.length; i++){
				String[] current = meta[i].split(";");
				if(!current[0].equals("MeterId")){
					list.add(meta[i]);
				}
				
			}			
		}catch (IllegalArgumentException | IOException
				| TransformerFactoryConfigurationError e) {
			System.out.println(e.toString());
		}
		
		return list;
	}

	public static void main(String[] args) throws IOException {

		/*
		 * VARIABLES "lkn-muntstraat:Jouleboulevard.EnergieID_15min"
		 * "gs://jouleboulevard2/Jouleboulevard_metadata.csv"
		 */
		// "gs://jouleboulevard2/energyiddev.blob.core.windows.net/jouleboulevard2/*/*/15min/*.csv"
		String sourceTopic = "projects/lkn-muntstraat/subscriptions/dataflow_subscription";
		String destination = "lkn-muntstraat:Jouleboulevard.EnergieID_pubsubtest";
		String sideSource = "gs://jouleboulevard2/sync_list/Jouleboulevard_metadata.csv";
		String unParseableDataDest = "lkn-muntstraat:Jouleboulevard.EnergieID_unParseable";

		MyOptions options = PipelineOptionsFactory.fromArgs(args)
				.withValidation().as(MyOptions.class);
		options.setStreaming(true);
		Pipeline p = Pipeline.create(options);

		
		
		List<String> metaLines = getMetaData("https://storage.googleapis.com/jouleboulevard2/sync_list/Jouleboulevard_metadata.csv");
		
		PCollection<String> metaParsed = p.apply(Create.of(metaLines));
		

		PCollection<String> line = p.apply(PubsubIO.Read.named("PubSubStreamRead").subscription(sourceTopic));

		//
		// PCollection<String> metaLine =
		// p.apply(TextIO.Read.named("FetchMetadata")
		// .from(sideSource));
		//
		// @SuppressWarnings("serial")
		// PCollection<String[]> metaParsed = metaLine.apply(ParDo.named(
		// "ParsingMetaData").of(new DoFn<String, String[]>() {
		// @Override
		// public void processElement(ProcessContext c) {
		// String[] meta = c.element().split(";");
		// if (meta.length == 28 && !meta[0].equals("MeterId")) {
		// c.output(meta);
		// }
		// }
		// }));
		//
		final PCollectionView<List<String>> metaList = metaParsed.apply(View.<String>asList());

		@SuppressWarnings("serial")
		PCollection<TableRow> parsedData = line.apply(ParDo
				.named("ParsingCombinedData").withSideInputs(metaList)
				.of(new DoFn<String, TableRow>() {
					@Override
					public void processElement(ProcessContext c) {
						String[] measuredData = c.element().split(",");
						if (checkSplitter(measuredData, 3)) {
							boolean parseable = false;
							for (int i = 0; i < c.sideInput(metaList).size(); i++) {
								String[] sideData = c.sideInput(metaList)
										.get(i).split(";");
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
								// TODO add sideoutput here
							}
						} else {
							LOG.debug("A faulty row was encountered: \t"
									+ c.element() + "\n");
						}
					}

					// Validates the processed String element before an attempt
					// will be made to create a TableRow object
					private boolean checkSplitter(String[] split,
							int wantedLength) {
						if (split.length == wantedLength) {
							for (int i = 0; i < wantedLength; i++) {
								if (split[i].equals("")
										|| split[i].equals("MeterId")
										|| split[i].equals("consumption")) {
									return false;
								}
							}
							return true;
						} else {
							return false;
						}
					}
				}));

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
		parsedData.apply(BigQueryIO.Write
				.named("BigQueryWrite")
				.to(destination)
				.withSchema(schema)
				.withWriteDisposition(
						BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(
						BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withoutValidation());

		// Runs the pipeline
		p.run();
	}
}
