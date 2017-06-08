package be.thinkcore.backbone.dataflow;

import com.google.api.services.bigquery.model.TableRow;

public class EnergyIDRow {
	private String meterID, recordID, type, displayName, dataStreamProvider, dataStreamSensor, dataStreamToken;
	private boolean validRow;
	
	public EnergyIDRow(String row){
		String delimiter = ",";
		String[] tokens = row.split(delimiter);				

		if(tokens.length == 7 && !tokens[0].equals("") && !tokens[1].equals("") 
				&& !tokens[2].equals("") && !tokens[3].equals("") && !tokens[4].equals("") 
				&& !tokens[5].equals("") && !tokens[6].equals("") ){
			
			meterID = tokens[0];
			recordID = tokens[1];
			type = tokens[2];
			displayName = tokens[3];
			dataStreamProvider = tokens[4];
			dataStreamSensor = tokens[5];
			dataStreamToken = tokens[6];
			validRow = true;
		} else {
			System.out.println("faulty row");
			validRow = false;
		}
	}

	public String getMeterID() {
		return meterID;
	}

	public String getRecordID() {
		return recordID;
	}

	public String getType() {
		return type;
	}

	public String getDisplayName() {
		return displayName;
	}

	public String getDataStreamProvider() {
		return dataStreamProvider;
	}

	public String getDataStreamSensor() {
		return dataStreamSensor;
	}

	public String getDataStreamToken() {
		return dataStreamToken;
	}

	public boolean isValidRow() {
		return validRow;
	}
}
