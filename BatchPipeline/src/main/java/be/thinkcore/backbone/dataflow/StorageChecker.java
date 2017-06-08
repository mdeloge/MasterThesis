package be.thinkcore.backbone.dataflow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;

public class StorageChecker {
	
	/*
	 * This is very wrong, implement it better
	 */
	
	private String [] correct = {"1E7F4E9F-28F8-4A09-9046-4A2F60E529EE"
			,"246C689E-3884-4BFB-B398-B0C96136BC4F"
			,"2BFC0700-E5EE-42DE-B7D9-391E3711BEBD"
			,"3F3A5DC5-1DC5-4725-91DC-9330056670D2"};
	
	public StorageChecker(){
		
	}
	
	/*
	 * variable 'bucket' must specify storage bucket and subfolder up until the meterID
	 * e.g.: "gs://jouleboulevard-azure/energyiddev.blob.core.windows.net/jouleboulevard/EA-14105155/1E7F4E9F-28F8-4A09-9046-4A2F60E529EE/
	 * Note the slash at the end!
	 */
	public boolean check(String bucket){
		return false;
	}
	
	public boolean checkTemp(String id){
		for(int i = 0; i < correct.length; i++){
			if(correct[i].equals(id)){
				return true;
			}
		}
		return false;
	}
}
