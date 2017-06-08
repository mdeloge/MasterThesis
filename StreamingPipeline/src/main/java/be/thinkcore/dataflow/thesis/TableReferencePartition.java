package be.thinkcore.dataflow.thesis;

import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/*
 * @Author Alex Van Boxel
 * @Source https://medium.com/google-cloud/bigquery-partitioning-with-beam-streams-97ec232a1fcc
 */

public class TableReferencePartition implements SerializableFunction<BoundedWindow, TableReference> {
	private final String projectId;
    private final String datasetId;
    private final String pattern;
    private final String table;

    public static TableReferencePartition perDay(String projectId, String datasetId, String tablePrefix) {
        return new TableReferencePartition(projectId, datasetId, "yyyyMMdd", tablePrefix + "$");
    }

    private TableReferencePartition(String projectId, String datasetId, String pattern, String table) {
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.pattern = pattern;
        this.table = table;
    }

    @Override
    public TableReference apply(BoundedWindow input) {
        DateTimeFormatter partition = DateTimeFormat.forPattern(pattern).withZoneUTC();

        TableReference reference = new TableReference();
        reference.setProjectId(this.projectId);
        reference.setDatasetId(this.datasetId);
        reference.setTableId(table + input.maxTimestamp().toString(partition));
        return reference;
    }
}
