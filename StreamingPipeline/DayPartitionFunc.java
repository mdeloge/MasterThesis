package be.thinkcore.dataflow.thesis;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
//http://stackoverflow.com/questions/38114306/creating-writing-to-parititoned-bigquery-table-via-google-cloud-dataflow/40863609#40863609

public class DayPartitionFunc implements SerializableFunction<BoundedWindow, String> {

	String destination = "";

	public DayPartitionFunc(String dest) {
		this.destination = dest + "$";
	}

	@Override
	public String apply(BoundedWindow boundedWindow) {
		// The cast below is safe because CalendarWindows.days(1) produces IntervalWindows.
		String dayString = DateTimeFormat.forPattern("yyyyMMdd")
				.withZone(DateTimeZone.UTC)
				.print(((IntervalWindow) boundedWindow).start());
		return destination + dayString;
	}
}
