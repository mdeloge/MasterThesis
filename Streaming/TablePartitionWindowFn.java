package be.thinkcore.dataflow.thesis;

import java.util.Arrays;
import java.util.Collection;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn.AssignContext;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.api.services.bigquery.model.TableRow;

//http://stackoverflow.com/questions/38114306/creating-writing-to-parititoned-bigquery-table-via-google-cloud-dataflow/40863609#40863609

public class TablePartitionWindowFn extends NonMergingWindowFn<Object, IntervalWindow> {

	private IntervalWindow assignWindow(AssignContext context) {
		TableRow source = (TableRow) context.element();
		String dttm_str = (String) source.get("DTTM");

		DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC();

		Instant start_point = Instant.parse(dttm_str,formatter);
		Instant end_point = start_point.withDurationAdded(1000, 1);

		return new IntervalWindow(start_point, end_point);
	};

	@Override
	public Coder<IntervalWindow> windowCoder() {
		return IntervalWindow.getCoder();
	}

	@Override
	public Collection<IntervalWindow> assignWindows(AssignContext c) throws Exception {
		return Arrays.asList(assignWindow(c));
	}

	@Override
	public boolean isCompatible(WindowFn<?, ?> other) {
		return false;
	}

	@Override
	public IntervalWindow getSideInputWindow(BoundedWindow window) {
		if (window instanceof GlobalWindow) {
			throw new IllegalArgumentException(
					"Attempted to get side input window for GlobalWindow from non-global WindowFn");
		}
		return null;
	}	
}