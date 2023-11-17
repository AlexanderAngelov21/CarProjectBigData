package uni.pu.fmi.car.project.bigdata;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

public class CarAverageReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, Text> {
	private JobConf conf;
	@Override
	public void configure(JobConf job) {
		super.configure(job);
		this.conf = job;
	}

	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		double totalMpg = 0;
		int count = 0;
		int count2=0;
		while (values.hasNext()) {
			double mpg = Double.parseDouble(values.next().toString());
			totalMpg += mpg;
			count++;
			count2++;
		}
		if (count > 0) {
			double averageMpg = totalMpg / count;
			output.collect(key, new Text(String.valueOf(averageMpg +" " +count2)));
			
		}
	}
}