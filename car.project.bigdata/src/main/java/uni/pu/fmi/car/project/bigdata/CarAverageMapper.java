package uni.pu.fmi.car.project.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CarAverageMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
	private org.apache.hadoop.mapred.JobConf configuration;

	@Override
	public void configure(org.apache.hadoop.mapred.JobConf job) {
		super.configure(job);
		this.configuration = job;
	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		String[] columns = line.split(";");
		if (columns.length >= 10) {
			String make = columns[0];

			double mpg = 0;

			try {
				mpg = Double.parseDouble(columns[2]);
			} catch (NumberFormatException e) {
				System.err.println(value.toString());
			}

			String resultType = this.configuration.get("option", "Average Fuel Economy");
			String brandFilter = this.configuration.get("brand", "");

			if (resultType.equals("Average Fuel Economy")) {
				if (brandFilter.isEmpty() || make.toLowerCase().contains(brandFilter.toLowerCase())) {
					output.collect(new Text(make), new DoubleWritable(mpg));
				}
			}
		}
	}
}