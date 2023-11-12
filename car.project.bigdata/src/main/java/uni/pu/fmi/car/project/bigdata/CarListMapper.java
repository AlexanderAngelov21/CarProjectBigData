package uni.pu.fmi.car.project.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CarListMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	private org.apache.hadoop.mapred.JobConf configuration;
	private int lineCounter = 0;
	@Override
	public void configure(org.apache.hadoop.mapred.JobConf job) {
		super.configure(job);
		this.configuration = job;
	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		lineCounter++;
        if (lineCounter <= 2) {// da skipne parvite 2 reda - ime i tip na danni
            return;
        }
		String line = value.toString();
		String[] columns = line.split(";");
		if (columns.length >= 10) {
			String make = columns[0];

			double horsepower = 0;
			double mpg = 0;

			try {
				horsepower = Double.parseDouble(columns[5]);
				mpg = Double.parseDouble(columns[2]);
			} catch (NumberFormatException e) {
				System.err.println(value.toString());
			}
			String resultType = this.configuration.get("option", "Car List");
			String brandFilter = this.configuration.get("brand", "");
			String[] brands = brandFilter.replaceAll("\\s+", " ").trim().split(" ");
			double minHorsepower = Double.parseDouble(this.configuration.get("minHp", "-1000"));
			double maxHorsepower = Double.parseDouble(this.configuration.get("maxHp", "999999"));
			double minMpg = Double.parseDouble(this.configuration.get("minMpg", "-1000"));
			if (resultType.equals("Car List")) {
				 boolean brandMatch = false;
		            for (String brand : brands) {
		                if (make.toLowerCase().contains(brand.toLowerCase())) {
		                    brandMatch = true;
		                    break;
		                }
		            }
			if ((brandMatch ||(brandFilter.isEmpty() || make.toLowerCase().contains(brandFilter.toLowerCase())))
					&& (horsepower >= minHorsepower && horsepower <= maxHorsepower) && (mpg > minMpg)) {
                String printMPG=String.valueOf(mpg);
                String printHP=String.valueOf(horsepower);
                if(horsepower<=0) {
                	printHP="Invalid horsepower - [Horsepower is less than or equal to 0]";
                }
                if(mpg<=0) {
                	printMPG="Invalid MPG - [MPG is less than or equal to 0]";
                }
				Text outputKey = new Text(make);
				Text outputValue = new Text(printHP + " " + printMPG);
				output.collect(outputKey, outputValue);
			}
			}
		}
	}
}