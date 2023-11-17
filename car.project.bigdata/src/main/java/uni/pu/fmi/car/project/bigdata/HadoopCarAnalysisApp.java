package uni.pu.fmi.car.project.bigdata;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.net.URI;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class HadoopCarAnalysisApp extends JFrame {

	private JComboBox<String> optiontDropdown;
	private JTextField brandSearchField;
	private JTextField minHorsepowerField;
	private JTextField maxHorsepowerField;
	private JTextField minMpgField;
	private JButton searchButton;

	public HadoopCarAnalysisApp() {
		setTitle("Car Analysis");
		setSize(400, 300);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		JPanel panel = new JPanel();
		panel.setLayout(null);

		JLabel resultLabel = new JLabel("Option:");
		resultLabel.setBounds(20, 20, 100, 20);
		panel.add(resultLabel);

		String[] resultOptions = { "Average Fuel Economy", "Car List" };
		optiontDropdown = new JComboBox<>(resultOptions);
		optiontDropdown.setBounds(120, 20, 150, 20);
		panel.add(optiontDropdown);

		JLabel brandLabel = new JLabel("Car search by brand:");
		brandLabel.setBounds(20, 50, 150, 20);
		panel.add(brandLabel);

		brandSearchField = new JTextField();
		brandSearchField.setBounds(190, 50, 150, 20);
		panel.add(brandSearchField);

		JLabel horsepowerLabel = new JLabel("By horsepower:");
		horsepowerLabel.setBounds(20, 80, 150, 20);
		panel.add(horsepowerLabel);

		minHorsepowerField = new JTextField();
		minHorsepowerField.setBounds(190, 80, 60, 20);
		panel.add(minHorsepowerField);

		JLabel toLabel = new JLabel("to");
		toLabel.setBounds(260, 80, 20, 20);
		panel.add(toLabel);

		maxHorsepowerField = new JTextField();
		maxHorsepowerField.setBounds(280, 80, 60, 20);
		panel.add(maxHorsepowerField);

		JLabel mpgLabel = new JLabel("By MPG:");
		mpgLabel.setBounds(20, 110, 150, 20);
		panel.add(mpgLabel);

		minMpgField = new JTextField();
		minMpgField.setBounds(190, 110, 150, 20);
		panel.add(minMpgField);

		searchButton = new JButton("Search");
		searchButton.setBounds(120, 140, 100, 30);
		panel.add(searchButton);
		setLocationRelativeTo(null);
		add(panel);
	}

	public void init() {
		minHorsepowerField.setEnabled(false);
	    maxHorsepowerField.setEnabled(false);
	    minMpgField.setEnabled(false);
		optiontDropdown.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				String selectedResult = (String) optiontDropdown.getSelectedItem();
				if (selectedResult.equals("Average Fuel Economy")) {
					minHorsepowerField.setEnabled(false);
					maxHorsepowerField.setEnabled(false);
					minMpgField.setEnabled(false);
				} else  {
					minHorsepowerField.setEnabled(true);
					maxHorsepowerField.setEnabled(true);
					minMpgField.setEnabled(true);
				}
				
			}
		});

		searchButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				String selectedOption = (String) optiontDropdown.getSelectedItem();
				String brand = brandSearchField.getText();
				String minHorsepower = minHorsepowerField.getText();
				String maxHorsepower = maxHorsepowerField.getText();
				String minMpg = minMpgField.getText();

				runHadoopJob(selectedOption, brand, minHorsepower, maxHorsepower, minMpg);
			}
		});
	}

	private void runHadoopJob(String option, String brand, String minHp, String maxHp, String minMpg) {
		Configuration conf = new Configuration();
		JobConf job = new JobConf(conf, HadoopCarAnalysisApp.class);
		job.setJobName("CarAnalysis");
		job.set("option", option);
		job.set("brand", brand);
		job.set("minHp", minHp);
		job.set("maxHp", maxHp);
		job.set("minMpg", minMpg);

		if (option.equals("Average Fuel Economy")) {
			job.setMapperClass(CarAverageMapper.class);
			job.setReducerClass(CarAverageReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
		} else {
			job.setMapperClass(CarListMapper.class);
			job.setReducerClass(CarListReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
		}

		Path input = new Path("hdfs://127.0.0.1:9000/input/cars.csv");
		Path output = new Path("hdfs://127.0.0.1:9000/output/car_analysis");

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		try {
			FileSystem fs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);
			if (fs.exists(output)) {
				fs.delete(output, true);
			}
			JobClient.runJob(job);
			JOptionPane.showMessageDialog(null, "Hadoop job executed successfully.", "Job Status",
					JOptionPane.INFORMATION_MESSAGE);
			System.out.println("Hadoop job executed successfully.");
		} catch (IOException ex) {
			ex.printStackTrace();
			JOptionPane.showMessageDialog(null, "Hadoop job execution failed: " + ex.getMessage(), "Job Status",
					JOptionPane.ERROR_MESSAGE);
		}
	}

	public static void main(String[] args) {
		HadoopCarAnalysisApp app = new HadoopCarAnalysisApp();
		app.init();

		app.setVisible(true);
	}
}
