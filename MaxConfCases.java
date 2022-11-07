import java.io.IOException;
// import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxConfCases {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // gets input from confcases reducer output: eg 2021-Hong Kong 29054 (hdfs file
      // /iseq1)

      String year = "2022";
      String countryCount = "Aditya:1";
      try {
        String line = value.toString();
        // System.out.println("line:" + line);
        String yearCountry = line.split("\t")[0];
        // System.out.println("yearCountry:" + yearCountry);
        if (yearCountry.split("-").length > 1) {
          year = yearCountry.split("-")[0];
          // System.out.println("year:" + year);
          String country = yearCountry.split("-")[1];
          // System.out.println("Country1:" + country);
          String count = line.split("\t")[1];
          // System.out.println("Count1:" + count);
          countryCount = country + ":" + count;
        }
        // System.out.println("year:Country:Count (Mapper) <->" + year + " : " +
        // countryCount);
        context.write(new Text(year), new Text(countryCount));
      } catch (ArrayIndexOutOfBoundsException e) {
        // ArrayIndexOutOfBoundsException
        System.out.println("Error1: " + e);
      }
    }
  }

  public static class TxtConfReducer extends Reducer<Text, Text, Text, Text> {
    private Text mCC = new Text();

    public void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
      int maxCount = 0;
      String maxCountryCount = "";

      for (Text val : values) {
        String countryCount = val.toString();
        // System.out.println("CountryCount<->" + countryCount);
        if (countryCount.split(":").length > 1) {
          String country = countryCount.split(":")[0];
          System.out.println("Country<->" + country);
          int count = Integer.parseInt(countryCount.split(":")[1]);
          System.out.println("Count<->" + Integer.toString(count));
          if (count > maxCount) {
            maxCount = count;
            maxCountryCount = country + "-" + Integer.toString(maxCount);
          }
        }
      }
      mCC.set(maxCountryCount);
      System.out.println(key.toString() + "<Reducer>" + mCC.toString());
      System.out.println("MaxCountryCount:" + mCC.toString());
      context.write(key, mCC);

    }

  }

  // Run using hadoop jar MaxConfCases.jar /iseq1 /iseq3
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MaxConfCases");
    job.setJarByClass(MaxConfCases.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(TxtConfReducer.class); <- This line caused context.write to not write output value of reducer
    job.setReducerClass(TxtConfReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setJar("MaxConfCases.jar");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}