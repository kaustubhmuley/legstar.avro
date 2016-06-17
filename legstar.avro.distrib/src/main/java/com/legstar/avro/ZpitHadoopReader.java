package com.legstar.avro.samples.zpitonly;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.legstar.avro.cob2avro.hadoop.mapreduce.Cob2AvroJob;
import com.legstar.avro.cob2avro.hadoop.mapreduce.ZosRdwAvroInputFormat;
import com.legstar.base.context.EbcdicCobolContext;

public class ZpitHadoopReader extends Configured implements Tool {
	

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ZpitHadoopReader(), args);
        System.exit(res);
    }
    
    public int run(String[] args) throws Exception {

    	/*final String AVRO_SHEMA_PATH = "gen/avsc/zpitonly.avsc";
    	Schema schema = new Schema.Parser().parse(new File(AVRO_SHEMA_PATH));*/
    	
        // Create configuration
        Configuration conf = this.getConf();

        // Create job
        Job job = Job.getInstance(conf);
        job.setJobName("recordsPerZpitRec");
        job.setJarByClass(ZpitHadoopReader.class);

        // Setup MapReduce classes
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Set only 1 reduce task
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(ZosRdwAvroInputFormat.class);
        Cob2AvroJob.setInputKeyCobolContext(job, EbcdicCobolContext.class);
        Cob2AvroJob.setInputKeyRecordType(job, CobolZpitRec.class);
        Cob2AvroJob.setInputRecordMatcher(job, ZpitZosRdwRecordMatcher.class);
        
        AvroJob.setInputKeySchema(job, ZpitRec.getClassSchema());
        AvroJob.setMapOutputKeySchema(job, ZpitRec.getClassSchema());
        AvroJob.setOutputKeySchema(job, ZpitRec.getClassSchema());
        
    
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
//        job.setMapOutputKeyClass(ZpitRec.class);
//        job.setMapOutputValueClass(IntWritable.class);
//        
//        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(ZpitRec.class);
        job.setOutputValueClass(NullWritable.class);
       
      

        // Execute job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MyMapper extends
            Mapper < AvroKey < ZpitRec >, NullWritable, AvroKey < ZpitRec >, NullWritable > {

        public void map(AvroKey < ZpitRec > key, NullWritable value,
                Context context) throws IOException, InterruptedException {
          
            context.write(key, value);
        }
    }

    public static class MyReducer extends
            Reducer < AvroKey < ZpitRec >, NullWritable,  AvroKey < ZpitRec >, NullWritable > {

    	 File avroData=new File("/home/swapnil/zpitonlyData5.avro");
    	 DatumWriter<ZpitRec> zpitDatumWriter = new SpecificDatumWriter<ZpitRec>();
   	  	 DataFileWriter<ZpitRec> zpitFileWriter = new DataFileWriter<ZpitRec>(zpitDatumWriter);
   	 
    	 
        public void reduce(AvroKey < ZpitRec > key, Iterable < NullWritable > values,
                Context context) throws IOException, InterruptedException {
        
        	
        	//zpitFileWriter.create(key.datum().getSchema(), avroData);
        	//zpitFileWriter.append(key.datum());
        	
        	context.write(key, null);
        }
    }

}
