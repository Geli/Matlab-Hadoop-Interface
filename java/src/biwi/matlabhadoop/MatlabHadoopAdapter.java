package biwi.matlabhadoop;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MatlabHadoopAdapter {
	
	public static class MatlabMapper extends
			Mapper<IntWritable, BytesWritable, IntWritable, BytesWritable> {
		
		
		
		Process MatlabProc=null;
		File fifofile=null;
		File InPipe=null;
		File OutPipe=null;
		FileOutputStream outStream=null;
		FileInputStream inStream=null;
		DataInputStream dinStream=null;
		DataOutputStream doutStream=null;
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf=context.getConfiguration();

			fifofile=File.createTempFile("Matlab", ".fifo");
								
			InPipe=new File(fifofile.getAbsolutePath()+".in");
			OutPipe=new File(fifofile.getAbsolutePath()+".out");
			ProcessBuilder MatlabProcBuilder=new ProcessBuilder(conf.get("MatlabHadoopAdapter.MatlabMapper.MapperCommand"));
			Map<String,String> env=MatlabProcBuilder.environment();
			env.put("MATLAB_MAPRED_INFIFO", InPipe.getAbsolutePath()); // matlab reads from this pipe so we write
			env.put("MATLAB_MAPRED_OUTFIFO", OutPipe.getAbsolutePath());
			env.put("LD_LIBRARY_PATH", conf.get("MatlabHadoopAdapter.MatlabMapper.MapperLD_LIBRARY_PATH"));
			
			
			Runtime.getRuntime().exec(String.format("mkfifo %s", InPipe.getAbsolutePath())).waitFor();
			Runtime.getRuntime().exec(String.format("mkfifo %s", OutPipe.getAbsolutePath())).waitFor();
			
			MatlabProc = MatlabProcBuilder.start();
			
			outStream= new FileOutputStream(InPipe);
			doutStream=new DataOutputStream(outStream); // we write to this

			inStream= new FileInputStream(OutPipe);
			dinStream=new DataInputStream(inStream); // we read from this
			
		}
		
		
		
		

		@Override
		protected void map(IntWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			doutStream.writeInt(key.get());
			doutStream.writeInt(value.getLength());
			doutStream.write(value.getBytes(),0,value.getLength());
			//System.out.format("Processing key %d with length %d\n", key.get(),value.getLength());
			int outKey=dinStream.readInt();
			int outDataLen=dinStream.readInt();
			//System.out.format("Recieving key %d with length %d\n", outKey,outDataLen);
			byte[] outData=new byte[outDataLen];
			dinStream.readFully(outData);
			
			context.write(new IntWritable(outKey), new BytesWritable(outData));
			
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			doutStream.close();
			dinStream.close();
			inStream.close();
			outStream.close();
			MatlabProc.waitFor();
			InPipe.delete();
			OutPipe.delete();
			fifofile.delete();
			super.cleanup(context);
		}

	}
	// Deletes all files and subdirectories under dir. 
	// Returns true if all deletions were successful.
	// If a deletion fails, the method stops attempting to delete and returns false. 
	public static boolean deleteDir(File dir) { 
		if (dir.isDirectory()) { 
			String[] children = dir.list(); 
			for (int i=0; i<children.length; i++) { 
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) { 
					return false; 
				}
			}
		}
		// The directory is now empty so delete it 
		return dir.delete();
	} 

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length<2){
			return;
		}
	
		String Job,LD_LIBRARY_PATH,infile,outfile;
		try{
			Job=otherArgs[0];
			LD_LIBRARY_PATH=otherArgs[1];
			infile=otherArgs[2];
			outfile=otherArgs[3];
		}catch(Exception e){
			System.out.println("Usage: Matlab MatlabCommand LD_LIBRARY_PATH infile outfile");
			return;								
		}
		conf.set("MatlabHadoopAdapter.MatlabMapper.MapperCommand",Job);
		conf.set("MatlabHadoopAdapter.MatlabMapper.MapperLD_LIBRARY_PATH",LD_LIBRARY_PATH);
		
		conf.set("mapred.job.tracker","local");
		deleteDir(new File(outfile));
		
		Job job = new Job(conf, String.format("Matlab: %s",Job));
		
		job.setJarByClass(MatlabHadoopAdapter.class);
		job.setMapperClass(MatlabMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(infile));
		FileOutputFormat.setOutputPath(job, new Path(outfile));

		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}


}
