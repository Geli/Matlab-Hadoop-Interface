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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MatlabHadoopAdapter {
	
	public static class MatlabConnection{
		Process MatlabProc=null;
		File fifofile=null;
		File InPipe=null;
		File OutPipe=null;
		FileOutputStream outStream=null;
		FileInputStream inStream=null;
		DataInputStream dinStream=null;
		DataOutputStream doutStream=null;
		
		class ShutdownHook extends Thread{
			public void run(){
				if (InPipe != null){
					try{
						InPipe.delete();
					} catch(Exception e){
						e.printStackTrace();
					}
				}
				if (OutPipe != null){
					try{
						OutPipe.delete();
					} catch(Exception e){
						e.printStackTrace();
					}
				}
				if (fifofile != null){
					try{
						fifofile.delete();
					} catch(Exception e){
						e.printStackTrace();
					}
				}
			}
		}
		
		ShutdownHook myShutdownHook=null;
		
		protected void create_connection(Configuration conf) throws IOException, InterruptedException{
			fifofile=File.createTempFile("Matlab", ".fifo");
			
			InPipe=new File(fifofile.getAbsolutePath()+".in");
			OutPipe=new File(fifofile.getAbsolutePath()+".out");
			ProcessBuilder MatlabProcBuilder=new ProcessBuilder(conf.get("MatlabHadoopAdapter.MatlabMapper.MapperCommand"));
			Map<String,String> env=MatlabProcBuilder.environment();
			env.put("MATLAB_MAPRED_INFIFO", InPipe.getAbsolutePath());
			env.put("MATLAB_MAPRED_OUTFIFO", OutPipe.getAbsolutePath());
			env.put("LD_LIBRARY_PATH", conf.get("MatlabHadoopAdapter.MatlabMapper.MapperLD_LIBRARY_PATH"));
			
			Runtime.getRuntime().exec(String.format("mkfifo %s", InPipe.getAbsolutePath())).waitFor();
			Runtime.getRuntime().exec(String.format("mkfifo %s", OutPipe.getAbsolutePath())).waitFor();
			
			myShutdownHook = new ShutdownHook();
			Runtime.getRuntime().addShutdownHook(myShutdownHook);
			
			//MatlabProc = MatlabProcBuilder.start();
			
			outStream= new FileOutputStream(InPipe);
			doutStream=new DataOutputStream(outStream); // we write to this
			inStream= new FileInputStream(OutPipe);
			dinStream=new DataInputStream(inStream); // we read from this
		}
		
		
		
		protected void cleanup_connection() throws IOException,
			InterruptedException{
			doutStream.close();
			dinStream.close();
			inStream.close();
			outStream.close();
			//MatlabProc.waitFor();
			
			
			InPipe.delete();
			OutPipe.delete();
			fifofile.delete();

			InPipe=null;
			OutPipe=null;
			fifofile=null;
		
			Runtime.getRuntime().removeShutdownHook(myShutdownHook);
		}
	}
	
	public static class MatlabReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
		MatlabConnection mc=null;
		
		KEYOUT ko=null;
		VALUEOUT vo=null;


		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf=context.getConfiguration();
			
			try {
				ko=(KEYOUT)context.getConfiguration().getClass("mapred.output.key.class",org.apache.hadoop.io.Text.class).newInstance();
				vo=(VALUEOUT)context.getConfiguration().getClass("mapred.output.value.class",org.apache.hadoop.io.Text.class).newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			mc=new MatlabConnection();
			mc.create_connection(conf);
				
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mc.cleanup_connection();
			super.cleanup(context);
		}
		
		@Override
		protected void reduce(KEYIN key, Iterable<VALUEIN> values,
				Context context) throws IOException, InterruptedException {

			((Writable) key).write(mc.doutStream);
			for (VALUEIN value : values) {
				mc.doutStream.writeByte(1);
				((Writable) value).write(mc.doutStream);
			}
			mc.doutStream.writeByte(0);
			
			while (mc.dinStream.readByte() != 0){
				((Writable)ko).readFields(mc.dinStream);
				((Writable)vo).readFields(mc.dinStream);
				context.write(ko, vo);
			}
		}

	}

	public static class MatlabMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends
			//Mapper<IntWritable, BytesWritable, IntWritable, BytesWritable> {
			Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
		
		MatlabConnection mc=null;
		KEYOUT ko=null;
		VALUEOUT vo=null;


		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf=context.getConfiguration();
			
			try {
				ko=(KEYOUT)context.getConfiguration().getClass("mapred.output.key.class",org.apache.hadoop.io.Text.class).newInstance();
				vo=(VALUEOUT)context.getConfiguration().getClass("mapred.output.value.class",org.apache.hadoop.io.Text.class).newInstance();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			mc=new MatlabConnection();
			mc.create_connection(conf);
		}
		
		
		@Override
		protected void map(KEYIN key, VALUEIN value, Context context)
				throws IOException, InterruptedException {
			((Writable) key).write(mc.doutStream);
			((Writable) value).write(mc.doutStream);
			mc.doutStream.flush();
			while (mc.dinStream.readByte()!=0){
				((Writable)ko).readFields(mc.dinStream);
				((Writable)vo).readFields(mc.dinStream);
				context.write(ko, vo);
			}
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mc.cleanup_connection();
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

	@SuppressWarnings("unchecked")
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
		job.setReducerClass(MatlabReducer.class);
		job.setCombinerClass(MatlabReducer.class);
		
		job.setInputFormatClass( (Class<? extends org.apache.hadoop.mapreduce.InputFormat<?,?>>) Class.forName("org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat"));
		job.setOutputFormatClass( (Class<? extends org.apache.hadoop.mapreduce.OutputFormat<?,?>>) Class.forName("org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"));
		
		job.setMapOutputKeyClass(Class.forName("org.apache.hadoop.io.IntWritable"));
		job.setMapOutputValueClass(Class.forName("org.apache.hadoop.io.BytesWritable"));
		job.setOutputKeyClass(Class.forName("org.apache.hadoop.io.IntWritable"));
		job.setOutputValueClass(Class.forName("org.apache.hadoop.io.BytesWritable"));
		
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(infile));
		FileOutputFormat.setOutputPath(job, new Path(outfile));

		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}


}

