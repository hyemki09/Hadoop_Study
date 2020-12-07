package myhadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

import myhadoop.mappers.WordCountMapper;
import myhadoop.reducer.WordCountReducer;

// MapReduce Driver 클래스
/*
 * 1. 잡 객체를 생성
 * 2. 잡 객체에 맵 리듀스 실행 정보 설정
 * 3. 맵 리듀스 작업 수행
 */
public class WordCount {

	public static void main(String[] args) throws Exception {
		// 파라미터 체크 , 부족하면 조욜
		if (args.length != 2) {
			System.err.println("Usage: WordCount <imput> <output>");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"WordCount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		// 입출력 포맷 등록
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 입출력 경로를 등록
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}

}
