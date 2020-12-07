package myhadoop.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import myhadoop.support.AirlinePerfomanceParser;

public class DelayCountMappe
	extends Mapper<LongWritable,
				   Text,
				   Text,
				   IntWritable>{

	// 사용자 지정 옵션 -D로 전달되는 workType (departure, arrival)
	private String workType;
	// 맵 출력 값
	private final static IntWritable outputValue = new IntWritable(1);
	// 맵 출력 키
	private Text outputkey = new Text();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// 매퍼가 처음 실행될 때 단 한번 호출
		// 옵션을 받아와서 설정
		workType = context.getConfiguration().get("workType");
	}

	@Override
	protected void map(LongWritable key, 
			Text value, 
			Context context)
			throws IOException, InterruptedException {
		if (key.get()==0 && value.toString().contains("YEAR")) {
			// 헤더이므로 중단
			return;
		}
		// cvs 파싱 (분석)
		AirlinePerfomanceParser parser = new AirlinePerfomanceParser(value);
		// 옵션 workType에 의한 매핑 작업 분기
		// 로직을 분리 
		if (workType.equals("departure")) {
			// 출발 지연시간을 검사
			if (parser.getDepartureDelayTime()>0) {
				// 출력 키 설정
				outputkey.set(parser.getYear()+" ," + parser.getMonth());
				// 출력
				context.write(outputkey, outputValue);
			}
		} else if (workType.equals("arrival")) {
			if (parser.getArriveDelayTime()>0) {
				outputkey.set(parser.getYear()+" ," + parser.getMonth());
				context.write(outputkey, outputValue);
			}
		}
	}
	
	// map 메서드 수행 이전에 옵션을 받아오기 위한 setup 메서드
	
}
