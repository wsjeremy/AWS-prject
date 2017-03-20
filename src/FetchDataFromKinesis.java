import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

public class FetchDataFromKinesis {

	private AmazonKinesisClient kinesis;

    private void init() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (/Users/xujinpeng1/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/Users/xujinpeng1/.aws/credentials), and is in valid format.",
                    e);
        }

        kinesis = new AmazonKinesisClient(credentials);
    }
    
    private void fetchData(){
    	FileOutputStream output=null;
		try {
			init();
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			final String myStreamName = AmazonKinesisApplicationSample.SAMPLE_APPLICATION_STREAM_NAME;
	        DescribeStreamRequest getData=new DescribeStreamRequest().withStreamName(myStreamName);
	        DescribeStreamResult result=kinesis.describeStream(getData);
	        List<Shard> shards=result.getStreamDescription().getShards();
	        if (!shards.isEmpty()){
	        	Shard t=shards.get(0);
	        	String shardId=t.getShardId();
	        	String snum=t.getSequenceNumberRange().getStartingSequenceNumber();
	        	GetShardIteratorRequest gs=new GetShardIteratorRequest().withStreamName(myStreamName).withStartingSequenceNumber(snum).withShardId(shardId).withShardIteratorType("AT_SEQUENCE_NUMBER");
	        	GetShardIteratorResult sit=kinesis.getShardIterator(gs);
	        	String nextShardIteratorStr=sit.getShardIterator();
	        	output=new FileOutputStream("testdate.txt");
	        	while(true){
	        		GetRecordsRequest grs=new GetRecordsRequest().withLimit(10).withShardIterator(nextShardIteratorStr);
		        	GetRecordsResult grr=kinesis.getRecords(grs);
		        	List<Record> records=grr.getRecords();
		        	if (records==null||records.isEmpty()){break;}
		        	if (records!=null){
		        		for (Record r:records){
		        			String id=UUID.randomUUID().toString().replaceAll("-", "");
		        			output.write(String.format("{\"id\":\"%s\",  \"timestamp\":\"%s\", \"content\":\"%s\"}\n",id,dateFormat.format(r.getApproximateArrivalTimestamp()), new String(r.getData().array(),"UTF-8")).getBytes());
		        		}
		        	}
		        	nextShardIteratorStr=grr.getNextShardIterator();
	        	}
	        }
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if (output!=null){
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
    }
    
    public boolean run(){
    	try {
			init();
			fetchData();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
    }
    
	public static void main(String[] args) {
		try {
			FetchDataFromKinesis test=new FetchDataFromKinesis();
			test.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
