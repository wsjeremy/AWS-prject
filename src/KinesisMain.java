
public class KinesisMain {

	public static void main(String[] args) {
		AmazonKinesisRecordProducerSample producer=new AmazonKinesisRecordProducerSample();
		if (producer.run()){
			try {
				System.out.println("sleep for consumer");
				Thread.sleep(10000);
				System.out.println("start consumer");
				FetchDataFromKinesis consumer=new FetchDataFromKinesis();
				consumer.run();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
