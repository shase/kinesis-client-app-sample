import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;


public class Main {

	public static void main(String... args) throws UnknownHostException {
		String applicationName = "kinesis-apps";
		String streamName = "develop-1";
		String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();

		AWSCredentialsProvider credentialsProvider = new ClasspathPropertiesFileCredentialsProvider();
		credentialsProvider.getCredentials();
		
		KinesisClientLibConfiguration kinesisClientLibConfiguration = new KinesisClientLibConfiguration(applicationName,
				streamName, credentialsProvider, workerId);

		kinesisClientLibConfiguration.withRegionName("ap-northeast-1");

		IRecordProcessorFactory recordProcessorFactory = new Factory();

		Worker worker = new Worker.Builder()
			    .recordProcessorFactory(recordProcessorFactory)
			    .config(kinesisClientLibConfiguration)
			    .metricsFactory(new NullMetricsFactory())
			    .build();
		
		worker.run();
	}
}