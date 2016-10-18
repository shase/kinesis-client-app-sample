import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

public class Processor implements IRecordProcessor{
	
	private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
	private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
	private long nextCheckpointTimeInMillis = 0L;
	private InitializationInput initializationInput;

	@Override
	public void initialize(InitializationInput initializationInput) {
		this.initializationInput = initializationInput;		
	}

	@Override
	public void processRecords(ProcessRecordsInput processRecordsInput) {
		String data = null;
		for (Record record : processRecordsInput.getRecords()) {
			try {
				data = decoder.decode(record.getData()).toString();
			} catch (CharacterCodingException e) {
				continue;
			}
			System.out.println("sample:" + data);
		}
		long currentTime = System.currentTimeMillis();
		if (currentTime > nextCheckpointTimeInMillis) {
			checkpoint(processRecordsInput.getCheckpointer());
			nextCheckpointTimeInMillis = currentTime + CHECKPOINT_INTERVAL_MILLIS;
		}		
	}

	@Override
	public void shutdown(ShutdownInput shutdownInput) {
		if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
			checkpoint(shutdownInput.getCheckpointer());
		}		
	}
	
	private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
		try {
			checkpointer.checkpoint();
		} catch (ShutdownException se) {
			// do something
		} catch (ThrottlingException e) {
			// do something
		} catch (InvalidStateException e) {
			// do something
		}
	}

}
