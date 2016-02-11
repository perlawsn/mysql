package org.dei.perla.fpc.mysql;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import org.dei.perla.core.fpc.Task;
import org.dei.perla.core.fpc.TaskHandler;
import org.dei.perla.core.fpc.Sample;

public class LatchingTaskHandler implements TaskHandler {

	private volatile int count = 0;
	private volatile int waitCount;
	private volatile ArrayList<Sample> allRecords = new ArrayList<Sample>();

	private volatile long previousTime = 0;
	private volatile double avgPeriod = 0;

	private final CountDownLatch latch;
	private volatile Throwable error;

	private volatile Sample lastRecord = null;

	public LatchingTaskHandler(int waitCount) {
		latch = new CountDownLatch(waitCount);
		this.waitCount = waitCount;
	}

	public int getCount() throws InterruptedException {
		latch.await();
		if (error != null) {
			throw new RuntimeException(error);
		}
		return count;
	}

	public double getAveragePeriod() throws InterruptedException {
		latch.await();
		if (error != null) {
			throw new RuntimeException(error);
		}
		return avgPeriod;
	}

	public ArrayList<Sample> getAllRecords() throws InterruptedException {
		latch.await();
		if (error != null) {
			throw new RuntimeException(error);
		}
		return allRecords;
	}

	public Sample getLastRecord() throws InterruptedException {
		latch.await();
		if (error != null) {
			throw new RuntimeException(error);
		}
		return lastRecord;
	}

	@Override
	public void complete(Task task) {

	}

	@Override
	public void data(Task task, Sample record) {			
		lastRecord = record;
		if (previousTime == 0) {
			previousTime = System.currentTimeMillis();
			return;
		}	
		count++;
		if(count == waitCount){			
			task.stop();
		}					
		if (count <= waitCount) {
			allRecords.add(record);
		}			
		
		latch.countDown();
		
		avgPeriod = (avgPeriod + (System.currentTimeMillis() - previousTime))
				/ count;
		
	}

	@Override
	public void error(Task task, Throwable cause) {
		error = cause;
		while (latch.getCount() > 0) {
			latch.countDown();
		}
	}

}
