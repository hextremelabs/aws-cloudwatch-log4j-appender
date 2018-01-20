package com.hextremelabs.cloudwatchappender;

import com.amazonaws.annotation.ThreadSafe;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Comparator.comparing;

/**
 * @author oladeji
 */
@ThreadSafe
public class CloudWatchAppender extends AppenderSkeleton {
  private static final int INITIAL_CAPACITY = 11; // copied from PBQ implementation
  private static final AtomicReference<Queue<LoggingEvent>> LOGS = new AtomicReference<>(
          new PriorityBlockingQueue<>(INITIAL_CAPACITY, comparing(LoggingEvent::getTimeStamp)));

  @Override
  protected void append(LoggingEvent loggingEvent) {
    LOGS.updateAndGet(queue -> {
      queue.add(loggingEvent);
      return queue;
    });
  }

  @Override
  public void close() {
  }

  @Override
  public boolean requiresLayout() {
    return true;
  }

  static Collection<LoggingEvent> retrieveLogsAndClear() {
      Queue<LoggingEvent> queue = new PriorityBlockingQueue<>(INITIAL_CAPACITY, comparing(LoggingEvent::getTimeStamp));
      return LOGS.getAndSet(queue);
  }
}
