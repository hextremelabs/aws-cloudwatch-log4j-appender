package com.hextremelabs.cloudwatchappender;

import com.amazonaws.annotation.ThreadSafe;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author oladeji
 */
@ThreadSafe
public class CloudWatchAppender extends AppenderSkeleton {

  private static final AtomicReference<Queue<LoggingEvent>> LOGS = new AtomicReference<>(new ConcurrentLinkedQueue<>());

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
    return LOGS.getAndSet(new ConcurrentLinkedQueue<>());
  }
}
