package com.hextremelabs.cloudwatchappender;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.Comparator.comparing;

/**
 * @author oladeji
 */
public class CloudWatchAppender extends AppenderSkeleton {

  private static final Queue<LoggingEvent> LOGS = new ConcurrentLinkedQueue<>();

  @Override
  protected void append(LoggingEvent loggingEvent) {
    LOGS.add(loggingEvent);
  }

  @Override
  public void close() {
  }

  @Override
  public boolean requiresLayout() {
    return true;
  }

  public static Collection<LoggingEvent> retrieveLogsAndClear() {
    final List<LoggingEvent> events = new LinkedList<>();
    LOGS.removeIf(e -> events.add(e));
    events.sort(comparing(LoggingEvent::getTimeStamp));
    return events;
  }
}
