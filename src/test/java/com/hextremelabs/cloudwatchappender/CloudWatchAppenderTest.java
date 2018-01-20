package com.hextremelabs.cloudwatchappender;

import org.apache.log4j.Category;
import org.apache.log4j.Priority;
import org.apache.log4j.spi.LoggingEvent;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author oladeji
 */
class CloudWatchAppenderTest {

  private static final ExecutorService POOL = Executors.newFixedThreadPool(100);

  @Test
  void retrieveLogsAndClear_emptyQueue() {
    assertTrue(CloudWatchAppender.retrieveLogsAndClear().isEmpty());
  }

  @Test
  void retrieveLogsAndClear_multiProducerPopulatedQueue() {
    generate100kLogs();
    final Collection<LoggingEvent> events = CloudWatchAppender.retrieveLogsAndClear();
    assertEquals(100000, events.size());
    final Iterator<LoggingEvent> iterator = events.iterator();
    long previous = iterator.next().timeStamp;
    while (iterator.hasNext()) {
      long current = iterator.next().timeStamp;
      assertTrue(current >= previous);
      previous = current;
    }
  }

  private static void generate100kLogs() {
    final List<Future> futures = new LinkedList<>();
    for (int a = 0; a < 100; a++) {
      futures.add(POOL.submit(newWritingTask()));
    }

    futures.removeIf(e -> {
      try {
        e.get();
        return true;
      } catch (InterruptedException | ExecutionException e1) {
        fail("Unable to await concurrent WRITE");
        return false;
      }
    });
  }

  @NotNull
  private static Runnable newWritingTask() {
    return () -> {
      final CloudWatchAppender sut = new CloudWatchAppender();
      for (int a = 0; a < 1000; a++) {
        LoggingEvent event = new LoggingEvent("stuff", Category.getRoot(),
            Priority.ERROR, "Random event" + (1000 * Math.random()), null);
        sut.append(event);
      }
    };
  }

  @AfterAll
  static void tearDown() {
    POOL.shutdownNow();
  }
}