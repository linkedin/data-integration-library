package com.linkedin.cdi.factory.producer;

/**
 * An interface to implement producer for events and metrics
 */
public interface EventReporter<T> {
  void send(T obj);
  void close();
}
