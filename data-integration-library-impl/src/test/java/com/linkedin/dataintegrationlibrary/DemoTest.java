package com.linkedin.dataintegrationlibrary;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.*;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Example test that illustrates basic features of JUnit5.
 * See http://go/junit5
 */
public class DemoTest {

  @Test
  void example_assertion() {
    assertThat(HelloMP.hello()).isEqualTo("Hello, multiproduct world!");
  }

  @Test
  void example_with_temp_dir(@TempDir File tmp) throws IOException {
    File file = new File(tmp, "some file");

    //when
    file.createNewFile();

    //then
    assertThat(file).isFile();
  }

  @ParameterizedTest
  @ValueSource(strings = {"LinkedIn", "Microsoft", "Google"})
  void example_parameterized(String param) {
    int paramLength = param.length();
    assertThat(paramLength).isGreaterThan(5);
  }

  @Test
  void assertionsDemo() {
    //given
    List<String> list = asList("one", "two", "three");

    //when
    Collections.reverse(list);

    //then
    assertThat(list)
        .hasSize(3)
        .containsSequence("three", "two")
        .containsExactly("three", "two", "one");
  }
}