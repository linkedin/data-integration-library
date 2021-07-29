package com.linkedin.dataintegrationlibrary;

public final class HelloMP {

  private HelloMP() {
  }

  static String hello() {
    return "Hello, multiproduct world!";
  }

  public static void main(String[] args) {
    System.out.println(hello());
  }

}
