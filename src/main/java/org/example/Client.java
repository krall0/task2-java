package org.example;

public interface Client {
  //блокирующий метод для чтения данных
  Event readData();

  //блокирующий метод отправки данных
  Result sendData(Address dest, Payload payload);
}