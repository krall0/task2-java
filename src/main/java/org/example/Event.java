package org.example;

import java.util.List;

public record Event(List<Address> recipients, Payload payload) {}