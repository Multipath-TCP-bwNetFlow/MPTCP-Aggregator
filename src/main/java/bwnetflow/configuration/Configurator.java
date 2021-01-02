package bwnetflow.configuration;

import bwnetflow.configuration.Configuration;

public interface Configurator {
    Configuration parseCLIArguments(String[] args);
}
