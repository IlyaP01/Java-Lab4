package com.pipeline;

public class ManagerConfigGrammar implements IConfigGrammar {
    public enum ConfigParams {
        INPUT_FILE ("INPUT_FILE"),
        OUTPUT_FILE ("OUTPUT_FILE"),
        READER_NAME ("READER_NAME"),
        WRITER_NAME ("WRITER_NAME"),
        EXECUTORS_NAMES ("EXECUTORS_NAMES"),
        READER_CONFIG ("READER_CONFIG"),
        EXECUTORS_CONFIGS ("EXECUTORS_CONFIGS"),
        WRITER_CONFIG ("WRITER_CONFIG"),
        LOG_FILE ("LOG_FILE");

        private final String str;
        ConfigParams(String str) {
            this.str = str;
        }

        public String toStr() {
            return str;
        }
    }

    @Override
    public boolean hasKey(String key) {
        for (ConfigParams param : ConfigParams.values()) {
            if (key.equalsIgnoreCase(param.toStr()))
                return true;
        }
        return false;
    }
}
