package com.pipeline;

public class ExecutorConfigGrammar implements IConfigGrammar {
    enum ConfigParams {
        BUFFER_SIZE ("BUFFER_SIZE"),
        MODE ("MODE"),
        NUM_OF_THREADS("NUM_OF_THREADS"),
        MAX_PACKAGES_NUM("MAX_PACKAGES_NUM");

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
