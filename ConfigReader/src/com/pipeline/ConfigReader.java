package com.pipeline;

import com.java_polytech.pipeline_interfaces.RC;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class ConfigReader {
    private final IConfigGrammar grammar;
    private final RC.RCWho owner;
    private final HashMap<String, String> params = new HashMap<>();

    public ConfigReader(RC.RCWho owner, IConfigGrammar grammar) {
        this.grammar = grammar;
        this.owner = owner;
    }

    public RC read (String configFile) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(configFile));
            String line = reader.readLine();
            while (line != null) {
                if (line.isEmpty() || line.startsWith(grammar.getCommentLinePrefix())) {
                    line = reader.readLine();
                    continue;
                }

                String[] words = line.split(grammar.getSeparator());

                if (words.length != 2)
                    return new RC(owner,
                            RC.RCType.CODE_CONFIG_GRAMMAR_ERROR,
                            "Grammar error in " + configFile + " in line: " + line);

                words[0] = words[0].trim();
                words[1] = words[1].trim();
                if (!grammar.hasKey(words[0]))
                    return new RC(owner, RC.RCType.CODE_CONFIG_GRAMMAR_ERROR, "Invalid parameter " +
                            words[0] + " in config file");

                if (!params.containsKey(words[0]))
                    params.put(words[0], words[1]);
                else
                    return new RC(owner, RC.RCType.CODE_CONFIG_GRAMMAR_ERROR, "Redefinition of parameter " +
                            words[0] +  " in config file");

                line = reader.readLine();
            }
        } catch (FileNotFoundException e) {
            return new RC(owner, RC.RCType.CODE_CONFIG_FILE_ERROR, "Could not open " + configFile);
        } catch (IOException e) {
            return new RC(owner, RC.RCType.CODE_FAILED_TO_READ, "Could not read " + configFile);
        }
        return RC.RC_SUCCESS;
    }

    public boolean hasKey(String key) {
        return params.containsKey(key);
    }

    public String getParam(String key) {
        return params.getOrDefault(key, "");
    }
}
