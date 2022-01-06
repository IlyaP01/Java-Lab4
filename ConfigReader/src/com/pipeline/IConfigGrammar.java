package com.pipeline;

public interface IConfigGrammar {
    default String getSeparator() {
        return "=";
    };
    default String getCommentLinePrefix() { return "#"; }
    boolean hasKey(String key);
}
