package org.apache.spark.stemming.feature;
import java.lang.reflect.InvocationTargetException;

public abstract class SnowballStemmer extends SnowballProgram {
    public abstract boolean stem();
};