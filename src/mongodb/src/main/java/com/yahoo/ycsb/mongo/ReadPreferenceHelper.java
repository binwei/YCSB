package com.yahoo.ycsb.mongo;

import com.mongodb.ReadPreference;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

public class ReadPreferenceHelper {

    private static final Map<String, ReadPreference> values = new HashMap<String, ReadPreference>(2);

    static {
        for (Field field : ReadPreference.class.getFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType().equals(ReadPreference.class)) {
                try {
                    values.put(field.getName().toLowerCase(), (ReadPreference) field.get(null));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static ReadPreference valueOf(String name) {
        return values.get(name.toLowerCase());
    }
}
