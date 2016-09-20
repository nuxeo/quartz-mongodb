package com.novemberain.quartz.mongodb.clojure;

import org.quartz.spi.ClassLoadHelper;

import java.io.InputStream;
import java.net.URL;


/**
 * Makes it possible for Quartz to load and instantiate jobs that are defined
 * using Clojure defrecord without AOT compilation.
 */
public class DynamicClassLoadHelper implements ClassLoadHelper {

    private final ClassLoader classLoader;

    public DynamicClassLoadHelper() {
        ClassLoader cl;
        try {
            // use Clojure DynamicClassLoader if available
            @SuppressWarnings("unchecked")
            Class<ClassLoader> classLoaderClass = (Class<ClassLoader>) Class.forName("clojure.lang.DynamicClassLoader");
            cl = classLoaderClass.newInstance();
        } catch (ReflectiveOperationException e) {
            cl = null;
        }
        classLoader = cl;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader == null ? Thread.currentThread().getContextClassLoader() : classLoader;
    }

    @Override
    public URL getResource(String name) {
        return null;
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        return null;
    }

    @Override
    public void initialize() {
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        return null;
    }

    @Override
    public <T> Class<? extends T> loadClass(String name, Class<T> clazz)
            throws ClassNotFoundException {
        return null;
    }
}
