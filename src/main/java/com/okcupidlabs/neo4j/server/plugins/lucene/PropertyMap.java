package com.okcupidlabs.neo4j.server.plugins.lucene;

import java.util.Map;
import java.util.HashMap;

import java.util.logging.*;

public class PropertyMap<K,V> extends HashMap<K,V> {
  
  private static final Logger log = Logger.getLogger(LuceneSearch.class.getName());

  public PropertyMap() {
    super();
  }
  
  public PropertyMap(Map map) {
    super();
    this.putAll(map);
  }
  // adds a few convenience methods for getting typed numeric arguments
  
  public static <T> float getFloat(Map<T, Object> props, T key) throws IllegalArgumentException {
    Object value = props.get(key);
    if (value == null)
      throw new IllegalArgumentException("Property map has no value for key "+key);
    try {
      return getFloatFromObject(value);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Can't get a float for key "+key+" in property map: "+e.getMessage());
    }
  }
  
  public static <T> double getDouble(Map<T, Object> props, T key) throws IllegalArgumentException {
    Object value = props.get(key);
    if (value == null)
      throw new IllegalArgumentException("Property map has no value for key "+key);
    try {
      return getDoubleFromObject(value);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Can't get a double for key "+key+" in property map: "+e.getMessage());
    }
  }
  
  // get ints from any numeric type also, but warn on loss of precision.
  public static <T> int getInt(Map<T, Object> props, T key) throws IllegalArgumentException {
    Object value = props.get(key);
    if (value == null)
      throw new IllegalArgumentException("Property map has no value for key "+key);
    try {
      return getIntFromObject(value);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Can't get an int for key "+key+" in property map: "+e.getMessage());
    }
  }
  
  public float getFloat(K key) throws IllegalArgumentException {
    Object o = this.get(key);
    return getFloatFromObject(o);
  }
  
  public double getDouble(K key) throws IllegalArgumentException {
    Object o = this.get(key);
    return getDoubleFromObject(o);
  }
  
  public int getInt(K key) throws IllegalArgumentException {
    Object o = this.get(key);
    return getIntFromObject(o);
  }
  
  public static float getFloatFromObject(Object o) throws IllegalArgumentException {
    if (o == null) {
      throw new IllegalArgumentException("Can't coerce null to a float.");
    }
    try {
      return ((Number)o).floatValue();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Can't coerce object of type "+o.getClass().getName()+" to a float.");
    }
  }
  
  public static double getDoubleFromObject(Object o) throws IllegalArgumentException {
    if (o == null) {
      throw new IllegalArgumentException("Can't coerce null to a double.");
    }
    try {
      return ((Number)o).doubleValue();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Can't coerce object of type "+o.getClass().getName()+" to a double.");
    }
  }
  
  public static int getIntFromObject(Object o) throws IllegalArgumentException {
    if (o == null) {
      throw new IllegalArgumentException("Can't coerce null to an int.");
    }
    // warn on loss of precision!
    if (o instanceof Double || o instanceof Float) {
      log.warning("Coercing "+o.getClass().getName()+" to int! Loss of precision.");
    }
    try {
      return ((Number)o).intValue();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Can't coerce object of type "+o.getClass().getName()+" to an int.");
    }
  }
  
  
}