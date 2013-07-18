package com.okcupidlabs.neo4j.server.plugins.lucene;

import java.util.Map;
import java.util.HashMap;

import java.util.logging.*;

/**
* Adds some convenience methods to a HashMap so we can fetch typed values.
*/
public class PropertyMap<K,V> extends HashMap<K,V> {
  
  private static final Logger log = Logger.getLogger(LuceneSearch.class.getName());

  /**
  * Construct an empty property map
  */
  public PropertyMap() {
    super();
  }
  
  /**
  * Instantiate a property map with values taken from map
  * @param A map whose contents this PropertyMap will contain
  */
  public PropertyMap(Map map) {
    super();
    this.putAll(map);
  }

  /** 
  * Get a value of type double.
  * @param props The property map
  * @param key  The key to retrieve
  * @return A double value found at {@code key} in {@code props}
  * @throws {@code IllegalArgumentException} if the key does not exist in props or its value cannot be coerced to a double 
  */
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
  
  /** 
  * Get a value of type float.
  * @param props The property map
  * @param key  The key to retrieve
  * @return A float value found at {@code key} in {@code props}
  * @throws {@code IllegalArgumentException} if the key does not exist in props or its value cannot be coerced to a float 
  */
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
  
  /** 
  * Get a value of type int.
  * @param props The property map
  * @param key  The key to retrieve
  * @return An int value found at {@code key} in {@code props}
  * @throws {@code IllegalArgumentException} if the key does not exist in props or its value cannot be coerced to an int 
  */
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
  
  /** 
  * Get a value of type double.
  * @param key  The key to retrieve
  * @return A double value found at {@code key}
  * @throws {@code IllegalArgumentException} if the key does not exist in props or its value cannot be coerced to a double 
  */
  public double getDouble(K key) throws IllegalArgumentException {
    Object o = this.get(key);
    if (o == null)
      throw new IllegalArgumentException("Property map has no value for key "+key);
    return getDoubleFromObject(o);
  }
  
  /** 
  * Get a value of type float.
  * @param key  The key to retrieve
  * @return A float value found at {@code key}
  * @throws {@code IllegalArgumentException} if the key does not exist in props or its value cannot be coerced to a float 
  */
  public float getFloat(K key) throws IllegalArgumentException {
    Object o = this.get(key);
    if (o == null)
      throw new IllegalArgumentException("Property map has no value for key "+key);
    return getFloatFromObject(o);
  }
  
  /** 
  * Get a value of type int.
  * @param key  The key to retrieve
  * @return An int value found at {@code key}
  * @throws {@code IllegalArgumentException} if the key does not exist in props or its value cannot be coerced to an int 
  */
  public int getInt(K key) throws IllegalArgumentException {
    Object o = this.get(key);
    if (o == null)
      throw new IllegalArgumentException("Property map has no value for key "+key);
    return getIntFromObject(o);
  }
  
  /** 
  * Try to coerce an object to a value of type double
  * @param o  The object
  * @return The double value of {@code o}
  * @throws {@code IllegalArgumentException} if o cannot be coerced to a double. 
  */
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
  
  /** 
  * Try to coerce an object to a value of type float
  * @param o  The object
  * @return The float value of {@code o}
  * @throws {@code IllegalArgumentException} if o cannot be coerced to a float. 
  */
  public static float getFloatFromObject(Object o) throws IllegalArgumentException {
    if (o == null) {
      throw new IllegalArgumentException("Can't coerce null to a float.");
    }
    // warn on loss of precision!
    if (o instanceof Double) {
      log.warning("Coercing "+o.getClass().getName()+" to float! Loss of precision.");
    }
    try {
      return ((Number)o).floatValue();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Can't coerce object of type "+o.getClass().getName()+" to a float.");
    }
  }
  
  /** 
  * Try to coerce an object to a value of type float
  * @param o  The object
  * @return The float value of {@code o}
  * @throws {@code IllegalArgumentException} if o cannot be coerced to a float. 
  */
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