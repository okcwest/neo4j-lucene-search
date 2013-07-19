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
  * @param  map A map whose contents this PropertyMap will contain
  */
  public PropertyMap(Map map) {
    super();
    this.putAll(map);
  }

  /** 
  * Try to get a value of type {@code double} from {@code key} in {@code props}
  * @param props The property map
  * @param key  The key to retrieve
  * @return A double value found at {@code key} in {@code props}
  * @throws IllegalArgumentException if the key does not exist in props or its value cannot be coerced to a double 
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
  * Try to get a value of type {@code float} from {@code key} in {@code props}
  * @param props The property map
  * @param key  The key to retrieve
  * @return A float value found at {@code key} in {@code props}
  * @throws IllegalArgumentException if the key does not exist in props or its value cannot be coerced to a float 
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
  * Try to get a value of type {@code int} from {@code key} in {@code props}
  * @param props The property map
  * @param key  The key to retrieve
  * @return An int value found at {@code key} in {@code props}
  * @throws IllegalArgumentException if the key does not exist in props or its value cannot be coerced to an int 
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
  * Try to get a value of type {@code double} from {@code key} in this map
  * @param key  The key to retrieve
  * @return A double value found at {@code key}
  * @throws IllegalArgumentException if the key does not exist in props or its value cannot be coerced to a double 
  */
  public double getDouble(K key) throws IllegalArgumentException {
    Object o = this.get(key);
    if (o == null)
      throw new IllegalArgumentException("Property map has no value for key "+key);
    return getDoubleFromObject(o);
  }
  
  /** 
  * Try to get a value of type {@code float} from {@code key} in this map
  * @param key  The key to retrieve
  * @return A float value found at {@code key}
  * @throws IllegalArgumentException if the key does not exist in props or its value cannot be coerced to a float 
  */
  public float getFloat(K key) throws IllegalArgumentException {
    Object o = this.get(key);
    if (o == null)
      throw new IllegalArgumentException("Property map has no value for key "+key);
    return getFloatFromObject(o);
  }
  
  /** 
  * Try to get a value of type {@code int} from {@code key} in this map
  * @param key  The key to retrieve
  * @return An int value found at {@code key}
  * @throws IllegalArgumentException if the key does not exist in props or its value cannot be coerced to an int 
  */
  public int getInt(K key) throws IllegalArgumentException {
    Object o = this.get(key);
    if (o == null)
      throw new IllegalArgumentException("Property map has no value for key "+key);
    return getIntFromObject(o);
  }

  /**
  * Get a validated set of coordinates from this map, in which latitude 
    and longitude are guaranteed to exist and be sane.
  * @param props The property map to find coordinates in
  * @param latKey The key where we expect a value for latitude
  * @param lonKey The key where we expect a value for longitude
  * @return A map of {latKey: latitude, lonKey: longitude} where both values
            are valid, or null if this map does not contain a valid set 
            of coordinates at these keys.
  */
  public PropertyMap<K, Double> getCoords(K latKey, K lonKey)
    throws IllegalArgumentException {
    PropertyMap<K, Double> coords = new PropertyMap<K, Double>();
    double lat = this.getDouble(latKey);
    if (lat > 90 || lat < -90)
      throw new IllegalArgumentException("Latitude must be in the range 0 +- 90, but was "+lat);
    coords.put(latKey, lat);
    double lon = this.getDouble(lonKey);
    if (lon > 180 || lon < -180)
      throw new IllegalArgumentException("Longitude must be in the range 0 +- 180, but was "+lon);
    coords.put(lonKey, lon);
    return coords;
  }
  
  /**
  * Get a validated search radius from this map, in which latitude, 
    longitude, and distance are guaranteed to exist and be sane.
  * @param props The property map to find coordinates in
  * @param latKey The key where we expect a value for latitude
  * @param lonKey The key where we expect a value for longitude
  * @param distKey The key where we expect a value for distance
  * @return A map of {latKey: latitude, lonKey: longitude, distKey: distance} 
            where all values are valid, or null if this map does not 
            contain a valid set of coordinates at these keys.
  */
  public PropertyMap<K, Double> getSearchRadius(K latKey, K lonKey, K distKey) 
    throws IllegalArgumentException {
    // this throws if the coords are no good
    PropertyMap<K, Double> searchRadius = this.getCoords(latKey, lonKey);
    double dist = this.getDouble(distKey);
    if (dist <= 0)
      throw new IllegalArgumentException("Distance must be a positive value, but was "+dist);
    searchRadius.put(distKey, dist);
    return searchRadius;
  }
  
  
  private static double getDoubleFromObject(Object o) throws IllegalArgumentException {
    if (o == null) {
      throw new IllegalArgumentException("Can't coerce null to a double.");
    }
    try {
      return ((Number)o).doubleValue();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Can't coerce object of type "+o.getClass().getName()+" to a double.");
    }
  }
  
  private static float getFloatFromObject(Object o) throws IllegalArgumentException {
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
  
  private static int getIntFromObject(Object o) throws IllegalArgumentException {
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