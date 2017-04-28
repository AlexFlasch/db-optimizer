/**
 * Tuple.java - base class for all DB tuples
 */

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class Tuple implements Comparable<Tuple> {
    private static long tuple_counter = 0;
    String[] my_names;
    String[] my_types;
    HashMap my_data;
    int num_attribs;
    String[] my_values;
    private String key = "";

    /**
     * Constructor for the Tuple class
     * @param attrib_names - array of attribute names
     * @param attrib_types - array of attribute types.  Each type must
     * be one of "FloatValue", "IntValue", "StringValue"
     * @param attrib_values - the values of the attributes in their string representation
     *
     */
    public Tuple(String attrib_names[], String attrib_types[], String attrib_values[]) {

        tuple_counter++;
        num_attribs = attrib_types.length;
        my_names = attrib_names;
        my_types = attrib_types;
        my_data = new HashMap();
        my_values = attrib_values;

        for (int i = 0; i < num_attribs; i++) {
            try {
                Class cl = Class.forName(attrib_types[i]);
                Constructor constructor =
                        cl.getConstructor(new Class[]{String.class});
                my_data.put(attrib_names[i],
                        (ColumnValue) constructor.newInstance
                                (new Object[]{attrib_values[i]}));
                key += attrib_values[i];
            } catch (java.lang.ClassNotFoundException e) {
                System.out.println(e.toString());
            } catch (java.lang.NoSuchMethodException e) {
                System.out.println(e.toString());
            } catch (java.lang.reflect.InvocationTargetException e) {
                System.out.println(e.toString());
            } catch (java.lang.InstantiationException e) {
                System.out.println(e.toString());
            } catch (java.lang.IllegalAccessException e) {
                System.out.println(e.toString());
            }

        }
    }

    /**
     * @return the total number of tuple accesses across all tuples
     *
     */
    public static long tuple_accesses() {
        return tuple_counter;
    }

    /**
     * Reset the tuple access counter to zero
     *
     */
    public static void reset_access_counter() {
        tuple_counter = 0;
    }

    /**
     * @param attrib - the name of the attribute you want
     * @return the value of the desired attribute
     *
     */
    public ColumnValue get_val(String attrib) {
        tuple_counter++;
        return (ColumnValue) my_data.get(attrib);
    }

    public static Tuple combine(Tuple t1, Tuple t2) {
        int namesLength = t1.my_names.length + t2.my_names.length;
        int typesLength = t1.my_types.length + t2.my_types.length;
        int valuesLength = t1.my_values.length + t2.my_values.length;

        ArrayList<String> tempNames = new ArrayList<>(Arrays.asList(t1.my_names));
        ArrayList<String> tempTypes = new ArrayList<>(Arrays.asList(t1.my_types));
        ArrayList<String> tempValues = new ArrayList<>(Arrays.asList(t1.my_values));

        // combine names array
        tempNames.addAll(Arrays.asList(t2.my_names));

        // combine types array
        tempTypes.addAll(Arrays.asList(t2.my_types));

        // combine values array
        tempValues.addAll(Arrays.asList(t2.my_values));

        return new Tuple(tempNames.toArray(new String[namesLength]), tempTypes.toArray(new String[typesLength]), tempValues.toArray(new String[valuesLength]));
    }

    @Override
    public boolean equals(Object obj){
        if (this == obj) return true;
        if (!(obj instanceof Tuple)) return false;

        Tuple that = (Tuple)obj;
        return this.key.equals(that.key);
    }

    @Override
    public int hashCode(){
        return key.hashCode();
    }

    @Override
    public int compareTo(Tuple that){
        //returns -1 if "this" object is less than "that" object
        //returns 0 if they are equal
        //returns 1 if "this" object is greater than "that" object
        return this.key.compareTo(that.key);
    }
}
