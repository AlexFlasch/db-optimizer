import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * JoinTable - this class implements the join operation of a
 * relational DB
 */

public class JoinTable extends Table {

    Table first_join_tab;
    Table second_join_tab;

    Conditional joinCondition;

    /**
     * @param t1 - One of the tables for the join
     * @param t2 - The other table for the join.  You are guaranteed
     *           that the tables do not share any common attribute names.
     * @param c  - the conditional used to make the join
     */
    public JoinTable(Table t1, Table t2, Conditional c) {

        super("Joining " + t1.toString() + " " + t2.toString() + (c == null ? "with a natural join" : " on condition " + c.toString()));
        first_join_tab = t1;
        second_join_tab = t2;
        joinCondition = c;

        int namesLength = t1.attr_names.length + t2.attr_names.length;
        int typesLength = t1.attr_types.length + t2.attr_types.length;

        attr_names = new String[namesLength];
        List<String> temp = new ArrayList<>();
        temp.addAll(Arrays.asList(t1.attr_names));
        temp.addAll(Arrays.asList(t2.attr_names));
        temp.toArray(attr_names);

        attr_types = new String[typesLength];
        List<String> temp1 = new ArrayList<>();
        temp1.addAll(Arrays.asList(t1.attr_types));
        temp1.addAll(Arrays.asList(t2.attr_types));
        temp1.toArray(attr_types);
    }

    public Table[] my_children() {
        return new Table[]{first_join_tab, second_join_tab};
    }

    public Table optimize() {
        // Right now no optimization is done -- you'll need to improve this

        // join the table that has the least amount of columns as the outer table
        if(first_join_tab.attr_names.length < second_join_tab.attr_names.length) {
            return this;
        } else {
            return new JoinTable(second_join_tab, first_join_tab, this.joinCondition);
        }
    }

    public ArrayList<Tuple> evaluate() {
        ArrayList<Tuple> tuples_to_return = new ArrayList<Tuple>();

        // if the join condition is null perform a natural join
        if(joinCondition == null) {
            for (Tuple tup1 : first_join_tab.evaluate()) {
                for (Tuple tup2 : second_join_tab.evaluate()) {
                    Tuple tempTuple = Tuple.combine(tup1, tup2);

                    tuples_to_return.add(tempTuple);
                }
            }
        } else {
            for (Tuple tup1 : first_join_tab.evaluate()) {
                for (Tuple tup2 : second_join_tab.evaluate()) {
                    Tuple tempTuple = Tuple.combine(tup1, tup2);

                    if(joinCondition.truthVal(tempTuple)) {
                        tuples_to_return.add(tempTuple);
                    }
                }
            }
        }

        profile_intermediate_tables(tuples_to_return);
        return tuples_to_return;
    }

}