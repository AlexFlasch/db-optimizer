import java.util.ArrayList;

/**
 * DupsRemovedTable - this class implements the duplicate removal operation of a
 * relational DB
 */

public class DupsRemovedTable extends Table {

    Table tab_dups_removed_from;

    /**
     * @param t - the table from which duplicates are to be removed
     */
    public DupsRemovedTable(Table t) {

        super("Removing duplicates from " + t.toString());
        tab_dups_removed_from = t;

    }

    public Table[] my_children() {
        return new Table[]{tab_dups_removed_from};
    }

    public Table optimize() {
        // Right now no optimization is done -- you'll need to improve this
        return this;
    }

    public ArrayList<Tuple> evaluate() {
        ArrayList<Tuple> toReturn = new ArrayList<>();
        for(Tuple t : tab_dups_removed_from.evaluate()){
            if(!toReturn.contains(t)){
                toReturn.add(t);
            }
        }
        profile_intermediate_tables(toReturn);
        return toReturn;
    }

}