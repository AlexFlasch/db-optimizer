import java.util.ArrayList;
import java.util.ListIterator;

/**
 * ProjectionTable - this class implements the projection operation of a
 * relational DB
 */

public class ProjectionTable extends Table {

    Table tab_projecting_on;

    /**
     * @param t                  - the table from which attributes are projected
     * @param attribs_to_project - the names of the attributes that are to be projected
     */
    public ProjectionTable(Table t, String attribs_to_project[]) {

        super("Projecting from " + t.toString());
        tab_projecting_on = t;
        attr_names = attribs_to_project;
        attr_types = calculateProjectedAttributeTypes(attr_names);
    }

    public Table[] my_children() {
        return new Table[]{tab_projecting_on};
    }

    public Table optimize() {

        // One of the projection optimizations is done to illustrate the technique
        if (tab_projecting_on instanceof ProjectionTable) {
            return new ProjectionTable(((ProjectionTable) tab_projecting_on).tab_projecting_on.optimize(),
                    attr_names);
        }/* else if (tab_projecting_on instanceof JoinTable) {
            // distribute projection over join
            JoinTable joinTab = (JoinTable) tab_projecting_on;

            ArrayList<String> firstTabProjCols = new ArrayList<>();
            ArrayList<String> secondTabProjCols = new ArrayList<>();

            // find what table the projection columns originated from before the join
            for(String projColName : this.attr_names) {
                for(String joinColName : joinTab.first_join_tab.attr_names) {
                    if(joinColName.equalsIgnoreCase(projColName)){
                        firstTabProjCols.add(projColName);
                    }
                }

                for(String joinColName : joinTab.second_join_tab.attr_names) {
                    if(joinColName.equalsIgnoreCase(projColName)){
                        secondTabProjCols.add(projColName);
                    }
                }
            }

            // find the columns that are needed for the join to happen


            ProjectionTable tab1 = new ProjectionTable();
            ProjectionTable tab2 = new ProjectionTable();
        }*/ else {
            tab_projecting_on = tab_projecting_on.optimize();
            return this;
        }
    }

    public ArrayList<Tuple> evaluate() {

        ArrayList<Tuple> tuples1 = tab_projecting_on.evaluate();
        ArrayList<Tuple> tuples_to_return = new ArrayList<Tuple>();
        ListIterator iterate_tuples = tuples1.listIterator(0);

        while (iterate_tuples.hasNext()) {
            Tuple tupleToProject = (Tuple) iterate_tuples.next();
            String[] projectedValues = new String[attr_names.length];
            for (int i = 0; i < attr_names.length; i++) {
                projectedValues[i] = tupleToProject.get_val(attr_names[i]).toString();
            }
            Tuple projectedTuple = new Tuple(attr_names, attr_types, projectedValues);
            tuples_to_return.add(projectedTuple);
        }
        profile_intermediate_tables(tuples_to_return);
        return tuples_to_return;
    }

    /**
     * Determines the types of the specified attributes.
     *
     * @param attributesToProject
     * @return String array of attribute types
     */
    String[] calculateProjectedAttributeTypes(String[] attributesToProject) {
        String[] attributeTypes = new String[attributesToProject.length];
        String[] attr_names = tab_projecting_on.attrib_names();
        String[] attr_types = tab_projecting_on.attrib_types();

        for (int i = 0; i < attributesToProject.length; i++) {
            String projectedAttribute = attributesToProject[i];
            String projectedAttributeType = null;
            for (int j = 0; j < attr_names.length; j++) {
                if (projectedAttribute.equals(attr_names[j])) {
                    projectedAttributeType = attr_types[j];
                    break;
                }
            }

            attributeTypes[i] = projectedAttributeType;
        }

        return attributeTypes;
    }


}