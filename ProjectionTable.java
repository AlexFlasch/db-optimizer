import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

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
        } else if (tab_projecting_on instanceof JoinTable &&
                (!((ComparisonConditional) ((JoinTable) tab_projecting_on).joinCondition).left.isConstant() ||
                !((ComparisonConditional) ((JoinTable) tab_projecting_on).joinCondition).right.isConstant())) {
            // distribute projection over join
            JoinTable joinTab = (JoinTable) tab_projecting_on;

            String[] projList = this.attr_names;

            boolean hasOverlap = false;

            for(String projCol : projList) {
                String projAttr = projCol.split("\\.")[1];

                List<String> tab1ColNames = Arrays.stream(joinTab.first_join_tab.attr_names).map(name -> name.split("\\.")[1]).collect(Collectors.toList());
                List<String> tab2ColNames = Arrays.stream(joinTab.second_join_tab.attr_names).map(name -> name.split("\\.")[1]).collect(Collectors.toList());

                boolean tab1HasProjCol = tab1ColNames.stream().filter(name -> name.equalsIgnoreCase(projAttr)).collect(Collectors.toList()).size() > 0;
                boolean tab2HasProjCol = tab2ColNames.stream().filter(name -> name.equalsIgnoreCase(projAttr)).collect(Collectors.toList()).size() > 0;

                if(tab1HasProjCol && tab2HasProjCol) {
                    hasOverlap = true;
                    break;
                }
            }

            ComparisonConditional joinCond = (ComparisonConditional) joinTab.joinCondition;

            boolean joinCondWillBeRemoved = false;
            boolean joinCondIsConstant = false;

            if(joinCond.left.isConstant() || joinCond.right.isConstant()) {
                joinCondIsConstant = true;
            } else {
                joinCondWillBeRemoved = Arrays.stream(projList).filter(name -> joinCond.left.attrib_name.equalsIgnoreCase(name)).collect(Collectors.toList()).size() > 0
                        || Arrays.stream(projList).filter(name -> joinCond.right.attrib_name.equalsIgnoreCase(name)).collect(Collectors.toList()).size() > 0;
            }

            if(hasOverlap || joinCondIsConstant || joinCondWillBeRemoved) {
                return this;
            } else {
                // find the col names of each table to be projected
                List<String> tab1ProjCols = Arrays.stream(projList).filter(Arrays.asList(joinTab.first_join_tab.attr_names)::contains).collect(Collectors.toList());
                List<String> tab2ProjCols = Arrays.stream(projList).filter(Arrays.asList(joinTab.second_join_tab.attr_names)::contains).collect(Collectors.toList());

                return new JoinTable(
                        new ProjectionTable(joinTab.first_join_tab, tab1ProjCols.toArray(new String[tab1ProjCols.size() - 1])),
                        new ProjectionTable(joinTab.second_join_tab, tab2ProjCols.toArray(new String[tab2ProjCols.size() - 1])),
                        joinTab.joinCondition);
            }
        } else {
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