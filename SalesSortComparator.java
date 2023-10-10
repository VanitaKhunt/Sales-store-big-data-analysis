package org.myorg;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SalesSortComparator extends WritableComparator {

    // Constructor to set up the WritableComparator with the Text.class type
    protected SalesSortComparator() {
        super(Text.class, true);
    }

    // Compares two WritableComparable objects, a and b
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        // Cast the input WritableComparables to Text objects
        Text part1 = (Text) w1;
        Text part2 = (Text) w2;

        // Split the Text objects into arrays by tab characters
        String[] key1Parts = part1.toString().split("\t");
        String[] key2Parts = part2.toString().split("\t");

        // Extract the subcategory and date from the key parts
        String subcategory1 = key1Parts[0];
        String subcategory2 = key2Parts[0];
        String date1 = key1Parts[1];
        String date2 = key2Parts[1];

        // Compare the subcategories
        int subcategoryCompare = subcategory1.compareTo(subcategory2);
        // If subcategories are different, return the comparison result
        if (subcategoryCompare != 0) {
            return subcategoryCompare;
        }

        // If subcategories are the same, compare the dates and return the result
        return date1.compareTo(date2); // Sort by date (ascending order)
    }
}
