package org.example.model;

import lombok.EqualsAndHashCode;
import lombok.Value;
import org.example.core.StreamItem;

@Value
@EqualsAndHashCode(callSuper = true)
public class BoroughCountItem extends StreamItem {

    String borough;
    int count;

    public BoroughCountItem(String borough, int count) {
        this.borough = borough;
        this.count = count;
    }

    @Override
    public boolean isPunctuation() {
        return false;
    }
}
