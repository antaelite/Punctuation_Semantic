package org.example.predicates;

import org.example.model.Tuple;

import java.io.Serializable;

public interface PunctuationPredicate extends Serializable {
    boolean matches(Tuple t);
    boolean intersects(PunctuationPredicate p);
}
