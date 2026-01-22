package org.example.model;

import org.example.predicates.PunctuationPredicate;

public class Punctuation implements StreamElement {
    private PunctuationPredicate predicate;
    public Punctuation(PunctuationPredicate predicate) {
        this.predicate = predicate;
    }
    public PunctuationPredicate getPredicate() { return predicate; }
    public boolean matches(Tuple tuple) {
        return predicate.matches(tuple);
    }

    @Override
    public boolean isData() { return false; }

    @Override
    public boolean isPunctuation() { return true; }
}
