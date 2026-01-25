package org.example.model;

import java.io.Serializable;

import lombok.Data;

public abstract class StreamItem implements Serializable {

    public abstract boolean isPunctuation();
}
