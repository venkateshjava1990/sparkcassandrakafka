package com.nisum;

import java.io.Serializable;
import java.util.Objects;

public class Word  implements Serializable {
    private String word;
    private int count;

    public String getWord() {
        return word;
    }

    @Override
    public String toString() {
        return "Word{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }

    public Word() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Word word1 = (Word) o;
        return count == word1.count &&
                Objects.equals(word, word1.word);
    }

    @Override
    public int hashCode() {
        return Objects.hash(word, count);
    }

    public Word(String word, int count) {
        this.word = word;
        this.count = count;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
