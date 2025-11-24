package org.itmo.distributed.service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class SentenceSortService {

    public static List<String> mergeSortedSentences(List<List<String>> lists) {
        List<String> result = new ArrayList<>();
        if (lists == null || lists.isEmpty()) return result;

        PriorityQueue<SentenceIterator> queue = new PriorityQueue<>(
                Comparator.comparingInt(it -> it.current.length())
        );

        for (List<String> list : lists) {
            if (list != null && !list.isEmpty()) {
                queue.add(new SentenceIterator(list.iterator()));
            }
        }

        while (!queue.isEmpty()) {
            SentenceIterator it = queue.poll();
            result.add(it.current);

            if (it.advance()) {
                queue.add(it);
            }
        }
        return result;
    }

    private static class SentenceIterator {
        final Iterator<String> iterator;
        String current;

        SentenceIterator(Iterator<String> iterator) {
            this.iterator = iterator;
            if (iterator.hasNext()) {
                this.current = iterator.next();
            }
        }

        boolean advance() {
            if (iterator.hasNext()) {
                current = iterator.next();
                return true;
            }
            return false;
        }
    }
}
