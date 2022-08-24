package com.udacity.webcrawler;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import com.udacity.webcrawler.parser.*;

public class InternalCrawler extends RecursiveAction {
    private String url;
    private int maxDepth;
    private Instant deadline;
    private Map<String, Integer> counts;
    private Set<String> visitedUrls;
    private List<Pattern> ignoredUrls;
    private Clock clock;
    private PageParserFactory parserFactory;

    public InternalCrawler(
            String url,
            int maxDepth,
            Instant deadline,
            Map<String, Integer> counts,
            Set<String> visitedUrls,
            List<Pattern> ignoredUrls,
            PageParserFactory parserFactory
    ) {
        this.url = url;
        this.maxDepth = maxDepth;
        this.deadline = deadline;
        this.counts = counts;
        this.visitedUrls = visitedUrls;
        this.ignoredUrls = ignoredUrls;
        this.parserFactory = parserFactory;
    }

    public String gerUrl() {
        return this.url;
    }
    public int getMaxDepth() {
        return this.maxDepth;
    }
    public Instant getDeadline() {
        return this.deadline;
    }
    public Map<String, Integer> getCount() {
        return this.counts;
    }
    public Set<String> getVisitedUrls() {
        return this.visitedUrls;
    }
    public List<Pattern> getIgnoredUrls() {
        return this.ignoredUrls;
    }
    public PageParserFactory getParserFactory() {
        return this.parserFactory;
    }

    @Override
    public void compute() {
        if (clock.instant().isAfter(deadline)) return;
        if (maxDepth == 0) return;
        if (visitedUrls.contains(url)) return;
        for (Pattern pattern : ignoredUrls) {
            if (pattern.matcher(url).matches()) return;
        }
        visitedUrls.add(url);
        // Parse the current url.
        PageParser.Result result = parserFactory.get(url).parse();
        Lock lock = new ReentrantLock();
        for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
            lock.lock();
            if (counts.containsKey(e.getKey())) {
                counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
            } else {
                counts.put(e.getKey(), e.getValue());
            }
            lock.unlock();
        }
        List<InternalCrawler> subCrawlers = new ArrayList<>();
        for (String link : result.getLinks()) {
            subCrawlers.add(
                    new InternalCrawler.Builder()
                            .setDeadline(deadline)
                            .setCounts(counts)
                            .setIgnoredUrls(ignoredUrls)
                            .setParserFactory(parserFactory)
                            .setUrl(link)
                            .setVisitedUrls(visitedUrls)
                            .setMaxDepth(maxDepth-1)
                            .build()
                    );
        }
        invokeAll(subCrawlers);
    }

    public static class Builder {
        private String url;
        private int maxDepth;
        private Instant deadline;
        private Map<String, Integer> counts;
        private Set<String> visitedUrls;
        private List<Pattern> ignoredUrls;
        private PageParserFactory parserFactory;

        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }
        public Builder setMaxDepth(int maxDepth) {
            this.maxDepth = maxDepth;
            return this;
        }
        public Builder setDeadline(Instant deadline) {
            this.deadline = deadline;
            return this;
        }
        public Builder setCounts(Map<String, Integer> counts) {
            this.counts = counts;
            return this;
        }
        public Builder setVisitedUrls(Set<String> visitedUrls) {
            this.visitedUrls = visitedUrls;
            return this;
        }
        public Builder setIgnoredUrls(List<Pattern> ignoredUrls) {
            this.ignoredUrls = ignoredUrls;
            return this;
        }
        public Builder setParserFactory(PageParserFactory parserFactory) {
            this.parserFactory = parserFactory;
            return this;
        }
        public InternalCrawler build() {
            return new InternalCrawler(
                    this.url,
                    this.maxDepth,
                    this.deadline,
                    this.counts,
                    this.visitedUrls,
                    this.ignoredUrls,
                    this.parserFactory
            );
        }
    }
}
