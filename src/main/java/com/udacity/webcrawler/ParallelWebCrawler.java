package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;

import javax.inject.Inject;
import javax.inject.Provider;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import com.udacity.webcrawler.parser.*;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final int maxDepth;
  private final List<Pattern> ignoredUrls;
  private final PageParserFactory parserFactory;

  /**
   * Start of the revised Code.
   */
  @Inject
  ParallelWebCrawler(
      Clock clock,
      PageParserFactory parserFactory,
      @Timeout Duration timeout,
      @PopularWordCount int popularWordCount,
      @TargetParallelism int threadCount,
      @MaxDepth int maxDepth,
      @IgnoredUrls List<Pattern> ignoredUrls) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.maxDepth = maxDepth;
    this.ignoredUrls = ignoredUrls;
    this.parserFactory = parserFactory;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    // Submit asynchronous tasks to the ForkJoinPool.
    Map<String, Integer> counts = new HashMap<>();
    Set<String> visitedUrls = new HashSet<>();
    Instant deadline = clock.instant().plus(timeout);
    for (String url : startingUrls) {
      InternalCrawler internalCrawler =
              new InternalCrawler.Builder()
                      .setCounts(counts)
                      .setDeadline(deadline)
                      .setMaxDepth(maxDepth)
                      .setUrl(url)
                      .setVisitedUrls(visitedUrls)
                      .setIgnoredUrls(ignoredUrls)
                      .setParserFactory(parserFactory)
                      .build();
      pool.execute(internalCrawler);
    }
    pool.shutdown();
    if (counts.isEmpty()) {
      return new CrawlResult.Builder()
              .setWordCounts(counts)
              .setUrlsVisited(visitedUrls.size())
              .build();
    }
    else {
      return new CrawlResult.Builder()
              .setWordCounts(WordCounts.sort(counts, popularWordCount))
              .setUrlsVisited(visitedUrls.size())
              .build();
    }
  }
  /**
   * End of the revised code.
   */
  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}
