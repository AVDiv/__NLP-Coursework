"""
News Article Scraper

This script implements a parallel RSS feed parser and article scraper that:
1. Reads news sources from a CSV file
2. Respects robots.txt policies
3. Scrapes articles using both RSS data and full webpage content
4. Saves results to a CSV file

The scraper uses threading for improved performance and implements proper logging.
"""

# Import required libraries for RSS parsing, web scraping, and parallel processing
import pandas as pd
import feedparser
from newspaper import Article
from urllib.robotparser import RobotFileParser
import time
from datetime import datetime
import csv
from tqdm import tqdm
import logging
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict


def setup_logging():
    """
    Configure and initialize the logging system.

    Returns:
        logging.Logger: Configured logger instance for the news scraper
    """
    logger = logging.getLogger("news_scraper")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


def get_robot_parser(domain):
    """
    Create a robot parser for the given domain.

    Args:
        domain (str): The domain to fetch robots.txt from

    Returns:
        RobotFileParser: Parser instance or None if robots.txt cannot be fetched
    """
    logger = logging.getLogger("news_scraper")
    rp = RobotFileParser()
    try:
        rp.set_url(f"https://{domain}/robots.txt")
        rp.read()
        return rp
    except Exception as e:
        logger.warning(f"Could not fetch robots.txt for {domain}: {str(e)}")
        return None


def can_fetch(robot_parser, url):
    """
    Check if a URL can be scraped according to robots.txt rules.

    Args:
        robot_parser (RobotFileParser): The parser for checking permissions
        url (str): The URL to check

    Returns:
        bool: True if the URL can be scraped, False otherwise
    """
    if robot_parser is None:
        return True
    try:
        return robot_parser.can_fetch("*", url)
    except:
        return True


def parse_date(date_str):
    """
    Parse various date string formats into datetime objects.

    Args:
        date_str (str): Date string to parse

    Returns:
        datetime: Parsed datetime object or None if parsing fails
    """
    # Try multiple date formats in order of likelihood
    if not date_str:
        return None
    try:
        # Standard SQL-style datetime format
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except:
        try:
            # Common RSS feed format with timezone
            return datetime.fromtimestamp(
                time.mktime(time.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z"))
            )
        except:
            try:
                # Alternative RSS feed format
                return datetime.fromtimestamp(
                    time.mktime(time.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z"))
                )
            except:
                return None


def scrape_article(url):
    """
    Extract content from a single article URL.

    Args:
        url (str): The article URL to scrape

    Returns:
        dict: Article data including title, authors, text, etc., or None if scraping fails
    """
    logger = logging.getLogger("news_scraper")
    try:
        article = Article(url)
        article.download()
        article.parse()
        return {
            "title": article.title,
            "authors": article.authors,
            "text": article.text,
            "publish_date": article.publish_date,
            "tags": article.tags,
            "images": list(article.images),
        }
    except Exception as e:
        logger.error(f"Failed to scrape article {url}: {str(e)}")
        return None


def process_article(entry, source, robot_parser):
    """
    Process a single RSS entry by combining RSS data with scraped webpage content.

    Args:
        entry (feedparser.FeedParserDict): RSS feed entry
        source (dict): Source information including title and domain
        robot_parser (RobotFileParser): Parser for checking robots.txt permissions

    Returns:
        dict: Combined article data or None if processing fails
    """
    logger = logging.getLogger("news_scraper")
    article_url = entry.link

    # Check robots.txt
    if not can_fetch(robot_parser, article_url):
        return None

    # Merge RSS feed data with scraped webpage content
    article_data = {
        # Basic metadata from RSS feed
        "title": entry.get("title", ""),
        # Join multiple authors with commas
        "authors": ",".join(
            author.get("name", "") for author in entry.get("authors", [])
        ),
        "published_date": parse_date(entry.get("published", "")),
        "source": source["title"],
        "url": article_url,
        "content": "",
        "tags": "",
        "images": "",
    }

    # Enhance RSS data with full webpage content
    scraped = scrape_article(article_url)
    if scraped:
        # Prefer RSS data but fall back to scraped data if missing
        if not article_data["title"]:
            article_data["title"] = scraped["title"]
        if not article_data["authors"]:
            article_data["authors"] = ",".join(scraped["authors"])
        if not article_data["published_date"]:
            article_data["published_date"] = scraped["publish_date"]
        article_data["content"] = scraped["text"]
        article_data["tags"] = ",".join(scraped["tags"])
        article_data["images"] = ",".join(scraped["images"])

        return article_data

    return None


def process_feed(source, robot_parser):
    """
    Process an entire RSS feed and extract article entries.

    Args:
        source (dict): Source information including RSS URL
        robot_parser (RobotFileParser): Parser for checking robots.txt permissions

    Returns:
        list: List of tuples containing (entry, source) pairs
    """
    articles = []
    try:
        feed = feedparser.parse(source["rss"])
        if not feed.bozo:
            for entry in feed.entries:
                if can_fetch(robot_parser, entry.link):
                    articles.append((entry, source))
        return articles
    except Exception:
        return []


def main():
    """
    Main execution function that orchestrates the scraping process:
    1. Reads source list
    2. Initializes robot parsers for each domain
    3. Processes RSS feeds in parallel
    4. Scrapes articles in parallel
    5. Saves results to CSV
    """
    logger = logging.getLogger("news_scraper")
    try:
        with tqdm(total=4, desc="Overall Progress", position=0) as main_pbar:
            # Phase 1: Load and validate source list
            logger.info("Reading source list...")
            try:
                sources_df = pd.read_csv("source_list.csv")
                main_pbar.update(1)
            except Exception as e:
                logger.error(f"Failed to read source list: {str(e)}")
                return

            # Phase 2: Initialize robot.txt parsers for each unique domain
            # Create robot parser cache
            robot_parsers = {}

            # Group sources by domain for efficient processing
            domain_sources = defaultdict(list)
            with tqdm(
                total=len(sources_df), desc="Processing sources", position=1
            ) as source_pbar:
                for _, source in sources_df.iterrows():
                    if source["is_enabled"]:
                        domain = source["domain"]
                        domain_sources[domain].append(source)
                        if domain not in robot_parsers:
                            robot_parsers[domain] = get_robot_parser(domain)
                    source_pbar.update(1)
            main_pbar.update(1)

            # Phase 3: Parallel RSS feed processing
            articles_to_process = []

            with ThreadPoolExecutor(max_workers=10) as feed_executor:
                feed_futures = []

                with tqdm(
                    total=sum(len(sources) for sources in domain_sources.values()),
                    desc="Processing feeds",
                    position=1,
                ) as feed_pbar:
                    for domain, sources in domain_sources.items():
                        for source in sources:
                            feed_futures.append(
                                feed_executor.submit(
                                    process_feed, source, robot_parsers[domain]
                                )
                            )

                    # Collect all articles to process
                    for future in as_completed(feed_futures):
                        articles_to_process.extend(future.result())
                        feed_pbar.update(1)
            main_pbar.update(1)

            # Phase 4: Parallel article scraping
            articles_data = []
            with ThreadPoolExecutor(max_workers=20) as article_executor:
                article_futures = []

                with tqdm(
                    total=len(articles_to_process),
                    desc="Processing articles",
                    position=1,
                ) as article_pbar:
                    for entry, source in articles_to_process:
                        article_futures.append(
                            article_executor.submit(
                                process_article,
                                entry,
                                source,
                                robot_parsers[source["domain"]],
                            )
                        )

                    for future in as_completed(article_futures):
                        try:
                            result = future.result()
                            if result:
                                articles_data.append(result)
                            article_pbar.update(1)
                        except Exception as e:
                            logger.error(f"Error processing article: {str(e)}")
                            article_pbar.update(1)
            main_pbar.update(1)

            # Phase 5: Save results
            logger.info("Saving articles to CSV...")
            try:
                with open("articles_raw.csv", "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(
                        f,
                        fieldnames=[
                            "title",
                            "authors",
                            "published_date",
                            "source",
                            "url",
                            "content",
                            "tags",
                            "images",
                        ],
                    )
                    writer.writeheader()
                    writer.writerows(articles_data)
                logger.info(f"Successfully saved {len(articles_data)} articles to CSV")

            except Exception as e:
                logger.error(
                    f"Failed to save articles to CSV: {str(e)}\n{traceback.format_exc()}"
                )

    except Exception as e:
        logger.critical(
            f"Critical error in main process: {str(e)}\n{traceback.format_exc()}"
        )


if __name__ == "__main__":
    # Initialize logging and start the scraping process
    logger = setup_logging()
    logger.info("Starting news scraper...")
    try:
        main()
        logger.info("Scraping completed successfully")
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}\n{traceback.format_exc()}")
