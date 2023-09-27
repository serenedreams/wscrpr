#!/usr/bin/env python3

import multiprocessing
import asyncio
import aiohttp
import re
import time
from bs4 import BeautifulSoup

# If sufficient new successes were produced, URLs which error out will be attempted again in another run
# MIN_SUCCESS_PCT_TO_CONTINUE sets the percentage of URLs which need to succeed for this to happen
# Set to lower values for greater result consistency between runs at the cost of time and vice versa.
# Greatly diminishing returns. With reasonable settings (such as defaults) most results should be 
# found in the first few runs, leaving only the more error-prone and time consuming URLs for further
# runs which in turn are likely to produce fewer and fewer successes per run.
MIN_SUCCESS_PCT_TO_CONTINUE = .5 # Arbitrary default based on consistency and speed preference
# After the initial bulk of quick successes have been achieved, each process is expected to
# spend more time awaiting timeouts from error-prone URLs and using fewer resources
# MAX_PROCESS_COUNT sets the upper bound only, not necessarily the number of processes
# Smaller URL lists will spawn fewer processes when using defaults as seen in get_main_config()
# Effective values of 128 to 256 consistently produced faster results on 16c 32t CPU when using all 
# defaults. Lower values should still produce similar end results, but take longer to get there
# Currently set to an arbitrary limit in the middle of the range which worked well during testing
MAX_PROCESS_COUNT = min(192, multiprocessing.cpu_count() * 6) 
# Filtering results according to requirements
STRONG_DOMAINS = [".io", ".fi", ".network", ".protocol"] # List of "strong" domains (matches will be kept no matter what) 
MIN_TOTAL_MATCHES = 5 # Minimum total matches for all matched categories/keywords
MIN_CAT_MATCHES = 3 # Minimum keyword/category matches
WEAK_CATEGORIES = ["liquidity", "blockchain", "decentral", "nft"] # List of "weak" categories/keywords. 
# These matches will be removed if not enough MIN_CAT_MATCHES or not enough MIN_TOTAL_MATCHES

if __name__ == '__main__':
    URLS_FILENAME = "./list.txt"
    RESULTS_FILENAME = "results.txt"
    start_time = time.time()

# Run entire url list through child processes, sublist_length urls at a time
def main():
    urls = domainsclearedlist(URLS_FILENAME)
    config = get_main_config(len(urls))
    
    categories = open("keyword.txt").read().split("\n")
    categories = [ (x.lower(), x ) for x in categories ] 
    categories.pop()

    results = []
    num_urls_before_run = 0
    total_success = 0
    last_run_success = 0
    successes_to_continue = 0
    with multiprocessing.Pool(config['process_count']) as p:
        current_run_num = 1
        while current_run_num == 1 or (last_run_success >= successes_to_continue):
            run_start_time = time.time()
            num_results_before_run = len(results)
            num_urls_before_run = len(urls)

            print("\n*** Starting run %d ***\nTotal remaining URLs: %d, Total successes: %d, Total results: %d, Total time taken: %d" 
                    % (current_run_num, len(urls), total_success, num_results_before_run, int(time.time() - start_time)))

            run_config = get_run_config(config, urls)
            successes_to_continue = run_config['successes_to_continue']
            urls_to_skip = [] if len(urls) <= run_config['num_run_urls'] else urls[run_config['num_run_urls']:len(urls) - 1]

            process_params_list = build_process_params(run_config, urls, categories)    
            process_result_lists = p.map(run_child, process_params_list)
            
            urls = [] 
            for result_list in process_result_lists:
                for result in result_list:
                    if result.startswith("http://"):
                        urls.append(result) # Add URLs which errored out to new list for further attempts in next run
                    else:
                        results.append(result)
            # Start with skipped URLs (if any) to ensure future runs don't attempt the same URLs first
            urls = urls_to_skip + urls if urls_to_skip else urls

            last_run_success = num_urls_before_run - len(urls)
            print("\n*** Completed run %d ***\nNew successes this run: %d, New results this run: %d, Time taken for this run: %d\n" 
                    % (current_run_num, last_run_success, len(results) - num_results_before_run, int(time.time() - run_start_time)))
            total_success += last_run_success
            current_run_num += 1
        
        print("\nThe last run produced fewer successes than the required %d. No further runs will be attempted." % successes_to_continue)
        print("URLs consistently failing: %d, URLs from which successful responses were received: %d, Results: %d, Time taken: %d\n" 
                    % (len(urls), total_success, len(results), int(time.time() - start_time)))
    with open(RESULTS_FILENAME, "a") as f:
        f.write("\n".join(sorted(results, reverse=True, key=lambda result: int(result[0:result.index(",")]))))

# Attempt to set sensible defaults based on what worked well for what URL list size on test hardware and connection
# Try to split URLs evenly among processes
# Favor higher process counts and fewer URLs per process which seems to produce better results
# Prevent max urls per run from going too high and causing asyncio to fail in individual processes
# Change initial declarations and values in dictionary being returned to override defaults
def get_main_config(num_input_urls):
    max_tested_process_count = 480 # Highest value we happen to have tested, increase to allow more
    max_urls_per_run = 150000 # Values above 200k consistently produced asyncio errors
    min_process_list_length = 50 # No point spawning additional processes if input list is tiny
    # Higher values up to 2-3k might be useful for lower process counts  
    # higher process list lengths result in faster runs but fewer successes per run
    # consider increasing timeout and/or max attempts per url per run to compensate
    # if increasing this value
    max_process_list_length = 1000 # Seems to get particularly problematic above 3-5k
    num_urls = min(num_input_urls, max_urls_per_run)
    max_process_count = min(MAX_PROCESS_COUNT, max_tested_process_count)

    if num_urls / max_process_count >= max_process_list_length:
        process_count = max_process_count
        max_urls_per_run = min(max_urls_per_run, process_count * max_process_list_length)
    else:
        max_urls_per_run = min(max_urls_per_run, num_urls)
        if num_urls / max_process_count <= min_process_list_length:
            process_count = int(num_urls / min_process_list_length)
        else: 
            process_list_goal = int((max_process_list_length + min_process_list_length) / 2)
            process_count = min(max_process_count, int(num_urls / process_list_goal) + 1)

    print("Process count: %d, Max URLs per run: %d" % (process_count, max_urls_per_run))
    return {
        'process_count': process_count,
        'max_urls_per_run': max_urls_per_run,
        'total_urls': num_input_urls
    }

# Attempt to set sensible defaults for each run based on what worked well for base config values
# and any given remaining URL list size on test hardware and connection
# Change initial declarations and values in dictionary being returned to override defaults
def get_run_config(main_config, remaining_urls):
    min_timeout_boundary = 1 # Lower values produce few results regardless of other configuration
    max_timeout_boundary = 30 # Higher values seem to cause asyncio issues with large numbers of URLs
    min_attempts_boundary = 2 # At least 1 retry per url per run regardless of number of URLs
    max_attempts_boundary = 7 # Higher attempts per url per run increase memory usage per process
    min_success_boundary = 1 # Prevent further runs if no success was produced in the last one

    num_run_urls = min(main_config['max_urls_per_run'], len(remaining_urls)) # Number of URLs to attempt this run
    urls_per_process_list = int(num_run_urls / main_config['process_count']) + 1 # Evenly split URLs among processes
    # Set successes required to continue after this run as a percentage of the number of URLs being atttempted
    successes_to_continue = max(min_success_boundary, int(num_run_urls / 100 * MIN_SUCCESS_PCT_TO_CONTINUE)) 
    # Larger URL lists appear to require higher timeouts to produce meaningful successes per run
    initial_timeout = int(max(min_timeout_boundary, min(max_timeout_boundary, num_run_urls / 10000)))
    # Can afford more attempts without having processes wait idle for too long with lower timeout values
    attempts_per_url = int(max(min_attempts_boundary, min(max_attempts_boundary, (max_timeout_boundary - initial_timeout) / 4)))
    # Attempt to provide meaningful headroom for timeout increase on retries
    max_timeout = max(min_timeout_boundary, min(max_timeout_boundary, initial_timeout * (attempts_per_url / 2)))

    # Too many retries resulted in asyncio hanging for a long time with the largest URL lists, limiting to minimum
    # Might not be necessary if being run overnight and time is of no concern, 
    # but produced similar end results albeit in more (and faster) runs regardless.
    if main_config['total_urls'] > 200000:
        attempts_per_url = min_attempts_boundary 

    print("URLs being attempted: %d, Successes required to continue after this run: %d, Initial timeout: %d, Max timeout: %d\nAttempts per URL: %d, URLs per process list: %d\n" 
            % (num_run_urls, successes_to_continue, initial_timeout, max_timeout, attempts_per_url, urls_per_process_list))
    return {
        'num_run_urls': num_run_urls,
        'successes_to_continue': successes_to_continue,
        'initial_timeout': initial_timeout,
        'max_timeout': max_timeout,
        'attempts_per_url': attempts_per_url,
        'urls_per_process_list': urls_per_process_list
    }

def build_process_params(run_config, run_urls, categories):
    process_params_list = []
    i = 0
    while i < run_config['num_run_urls']:
        j = i 
        i += run_config['urls_per_process_list']
        process_params = {
            'initial_timeout': run_config['initial_timeout'],
            'max_timeout': run_config['max_timeout'],
            'attempts_per_url': run_config['attempts_per_url'],
            'categories': categories,
            'urls': run_urls[j:min(i, len(run_urls))]
        }
        process_params_list.append(process_params)
    return process_params_list

def run_child(params):
    try:    
        return asyncio.run(fetch_multiple(params))
    except:
        urls = params['urls']
        print('Error in child process run attempt. URLs will be returned to be retried in future runs.')
        return urls if urls else []

async def fetch_multiple(params):
    tasks = []
    urls = params['urls']
    timeout = params['initial_timeout']
    timeout_max = params['max_timeout']
    categories = params['categories']
    attempts_per_url = params['attempts_per_url']
    async with aiohttp.ClientSession(trust_env=True, connector=aiohttp.TCPConnector(limit=0)) as session:
        for i in range(len(urls)):
            url = urls[i]
            task = asyncio.ensure_future(fetch(url.format(i), session, categories, timeout, timeout_max, attempts_per_url)) 
            tasks.append(task)
        responses = asyncio.gather(*tasks) 
        matches = await responses
        return list(match for match in matches if match)

attempts = {}

async def fetch(url, session, categories, timeout, timeout_max, max_attempts):
    try:
        async with session.get(url,timeout=timeout, headers={'User-Agent': 'python-requests/2.22.0'}) as response:
            print("Fetching " + url)
            resp = await response.read()
            resp = cleanMe(resp).lower()
            result = ""
            total_matches = 0
            all_matched_categories = []
            for cat in categories:
                matches = re.findall("%s" % cat[0], resp)
                if matches:
                    num_matches = len(matches)
                    total_matches += num_matches
                    result += ", %s:%d" % (cat[0], num_matches)
                    all_matched_categories.append(cat[0])
            if result:
                result = str(total_matches) + ", " + url + result
                print(result)

                has_weak_cat = any(cat in all_matched_categories for cat in WEAK_CATEGORIES)
                no_strong_domain = all(tld not in url for tld in STRONG_DOMAINS)
                insufficient_matches = total_matches < MIN_TOTAL_MATCHES
                insufficient_categories = len(all_matched_categories) < MIN_CAT_MATCHES

                if has_weak_cat and no_strong_domain and insufficient_matches and insufficient_categories:
                    print("Skipping result")
                    return None
                    
                return result      
    except FileNotFoundError:
        raise FileNotFoundError 
    except:
        if url not in attempts:
            attempts[url] = 1
        else:
            attempts[url] += 1
        if attempts[url] < max_attempts:
            if attempts[url] == max_attempts - 1:
                next_attempt_timeout = timeout_max
            else:
                timeout_increase_per_retry = int((timeout_max - timeout) / (max_attempts - 1))
                next_attempt_timeout = min(timeout + (timeout_increase_per_retry * attempts[url]), timeout_max)
            return await fetch(url, session, categories, next_attempt_timeout, timeout_max, max_attempts)
        else:
            return url # returning url for further attempts in future runs

def domainsclearedlist(path):
    alldomainslist = open(path).read().split("\n")
    domainlist = []
    for i in alldomainslist:
        # if not i.endswith((".ru", ".icu", ".info", ".us", ".vip", '.shop', '.live', '.mobi', '.fun', ".life", '.blog',
        #                    '.tokyo', '.fit', '.world', '.su', '.cat', '.page', '.email', '.bar', '.monster', '.today',
        #                    ".xxx", ".solutions", ".news", ".services", ".cyou", ".expert", ".xn", ".wtf", ".xyz"
        #                    , ".com", ".ru", ".org", ".online", ".net", ".uk", ".co")):
        domainlist.append("http://" + i)
    print("TOTAL DOMAINS: {}".format(len(domainlist)))
    return list(set(domainlist))

def cleanMe(html):
    soup = BeautifulSoup(html, "html.parser")
    for script in soup(["script", "style"]):
        script.extract()
    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)
    return text

if __name__ == '__main__':
    main()