import asyncio
import threading
import queue
import json
from pathlib import Path
from decouple import config
import numpy as np
from stockx import get_all_subcategories, get_all_thirdlevel, scrape_slugs, scrape_product
import stockx
from tools import formatted_products, post_products

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)

SERVER_NUMBER = int(config('SERVER_NUMBER'))
NUM_PROCESSES = int(config('NUM_PROCESSES'))
NUM_PROCESSES=5

async def run_async_scrape_slugs(category):
    slugs = await stockx.scrape_slugs(category)
    return slugs


async def run_async_scrape_product(url, result_queue):
    product = await stockx.scrape_product(url)
    result_queue.put(product)


async def process_category(c):
    slugs = []
    for category in c.tolist():
        slugs += await run_async_scrape_slugs(category)
    return slugs


async def run():
    stockx.BASE_CONFIG["cache"] = True
    with open('results/slugs_new.json', 'r') as f1:
        slugs = np.array(json.load(f1))
    slugs = [dict(s) for s in set(frozenset(d.items()) for d in slugs)]
    slugs = ["https://www.neimanmarcus.com/en-kz" + slug_by_server_number.get('slug') for slug_by_server_number in
             slugs]
    formatted_slugs = np.array_split(slugs, len(slugs) / NUM_PROCESSES)

    for slug_parts in formatted_slugs:
        result_queue = queue.Queue()
        threads = []
        for i in slug_parts.tolist():
            thread = threading.Thread(target=run_async_scrape_product, args=(i, result_queue))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        post_products(results)
        print("All threads have completed successfully")


if __name__ == "__main__":
    asyncio.run(run())