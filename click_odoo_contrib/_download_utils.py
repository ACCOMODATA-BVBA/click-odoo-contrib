import requests
import threading
from concurrent.futures import ThreadPoolExecutor
import time

import logging

_logger = logging.getLogger(__name__)


def url_chunck_generator(
    url, chunk_size=20 * 1024 * 1024, num_parallel=4, max_buffer=10
):
    """
    Downloads an URL in parallel with back-pressure.
    :param max_buffer: Max number of chunks to hold in RAM before pausing downloads.
    """

    # A presigned URL is typically only signed for a single HTTP method
    # so we cannot use HEAD, but use a GET request and close the
    # connection immediately
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_size = int(r.headers.get("Content-Length", 0))

    num_parts = (total_size + chunk_size - 1) // chunk_size

    # Use a semaphore to lock on buffer MAX
    max_buffers_semaphore = threading.BoundedSemaphore(max_buffer)
    pending_parts = {}
    lock = threading.Lock()

    def download_part(part_index):
        # Wait for a free slot in the buffer (Back-pressure)
        max_buffers_semaphore.acquire()

        start = part_index * chunk_size
        end = min(start + chunk_size - 1, total_size - 1)

        response = requests.get(url, headers={"Range": f"bytes={start}-{end}"})
        response.raise_for_status()

        with lock:
            pending_parts[part_index] = response.content

    # Start the downloader threads
    with ThreadPoolExecutor(max_workers=num_parallel) as executor:
        for i in range(num_parts):
            executor.submit(download_part, i)

        for i in range(num_parts):
            # Wait for the specific part to arrive in the buffer
            start_wait = time.time()
            while True:
                with lock:
                    if i in pending_parts:
                        chunk = pending_parts.pop(i)
                        break
                time.sleep(0.1)  # Avoid burning CPU while waiting

            if time.time() - start_wait > 60:
                raise TimeoutError(f"Part {i} download timed out.")
            yield chunk

            # Part yielded! Signal the semaphore that a buffer slot is now free
            max_buffers_semaphore.release()
