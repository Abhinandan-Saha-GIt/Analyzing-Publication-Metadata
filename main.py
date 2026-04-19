import requests
import time
from multiprocessing import Pool, cpu_count
from collections import Counter

# Configuration
BASE_URL = "http://72.60.221.150:8080"
STUDENT_ID = "BMC202302"  # As specified in the snippet

def get_publication_title(student_id, filename):
    """
    Retrieves the title for a single filename.
    Includes login, retrieval, and 429 throttling handling.
    """
    # 1. Log in to get a dynamic SHA256 secret key
    try:
        login_resp = requests.post(f"{BASE_URL}/login", json={"student_id": student_id})
        if login_resp.status_code != 200:
            return None
        secret_key = login_resp.json().get("secret_key")

        # 2. Use the key to retrieve the publication title
        while True:
            payload = {"secret_key": secret_key, "filename": filename}
            resp = requests.post(f"{BASE_URL}/lookup", json=payload)
            
            if resp.status_code == 200:
                return resp.json().get("title")
            elif resp.status_code == 429:
                # 3. Handle 429 (Too Many Requests) by waiting and retrying
                time.sleep(1) 
                continue
            else:
                # Handle other errors (404, 500, etc.)
                return None
    except Exception as e:
        print(f"Error fetching {filename}: {e}")
        return None

def mapper(filename_chunk):
    """
    Map phase: Processes a chunk of filenames and returns word frequencies.
    """
    word_counts = Counter()
    for filename in filename_chunk:
        title = get_publication_title(STUDENT_ID, filename)
        if title:
            # Extract the first word (split by whitespace and take the first element)
            words = title.split()
            if words:
                first_word = words[0].strip()
                word_counts[first_word] += 1
    return word_counts

def verify_top_10(student_id, top_10_list):
    """
    Submits the final list to the server for verification.
    """
    # 1. Log in to get key
    login_resp = requests.post(f"{BASE_URL}/login", json={"student_id": student_id})
    secret_key = login_resp.json().get("secret_key")

    # 2. Submit the top_10_list
    payload = {
        "secret_key": secret_key,
        "top_10": top_10_list
    }
    resp = requests.post(f"{BASE_URL}/verify", json=payload)
    
    # 3. Print final score
    if resp.status_code == 200:
        result = resp.json()
        print("\n--- Verification Result ---")
        print(f"Score: {result.get('score')}/10")
        print(f"Message: {result.get('message')}")
    else:
        print(f"Verification failed: {resp.text}")

if __name__ == "__main__":
    # 1. Divide filenames (pub_0.txt to pub_999.txt) into chunks
    all_filenames = [f"pub_{i}.txt" for i in range(1000)]
    
    # Define number of workers and chunk size
    num_workers = cpu_count()
    chunk_size = len(all_filenames) // num_workers
    chunks = [all_filenames[i:i + chunk_size] for i in range(0, len(all_filenames), chunk_size)]

    print(f"Starting Map-Reduce with {num_workers} workers...")

    # 2. Use multiprocessing.Pool to map 'mapper' over the chunks
    with Pool(processes=num_workers) as pool:
        results = pool.map(mapper, chunks)

    # 3. Combine (Reduce) the frequency counts from all workers
    final_counts = Counter()
    for chunk_counter in results:
        final_counts.update(chunk_counter)

    # 4. Identify the Top 10 most frequent first words
    # most_common(10) returns a list of (word, count) tuples
    top_10_data = final_counts.most_common(10)
    top_10 = [item[0] for item in top_10_data]

    print(f"Top 10 words found: {top_10}")

    # 5. Call verify_top_10
    if top_10:
        verify_top_10(STUDENT_ID, top_10)
    else:
        print("Compute the top 10 words first!")