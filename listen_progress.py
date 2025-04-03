import os
import time
import argparse
from pathlib import Path
from humanize import naturalsize
import psutil

def count_curl_processes():
    count = 0
    for p in psutil.process_iter(['name', 'exe', 'cmdline']):
        try:
            name = p.info['name']
            exe = str(p.info['exe']) if p.info['exe'] else ''
            cmdline = ' '.join(p.info['cmdline']) if p.info['cmdline'] else ''

            if 'curl' in name or 'curl' in exe or cmdline.startswith('curl'):
                count += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return count


def scan_status(root_dir: Path):
    total_files = 0
    total_bytes = 0

    for node_dir in root_dir.glob("Node*"):
        if not node_dir.is_dir():
            continue
        for file in node_dir.glob("*.zip"):
            try:
                size = file.stat().st_size
                total_files += 1
                total_bytes += size
            except Exception:
                continue

    return total_files, total_bytes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', default='.', help='Path to SINS download root.')
    parser.add_argument('--interval', type=int, default=1, help='Seconds between scans.')

    args = parser.parse_args()
    prev_bytes = 0

    print(f"Monitoring download progress under: {args.path}")
    print("Press Ctrl+C to stop.\n")

    while True:
        try:
            total_files, total_bytes = scan_status(Path(args.path))
            # curl_count = sum(1 for p in psutil.process_iter(['name']) if p.info['name'] == 'curl')
            curl_count = count_curl_processes()
            diff = total_bytes - prev_bytes
            prev_bytes = total_bytes

            print(f"\r📦 Files: {total_files} | 💾 Size: {naturalsize(total_bytes)} | ⏱ +{naturalsize(diff)}/s | 🌀 curl: {curl_count}", end='\r', flush=True)
            time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nStopped.")
            break


if __name__ == '__main__':
    main()