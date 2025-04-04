import os
import subprocess
from pathlib import Path
from dataclasses import dataclass
from time import sleep
from typing import List
import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description='Download files from Zenodo.')
    parser.add_argument('--dry-run', action='store_true', default=False, help='Simulate download without actual execution.')
    parser.add_argument('--nodes', type=str, default=None, help='Comma-separated list of node ids to download (e.g. 1,3,7).')
    parser.add_argument('--max-concurrent', type=int, default=5, help='Maximum number of concurrent downloads.')
    parser.add_argument('--sleep', type=int, default=3, help='Seconds to wait between polling.')

    return parser.parse_args()

MAX_CONCURRENT_DOWNLOADS = 5

RECORDS = {
    '1': '2546677',
    '2': '2547307',
    '3': '2547309',
    '4': '2555084',
    '6': '2547313',
    '7': '2547315',
    '8': '2547319',
    '9': '2555080',
    '10': '2555137',
    '11': '2558362',
    '12': '2555141',
    '13': '2555143'
}


@dataclass
class DownloadTask:
    url: str
    target_dir: str
    filename: str

    def full_path(self):
        return os.path.join(self.target_dir, self.filename)


def build_download_tasks(selected_nodes: List[str] | None) -> List[DownloadTask]:
    tasks = []

    for node_id, record_id in RECORDS.items():
        if selected_nodes and node_id not in selected_nodes:
            continue

        node_dir = f'Node{node_id}'
        Path(node_dir).mkdir(exist_ok=True)

        # zip files
        for j in range(1, 11 if int(node_id) < 9 else 10):
            if int(node_id) < 10:
                zip_name = f'Node{node_id}_audio_{j:02}.zip'
            else:
                zip_name = f'Node{node_id}_audio_{j}.zip'
            url = f'https://zenodo.org/record/{record_id}/files/{zip_name}'
            tasks.append(DownloadTask(url=url, target_dir=node_dir, filename=zip_name))

        # license.pdf
        tasks.append(DownloadTask(
            url=f'https://zenodo.org/record/{record_id}/files/license.pdf',
            target_dir=node_dir,
            filename='license.pdf'
        ))

        # readme.txt
        if node_id not in ['1', '8']:
            tasks.append(DownloadTask(
                url=f'https://zenodo.org/record/{record_id}/files/readme.txt',
                target_dir=node_dir,
                filename='readme.txt'
            ))

    return tasks


def is_downloaded(task: DownloadTask):
    path = task.full_path()
    return os.path.exists(path) and os.path.getsize(path) > 0


def download_task(task: DownloadTask, dry_run: bool):
    if dry_run:
        print(f"[MOCK] curl -L -C - -o {task.full_path()} {task.url}")
        return None
    print(f"[START] curl -L -C - -o {task.full_path()} {task.url}")
    return subprocess.Popen(['curl', '-L', '-C', '-', '-sS', '-o', task.full_path(), task.url])


def run_scheduler(tasks: List[DownloadTask], dry_run: bool, max_concurrent: int, sleep_time: int):
    active_downloads:List[tuple] = []
    idx = 0

    while idx < len(tasks) or active_downloads:
        # Fill up download pool
        while len(active_downloads) < max_concurrent and idx < len(tasks):
            task = tasks[idx]
            if is_downloaded(task):
                print(f"[SKIP] Already exists: {task.full_path()}")
                idx += 1
                continue
            proc = download_task(task, dry_run)
            active_downloads.append((proc, task))
            idx += 1

        # 检查活跃进程
        still_active = []
        for proc, task in active_downloads:
            if proc is None:
                print(f"[MOCK-DONE] {task.filename}")
            else:
                ret = proc.poll()
                if ret is None:
                    still_active.append((proc, task))  # 仍在运行
                elif ret == 0:
                    print(f"[DONE] {task.filename}")
                else:
                    print(f"[FAIL] {task.filename}, retry later.")

        active_downloads = still_active
        sleep(sleep_time)  # 避免 CPU 爆炸


if __name__ == '__main__':
    args = parse_arguments()
    selected_nodes = args.nodes.split(',') if args.nodes else None
    all_tasks = build_download_tasks(selected_nodes)
    run_scheduler(all_tasks, args.dry_run, args.max_concurrent, args.sleep)
