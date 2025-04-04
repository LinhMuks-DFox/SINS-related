import os
import zipfile
import smtplib
import yaml
import argparse
from email.message import EmailMessage
from datetime import datetime


def load_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def unzip_all(target_dir, out_dir):
    zip_files = []
    for root, dirs, files in os.walk(target_dir):
        for file in files:
            if file.endswith(".zip"):
                zip_files.append(os.path.join(root, file))

    extracted = []
    for zip_path in zip_files:
        extract_to = out_dir
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            extracted.append(zip_path)
        except zipfile.BadZipFile:
            print(f"[ERROR] Cannot unzip: {zip_path}")
    return zip_files, extracted

# === 校验：哪些 zip 没有被成功解压 ===
def verify_unzip(all_zips, extracted_zips):
    return list(set(all_zips) - set(extracted_zips))

# === 发送邮件通知 ===
def send_mail(subject, body, config):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = config["email"]["from"]
    msg["To"] = config["email"]["to"]
    msg.set_content(body)

    with smtplib.SMTP(config["email"]["smtp_host"], config["email"]["smtp_port"]) as smtp:
        smtp.starttls()
        smtp.login(config["email"]["from"], config["email"]["password"])
        smtp.send_message(msg)

# === 主逻辑 ===
def main():
    parser = argparse.ArgumentParser(description="Batch unzip SINS data and send notification emails")
    parser.add_argument("--target", required=True, help="The directory path to unzip")
    parser.add_argument("--outdir", required=True, help="The target directory for unzipped files")
    parser.add_argument("--config", required=True, help="Path to the configuration file")
    parser.add_argument("--dry-run", action="store_true", help="Only send a test email, no unzip")
    args = parser.parse_args()

    config = load_config(args.config)

    if args.dry_run:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        subject = "[Dry Run] Email test succeeded"
        body = f"This is a dry-run email sent at {now}. No unzipping was performed."
        send_mail(subject, body, config)
        print("[INFO] Dry-run email sent.")
        return

    all_zips, extracted = unzip_all(args.target, args.outdir)
    failed = verify_unzip(all_zips, extracted)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if failed:
        subject = "[Task Failed] Some zip files failed"
        body = f"The following {len(failed)} zip files failed to unzip:\n" + "\n".join(failed)
    else:
        subject = "[Task Complete] All zip files extracted"
        body = f"All {len(extracted)} zip files were successfully unzipped."

    send_mail(subject, body, config)

if __name__ == "__main__":
    main()
