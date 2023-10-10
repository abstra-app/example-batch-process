import pathlib, re, time, json
import abstra.tables as at, abstra.common as ac

# Files - Directory to save ingested files
persistent_dir = ac.get_persistent_dir()
ingested_dir = persistent_dir / "ingested"
ingested_dir.mkdir(parents=True, exist_ok=True)


def get_queued_cpfs_csv_path(ingestion_id: str) -> pathlib.Path:
    return ingested_dir / f"{ingestion_id}-queued.csv"


def get_invalid_cpfs_csv_path(ingestion_id: str) -> pathlib.Path:
    return ingested_dir / f"{ingestion_id}-invalid.csv"


def get_valid_cpfs_csv_path(ingestion_id: str) -> pathlib.Path:
    return ingested_dir / f"{ingestion_id}-valid.csv"


def _delete_all_csvs():
    for child in ingested_dir.iterdir():
        child.unlink()


def delete_ingestion_csv(ingestion_id: str):
    paths = [
        get_queued_cpfs_csv_path(ingestion_id),
        get_invalid_cpfs_csv_path(ingestion_id),
        get_valid_cpfs_csv_path(ingestion_id),
    ]
    for path in paths:
        path.unlink()


# Tables
def create_ingestion(id: str, user: str, total: int, tags) -> str:
    at.insert(
        "ingestions",
        {
            "id": id,
            "user": user,
            "status": "queued",
            "total": total,
            "tags": json.dumps(tags),
        },
    )


def get_ingestion_by_id(ingestion_id: str) -> dict:
    return at.run(f"SELECT * FROM ingestions WHERE id = $1", [ingestion_id])[0]


def update_ingestion_running(ingestion_id: str) -> None:
    at.update_by_id("ingestions", ingestion_id, {"status": "running"})


def update_ingestion_done(ingestion_id: str) -> None:
    at.update_by_id("ingestions", ingestion_id, {"status": "done"})


def update_ingestion_progress(ingestion_id: str, progress: int) -> None:
    at.update_by_id("ingestions", ingestion_id, {"progress": progress})


def delete_all_ingestions():
    _delete_all_csvs()
    ids = at.run("DELETE FROM ingestions RETURNING id")
    return len(ids)


def delete_ingestion_by_id(ingestion_id: str):
    delete_ingestion_csv(ingestion_id)
    at.delete_by_id("ingestions", ingestion_id)


# Notifications
def notify_processing_done(ingestion_id: str, email: str):
    # Mock notification
    print(f"[for {email}] Processing ingestion {ingestion_id} done")


# External APIS
def validate(cpf: str) -> bool:
    # Simulates an external api call latency
    time.sleep(0.2)

    if not re.match(r"\d{3}\.\d{3}\.\d{3}-\d{2}", cpf):
        return False

    numbers = [int(digit) for digit in cpf if digit.isdigit()]
    if len(numbers) != 11 or len(set(numbers)) == 1:
        return False

    sum_of_products = sum(a * b for a, b in zip(numbers[0:9], range(10, 1, -1)))
    expected_digit = (sum_of_products * 10 % 11) % 10
    if numbers[9] != expected_digit:
        return False

    sum_of_products = sum(a * b for a, b in zip(numbers[0:10], range(11, 1, -1)))
    expected_digit = (sum_of_products * 10 % 11) % 10
    if numbers[10] != expected_digit:
        return False

    return True
