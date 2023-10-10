import sys, apis, abstra.forms as af, abstra.common as ac

# Access control
auth_info = ac.get_user()
email = auth_info.email

if "admin@abstra.app" != email:
    print("Ingest attempt from non-admin user")
    af.display("This workflow is only available to Abstra admins.")
    sys.exit(1)

choice = af.read_multiple_choice(
    "Choose Topic",
    [
        {"label": "Delete one", "value": "delete-one"},
        {"label": "Delete all", "value": "delete-all"},
    ],
)

if choice == "delete-all":
    count = apis.delete_all_ingestions()
    af.display(f"Deleted {count} ingestions")
    sys.exit(0)

if choice == "delete-one":
    ingestion_id = af.read("ref id")
    try:
        apis.delete_ingestion_by_id(ingestion_id)
        af.display(f"Deleted ingestion")
    except Exception as e:
        msg = f"Failed to delete ingestion {ingestion_id}"
        print(msg, e)
        af.display(msg)
    finally:
        sys.exit(0)
