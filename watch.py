import sys, apis, time
import abstra.forms as af
import abstra.common as ac


# Access control
auth_info = ac.get_user()
email = auth_info.email

if "@abstra.app" not in email:
    print("Watch attempt from non-Abstra user")
    af.display("This form is only available to Abstra users.")
    sys.exit(1)


ingestion_id = ac.get_query_params().get("ingestion_id")
if not ingestion_id:
    print("Watch attempt without ingestion_id")
    af.display("Missing ingestion_id query parameter.")
    sys.exit(1)


ingestion = apis.get_ingestion_by_id(ingestion_id)
while ingestion["status"] != "done":
    total = ingestion["total"]
    progress = ingestion["progress"]
    af.display_progress(progress, total, text=f"Progress: {progress}/{total}")
    time.sleep(2)
    ingestion = apis.get_ingestion_by_id(ingestion_id)


total = ingestion["total"]
af.Page().display_markdown(f"## Done!   \n### Processed: {total}").run(end_program=True)
