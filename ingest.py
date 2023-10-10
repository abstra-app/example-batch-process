import abstra.forms as af
import abstra.common as ac
import abstra.workflows as aw
import sys, apis, uuid, pandas as pd

# Access control
auth_info = ac.get_user()
email = auth_info.email

if "@abstra.app" not in email:
    print("Ingest attempt from non-Abstra user")
    af.display("This form is only available to Abstra users.")
    sys.exit(1)

# Register the ingest in the database
ingestion_id = str(uuid.uuid4())

# Request the CSV with the CPFs to be validated
result = (
    af.Page()
    .display_markdown(
        f"## Upload File for async processing.    \n```ref id: {ingestion_id}```"
    )
    .read_file("Upload CSV", key="csv")
    .read_tag("Tags", multiple=True, key="tags")
    .run()
)


tags = result["tags"]
csv_file = result["csv"].file
clean_csv = apis.get_queued_cpfs_csv_path(ingestion_id)

# Cleans and saves the csv file
df = pd.read_csv(csv_file)
df = df.dropna(how="all")
total = len(df)
df.to_csv(clean_csv, index=False)

# Creates the ingestion
apis.create_ingestion(ingestion_id, email, total, tags)

# Store ingestion info on the workflow
stage = aw.get_stage()
stage.assignee = email
stage["ingestion_id"] = ingestion_id

af.display_markdown(
    f"Your data is beeing processed.   \nTrack the progress [here](/watch?ingestion_id={ingestion_id})",
    end_program=True,
)
