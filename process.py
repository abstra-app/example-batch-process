import apis, pandas as pd
import abstra.workflows as aw

stage = aw.get_stage()
ingestion_id = stage["ingestion_id"]

valid_csv_path = apis.get_valid_cpfs_csv_path(ingestion_id)
queued_csv_path = apis.get_queued_cpfs_csv_path(ingestion_id)
invalid_csv_path = apis.get_invalid_cpfs_csv_path(ingestion_id)

df = pd.read_csv(queued_csv_path)
apis.update_ingestion_running(ingestion_id)

for idx, row in df.iterrows():
    cpf = row["CPF"]
    valid = apis.validate(cpf)
    df.loc[idx, "valid"] = valid
    apis.update_ingestion_progress(ingestion_id, idx + 1)

valid_df = df[df["valid"] == True]
valid_df.to_csv(valid_csv_path, index=False)

invalid_df = df[df["valid"] == False]
invalid_df.to_csv(invalid_csv_path, index=False)

apis.update_ingestion_done(ingestion_id)
apis.notify_processing_done(ingestion_id, stage.assignee)
queued_csv_path.unlink()
