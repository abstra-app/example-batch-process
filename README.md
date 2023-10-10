# Example Batch Processing
This project example shows how to use abstra worlflows to process data in batch asynchronously.   
For this example the data is brazilian CPF and the processing is the validation if it.   

# Setup
What this project uses

## pip requirements
```sh
    pip install -r requirements.txt
```

## tables
This examples stores info on abstra tables.

The following tables are required:

```
    ingestions (
        id uuid not null
        created_at timestamp not null
        user varchar not null
        status varchar not null
        tags jsonb not null
        total int not null
        progress int nullable
    )
```