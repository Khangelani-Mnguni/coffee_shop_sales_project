import click
import yaml
from loader import BigQueryLoader


@click.command()
@click.option("--file", required=True, help="Path to CSV file")
@click.option("--store", required=True, help="Store name")
@click.option("--mode", default=None, help="append or truncate")
def load(file, store, mode):

    with open("ingestion/config.yaml") as f:
        config = yaml.safe_load(f)

    write_mode = mode or config["write_mode"]

    loader = BigQueryLoader(
        config["project_id"],
        config["dataset"],
        write_mode
    )

    table_name = f"raw_{store.lower().replace(' ', '_')}"

    loader.load_csv(file, table_name, store)


if __name__ == "__main__":
    load()