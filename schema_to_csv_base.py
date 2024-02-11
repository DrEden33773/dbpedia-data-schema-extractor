import os
from local_schema_extractor import OUTPUT_PREFIX, OUTPUT_ATTRIBUTE
from tqdm.auto import tqdm

OUT_PATH = "out"
SCHEMA_EDGES_GENERAL = f"{OUT_PATH}/{OUTPUT_PREFIX}_{OUTPUT_ATTRIBUTE}_schema_edges"
SCHEMA_VERTICES_GENERAL = (
    f"{OUT_PATH}/{OUTPUT_PREFIX}_{OUTPUT_ATTRIBUTE}_schema_vertices"
)


def convert_edges_to_csv(
    input_filename: str = f"{SCHEMA_EDGES_GENERAL}.txt",
    output_filename: str = f"{SCHEMA_EDGES_GENERAL}.csv",
):
    if not os.path.exists(input_filename):
        raise FileNotFoundError(
            f"File `{input_filename}` does not exist, please run `LocalSchemaExtractor.exec()` first."
        )
    with open(input_filename, "r") as f:
        lines = f.readlines()
    with open(output_filename, "w", newline="") as f:
        headers = ["START_TYPE", "PROPERTY_TYPE", "END_TYPE"]
        f.write(",".join(headers) + "\n")
        with tqdm(
            total=len(lines),
            desc=f"Converting `schema_edges.txt` to `schema_edges.csv`",
        ) as bar:
            for line in lines:
                spo = [f'"{e}"' for e in line.strip().split()][0:3]
                f.write(",".join(spo) + "\n")
                bar.update(1)


def convert_vertices_to_csv(
    input_filename: str = f"{SCHEMA_VERTICES_GENERAL}.txt",
    output_filename: str = f"{SCHEMA_VERTICES_GENERAL}.csv",
):
    if not os.path.exists(input_filename):
        raise FileNotFoundError(
            f"File `{input_filename}` does not exist, please run `LocalSchemaExtractor.exec()` first."
        )
    with open(input_filename, "r") as f:
        lines = f.readlines()
    with open(output_filename, "w", newline="") as f:
        headers = ["LABEL_TYPE"]
        f.write(",".join(headers) + "\n")
        with tqdm(
            total=len(lines),
            desc=f"Converting `schema_vertices.txt` to `schema_vertices.csv`",
        ) as bar:
            for line in lines:
                label = f'"{line.strip()}"'
                f.write(label + "\n")
                bar.update(1)


def exec():
    convert_vertices_to_csv()
    convert_edges_to_csv()


if __name__ == "__main__":
    exec()
