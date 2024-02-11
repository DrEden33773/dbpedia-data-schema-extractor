from local_schema_extractor import DUMP_PATH
from schema_to_csv_base import SCHEMA_EDGES_GENERAL
import os, subprocess
from tqdm.auto import tqdm
from options import hasOption


DUMPED_PREDICATES_FILE = f"{DUMP_PATH}/predicates.txt"


def pre_check():
    if not os.path.exists(DUMP_PATH):
        os.mkdir(DUMP_PATH)
    if not os.path.exists(SCHEMA_EDGES_GENERAL + ".txt"):
        raise FileNotFoundError(
            f"File `{SCHEMA_EDGES_GENERAL}.txt` does not exist, please run `LocalSchemaExtractor.exec()` first."
        )


def dump_predicates():
    if hasOption("SCHEMA_STATISTICS"):
        pre_check()
        predicates = set[str]()
        with open(SCHEMA_EDGES_GENERAL + ".txt", "r") as f:
            num_of_lines = int(
                subprocess.check_output(["wc", "-l"], stdin=f).split()[0]
            )
            f.seek(0)
            with tqdm(
                total=num_of_lines,
                desc=f"Extracting predicates from `{SCHEMA_EDGES_GENERAL}.txt`",
            ) as bar:
                for line in f:
                    predicates.add(line.strip().split()[1])
                    bar.update(1)
        with open(DUMPED_PREDICATES_FILE, "w") as f:
            with tqdm(
                total=len(predicates),
                desc=f"Dumping predicates to `{DUMPED_PREDICATES_FILE}`",
            ) as bar:
                for predicate in predicates:
                    f.write(predicate + "\n")
                    bar.update(1)
    else:
        print(
            "SCHEMA_STATISTICS is not set to True, skipping schema statistics extraction ..."
        )


if __name__ == "__main__":
    dump_predicates()
