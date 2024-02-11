from local_schema_extractor import LocalSchemaExtractor
from typing import Any, Callable
from options import hasOption
import schema_to_csv_base, os, schema_statistics, schema_to_csv, instance_to_csv


def all_satisfied(f: Callable[[Any], bool], *args):
    satisfied = 0
    for arg in args:
        satisfied += 1 if f(arg) else 0
    return satisfied == len(args)


def all_unsatisfied(f: Callable[[Any], bool], *args):
    satisfied = 0
    for arg in args:
        satisfied += 1 if f(arg) else 0
    return satisfied == 0


if __name__ == "__main__":
    if hasOption("LOCAL_EXTRACT"):
        if all_unsatisfied(
            os.path.exists,
            schema_to_csv_base.SCHEMA_EDGES_GENERAL + ".txt",
            schema_to_csv_base.SCHEMA_VERTICES_GENERAL + ".txt",
        ):
            LocalSchemaExtractor().exec()
        else:
            print(
                "Detected existing schema files(format=.txt), skipping schema extraction ..."
            )
        schema_statistics.dump_predicates()
        schema_to_csv.exec()
        instance_to_csv.exec()
        exit()

    print(
        "`OnlineSchemaExtractor` has been deprecated, please use `LocalSchemaExtractor` instead."
    )
