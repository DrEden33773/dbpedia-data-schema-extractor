import os
from tqdm.auto import tqdm
from options import hasOption
from schema_to_csv_base import SCHEMA_EDGES_GENERAL, SCHEMA_VERTICES_GENERAL

OUT_PATH = "out"
RELATIONSHIP = f"{OUT_PATH}/type_type_relationships"
NODES = f"{OUT_PATH}/type_nodes"

NAMESPACE = "Type"

type_node_name_id_dict = dict[str, int]()


def tt_relationships(
    input_filename: str = f"{SCHEMA_EDGES_GENERAL}.txt",
    output_filename: str = f"{RELATIONSHIP}.csv",
    detailed: bool = True,
):
    global type_node_name_id_dict
    if not os.path.exists(input_filename):
        raise FileNotFoundError(
            f"File `{input_filename}` does not exist, please run `LocalSchemaExtractor.exec()` first."
        )
    with open(input_filename, "r") as f:
        lines = f.readlines()
    with open(output_filename, "w", newline="") as f:
        RELATION_TYPE = "TypeType"
        headers = [
            f":START_ID({NAMESPACE})",
            f":END_ID({NAMESPACE})",
            ":TYPE",
            "Start",
            "End",
        ] + (
            []
            if hasOption("USE_PRED_TYPE")
            else [
                f"Predicate",  # option: 1. add namespace 2. change name (e.g. `predicate_between_types`)
            ]
        )
        f.write(",".join(headers) + "\n")
        with tqdm(
            total=len(lines),
            desc=f"Converting `schema_edges.txt` to `type_type_relationships.csv`",
        ) as bar:
            for line in lines:
                s, p, o = line.strip().split()[0:3]
                s_id, o_id = (
                    type_node_name_id_dict[s],
                    type_node_name_id_dict[o],
                )
                s_type, pred, o_type = (
                    f'"{s}"',
                    f'"{p}"',
                    f'"{o}"',
                )
                TYPE = p if hasOption("USE_PRED_TYPE") else RELATION_TYPE
                row = [str(s_id), str(o_id), TYPE, s_type, o_type] + (
                    [] if hasOption("USE_PRED_TYPE") else [pred]
                )
                f.write(",".join(row) + "\n")
                bar.update(1)


def type_nodes(
    input_filename: str = f"{SCHEMA_VERTICES_GENERAL}.txt",
    output_filename: str = f"{NODES}.csv",
):
    global type_node_name_id_dict
    if not os.path.exists(input_filename):
        raise FileNotFoundError(
            f"File `{input_filename}` does not exist, please run `LocalSchemaExtractor.exec()` first."
        )
    with open(input_filename, "r") as f:
        lines = f.readlines()
    with open(output_filename, "w", newline="") as f:
        headers = [f":ID({NAMESPACE})", ":LABEL", "Name"]
        f.write(",".join(headers) + "\n")
        with tqdm(
            total=len(lines),
            desc=f"Converting `schema_vertices.txt` to `type_nodes.csv`",
        ) as bar:
            for line in lines:
                raw = line.strip()
                type_id = type_node_name_id_dict[raw]
                type_info = f'"{raw}"'
                row = [str(type_id), "Type", type_info]
                f.write(",".join(row) + "\n")
                bar.update(1)


def build_type_node_name_id_dict():
    global type_node_name_id_dict

    raw_type_vertices = f"{SCHEMA_VERTICES_GENERAL}.txt"
    type_node_name_id_serialized = f"{OUT_PATH}/type_node_name_id_map.txt"

    if os.path.exists(type_node_name_id_serialized):
        with open(type_node_name_id_serialized, "r") as f:
            lines = f.readlines()
            with tqdm(
                total=len(lines),
                desc=f"Loading `type_node_name_id_dict` from `{type_node_name_id_serialized}`",
            ) as bar:
                for line in lines:
                    name, id = line.strip().split()
                    type_node_name_id_dict[name] = int(id)
                    bar.update(1)
        return

    with open(raw_type_vertices, "r") as f:
        with open(type_node_name_id_serialized, "w") as f2:
            lines = f.readlines()
            with tqdm(
                total=len(lines),
                desc=f"Building `type_node_name_id_dict` from `{raw_type_vertices}`",
            ) as bar:
                for cnt, line in enumerate(lines):
                    name = line.strip()
                    id = cnt
                    type_node_name_id_dict[name] = id
                    f2.write(f"{name} {id}\n")
                    bar.update(1)


def notify_done():
    print(f"Done!")
    print(f"See `type_nodes` at: `{NODES}.csv`")
    print(f"See `type_type_relationships` at: `{RELATIONSHIP}.csv`")


def exec():
    build_type_node_name_id_dict()
    type_nodes()
    tt_relationships()
    notify_done()


if __name__ == "__main__":
    exec()
