"""
# Scale

We have `78w` instances in total.

Currently, we tries to pick `10w` instances.

# Format

`(s: Instance)-[p]->(o: Instance)` is current default relationship we will consider.

By adding `INST_LITERAL_PROPERTY` in `options::OPTIONS`, we will also consider `(s: Instance)-[p]->(o: Literal)`.
"""

from local_schema_extractor import (
    pre_check,
    TypeDict,
    LPVTableDecoder,
    LPVTableEncoder,
    TypeDictDecoder,
    TypeDictEncoder,
    OUT_PATH,
    DUMP_PATH,
)
from tqdm.auto import tqdm
from env import DATASET
from main import all_satisfied
from options import hasOption
import schema_to_csv, json, os, subprocess

SPOTable = dict[str, dict[str, set[str]]]
""" `{subject(inst): {predicate: {object(inst)}}}` """
SPOEncoder = LPVTableEncoder
""" `SPOTableEncoder (toJson)` """
SPODecoder = LPVTableDecoder
""" `SPOTableDecoder (fromJson)` """

INST_SRC = f"{DATASET}/mappingbased-objects_lang=en.ttl"
TYPE_DICT_SRC = f"{DUMP_PATH}/type_dict.json"

SPO_TABLE_SERIALIZED = f"{DUMP_PATH}/spo_table.json"
SAMPLED_INSTANCES = f"{DUMP_PATH}/sampled_instances.txt"
SAMPLED_TYPE_DICT_SERIALIZED = f"{DUMP_PATH}/sampled_type_dict.json"

TYPE_ID_SERIALIZED = f"{OUT_PATH}/type_node_name_id_map.txt"
INSTANCE_ID_SERIALIZED = f"{OUT_PATH}/instance_node_name_id_map.txt"

I_NODES_CSV_FILE = f"{OUT_PATH}/instance_nodes.csv"
IT_NODES_CSV_FILE = f"{OUT_PATH}/instance_type_nodes.csv"
MIN_I_NODES_CSV_FILE = f"{OUT_PATH}/minimum_instance_nodes.csv"
II_RELATIONSHIPS_CSV_FILE = f"{OUT_PATH}/instance_instance_relationships.csv"
IT_RELATIONSHIPS_CSV_FILE = f"{OUT_PATH}/instance_type_relationships.csv"

NAMESPACE = "Instance"
UPPER_LIMIT = int(1e5)

spo_table = SPOTable()
inst_set = set[str]()
original_type_dict = TypeDict()
sampled_type_dict = TypeDict()
type_node_name_id_dict = dict[str, int]()
instance_node_name_id_dict = dict[str, int]()

num_of_ii_relationships = 0
num_of_it_relationships = 0

finished_task_name_list = [
    "Done!",
]


def build_spo_table_and_inst_set():
    """
    BUG: May lose properties of instances `iff` appeared num of instances arrived the `UPPER_LIMIT`.
    """

    global spo_table, inst_set, num_of_ii_relationships

    if all_satisfied(os.path.exists, SPO_TABLE_SERIALIZED, SAMPLED_INSTANCES):
        print(
            f"Detected existing `spo_table.json` and `sampled_instances.txt`",
        )

        print(f"Loading `spo_table` ... ", end="")
        with open(SPO_TABLE_SERIALIZED, "r") as f:
            spo_table = json.loads(f.read(), cls=SPODecoder)
        print("Done!")

        num_of_lines = int(
            subprocess.check_output(["wc", "-l", SAMPLED_INSTANCES]).split()[0]
        )
        with tqdm(total=num_of_lines, desc="Loading `inst_set`") as bar:
            with open(SAMPLED_INSTANCES, "r") as f:
                f.seek(0)
                for inst in f:
                    inst_set.add(inst.strip())
                    bar.update(1)

        return

    pre_check()

    with tqdm(total=UPPER_LIMIT, desc=f"Building spo_table from `{INST_SRC}`") as bar:
        with open(INST_SRC, "r") as f:
            for line in f:
                if len(inst_set) >= UPPER_LIMIT:
                    break
                s, p, o = [literal[1:-1] for literal in line.strip().split()[0:3]]
                if s not in spo_table:
                    spo_table[s] = {}
                    inst_set.add(s)
                    bar.update(1)
                if p not in spo_table[s]:
                    spo_table[s][p] = set()
                if o not in spo_table[s][p]:
                    num_of_ii_relationships += 1
                    spo_table[s][p].add(o)
                    inst_set.add(o)
                    bar.update(1)

    print(f"Serializing spo_table to json ... ", end="")
    with open(SPO_TABLE_SERIALIZED, "w") as f:
        f.write(json.dumps(spo_table, cls=SPOEncoder, indent=2))
    print("Done!")

    with tqdm(total=len(inst_set), desc="Serializing inst_set to txt") as bar:
        with open(SAMPLED_INSTANCES, "w") as f:
            for inst in inst_set:
                f.write(inst + "\n")
                bar.update(1)


def sample_the_type_dict():
    global original_type_dict, sampled_type_dict, inst_set, num_of_it_relationships

    if all_satisfied(os.path.exists, SAMPLED_TYPE_DICT_SERIALIZED):
        print(
            f"Detected existing `sampled_type_dict.json`, loading from it instead of rebuilding ... ",
            end="",
        )
        with open(SAMPLED_TYPE_DICT_SERIALIZED, "r") as f:
            sampled_type_dict = json.loads(f.read(), cls=TypeDictDecoder)
        print("Done!")
        return

    pre_check()

    print(
        f"Building original_type_dict from `{TYPE_DICT_SRC}` ... ",
        end="",
    )
    with open(TYPE_DICT_SRC, "r") as f:
        original_type_dict = json.loads(f.read(), cls=TypeDictDecoder)
    print("Done!")

    with tqdm(total=len(inst_set), desc="Sampling type_dict") as bar:
        for inst in inst_set:
            if inst in original_type_dict:
                sampled_type_dict[inst] = original_type_dict[inst]
                num_of_it_relationships += len(sampled_type_dict[inst])
            bar.update(1)

    print(
        f"Serializing sampled_type_dict to json ... ",
        end="",
    )
    json_data = json.dumps(sampled_type_dict, cls=TypeDictEncoder, indent=2)
    with open(SAMPLED_TYPE_DICT_SERIALIZED, "w") as f:
        f.write(json_data)
    print("Done!")


def load_type_node_name_id_dict():
    global type_node_name_id_dict
    if os.path.exists(TYPE_ID_SERIALIZED):
        with open(TYPE_ID_SERIALIZED, "r") as f:
            lines = f.readlines()
            with tqdm(
                total=len(lines),
                desc=f"Loading `type_node_name_id_dict` from `{TYPE_ID_SERIALIZED}`",
            ) as bar:
                for line in lines:
                    name, id = line.strip().split()
                    type_node_name_id_dict[name] = int(id)
                    bar.update(1)


def build_instance_node_name_id_dict():
    global instance_node_name_id_dict

    if os.path.exists(INSTANCE_ID_SERIALIZED):
        with open(INSTANCE_ID_SERIALIZED, "r") as f:
            lines = f.readlines()
            with tqdm(
                total=len(lines),
                desc=f"Loading `instance_node_name_id_dict` from `{INSTANCE_ID_SERIALIZED}`",
            ) as bar:
                for line in lines:
                    name, id = line.strip().split()
                    instance_node_name_id_dict[name] = int(id)
                    bar.update(1)
        return

    with open(INSTANCE_ID_SERIALIZED, "w") as f:
        with tqdm(
            total=len(inst_set),
            desc=f"Building `instance_node_name_id_dict` from `inst_set`",
        ) as bar:
            for cnt, name in enumerate(inst_set):
                id = cnt
                instance_node_name_id_dict[name] = id
                f.write(f"{name} {id}\n")
                bar.update(1)


def i_nodes(append_types_into_label: bool = True):
    global instance_node_name_id_dict, finished_task_name_list
    headers = [f":ID({NAMESPACE})", "Name", ":LABEL"]
    used_inst_set = (
        sampled_type_dict.keys() if hasOption("USE_TYPE_LABEL") else inst_set
    )
    with tqdm(
        total=len(used_inst_set),
        desc=f"Building `{I_NODES_CSV_FILE}`"
        + (" (with `type_labels`)" if hasOption("USE_TYPE_LABEL") else ""),
    ) as bar:
        with open(I_NODES_CSV_FILE, "w") as f:
            f.write(",".join(headers) + "\n")
            for inst in used_inst_set:
                type_labels = (
                    list(sampled_type_dict[inst])
                    if hasOption("USE_TYPE_LABEL") and inst in sampled_type_dict
                    else []
                )
                name = f'"{inst}"'
                id = instance_node_name_id_dict[inst]
                label_str = ";".join(["Instance"] + type_labels)
                row = [str(id), name, label_str]
                f.write(",".join(row) + "\n")
                bar.update(1)
    finished_task_name_list.append(f"See `instance_nodes` at: `{I_NODES_CSV_FILE}`")


def minimum_i_nodes():
    global instance_node_name_id_dict, finished_task_name_list
    used_inst_set = (
        sampled_type_dict.keys() if hasOption("USE_TYPE_LABEL") else inst_set
    )
    headers = [f":ID({NAMESPACE})"]
    with tqdm(
        total=len(used_inst_set), desc=f"Building `{MIN_I_NODES_CSV_FILE}`"
    ) as bar:
        with open(MIN_I_NODES_CSV_FILE, "w") as f:
            f.write(",".join(headers) + "\n")
            for inst in used_inst_set:
                row = [str(instance_node_name_id_dict[inst])]
                f.write(",".join(row) + "\n")
                bar.update(1)
    finished_task_name_list.append(
        f"See `minimum_instance_nodes` at: `{MIN_I_NODES_CSV_FILE}`"
    )


def it_nodes():
    global sampled_type_dict, type_node_name_id_dict, instance_node_name_id_dict, finished_task_name_list
    headers = [f":ID({NAMESPACE})", "Name", "TypeIdList"]
    length = 0
    for e in sampled_type_dict.values():
        length += len(e)
    with tqdm(
        total=length,
        desc=f"Building `{IT_NODES_CSV_FILE}`",
    ) as bar:
        with open(IT_NODES_CSV_FILE, "w") as f:
            f.write(",".join(headers) + "\n")
            for inst in sampled_type_dict.keys():
                name = f'"{inst}"'
                id = instance_node_name_id_dict[inst]
                type_id_list = list[str]()
                for ontology in sampled_type_dict[inst]:
                    type_id_list.append(str(type_node_name_id_dict[ontology]))
                    bar.update(1)
                label_str = ";".join(type_id_list)
                row = [str(id), name, label_str]
                f.write(",".join(row) + "\n")
    finished_task_name_list.append(
        f"See `instance_type_nodes` at: `{IT_NODES_CSV_FILE}`"
    )


def ii_relationships():
    """
    `(s: Instance)-[p]->(o: Instance)`'s csv builder.
    """
    global spo_table, instance_node_name_id_dict, finished_task_name_list
    RELATION_TYPE = "InstInst"
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
            f"Predicate",  # option: 1. add namespace 2. change name (e.g. `predicate_between_instances`)
        ]
    )
    with tqdm(
        total=num_of_ii_relationships,
        desc=f"Building `{II_RELATIONSHIPS_CSV_FILE}`",
    ) as bar:
        with open(II_RELATIONSHIPS_CSV_FILE, "w") as f:
            f.write(",".join(headers) + "\n")
            for s in spo_table:
                if hasOption("PICK_SAMPLED_INST_ONLY") and (
                    s not in sampled_type_dict.keys()
                ):
                    continue
                s_inst = f'"{s}"'
                s_id = instance_node_name_id_dict[s]
                for p in spo_table[s]:
                    pred = f'"{p}"'
                    TYPE = p if hasOption("USE_PRED_TYPE") else RELATION_TYPE
                    for o in spo_table[s][p]:
                        o_inst = f'"{o}"'
                        o_id = instance_node_name_id_dict[o]
                        f.write(
                            ",".join(
                                [
                                    str(s_id),
                                    str(o_id),
                                    TYPE,
                                    s_inst,
                                    o_inst,
                                ]
                                + ([] if hasOption("USE_PRED_TYPE") else [pred])
                            )
                            + "\n"
                        )
                        bar.update(1)
    finished_task_name_list.append(
        f"See `instance_instance_relationships` at: `{II_RELATIONSHIPS_CSV_FILE}`"
    )


def it_relationships():
    """
    `(s: Instance)-[p]->(o: Type)`'s csv builder.
    """
    if hasOption("USE_TYPE_LABEL"):
        print(
            "Detected `USE_TYPE_LABEL` option, `type_labels` for each `instance_node` has been appended, skipping `it_relationships_building`"
        )
        return

    global instance_node_name_id_dict, type_node_name_id_dict, finished_task_name_list
    RELATION_TYPE = "HasOntology"
    headers = [
        f":START_ID({NAMESPACE})",
        f":END_ID({schema_to_csv.NAMESPACE})",
        ":TYPE",
        "Start",
        "End",
    ]
    with tqdm(
        total=num_of_it_relationships,
        desc=f"Building `{IT_RELATIONSHIPS_CSV_FILE}`",
    ) as bar:
        with open(IT_RELATIONSHIPS_CSV_FILE, "w") as f:
            f.write(",".join(headers) + "\n")
            for i in sampled_type_dict:
                i_id = instance_node_name_id_dict[i]
                for t in sampled_type_dict[i]:
                    t_id = type_node_name_id_dict[t]
                    inst, type = f'"{i}"', f'"{t}"'
                    row = [str(i_id), str(t_id), RELATION_TYPE, inst, type]
                    f.write(",".join(row) + "\n")
                    bar.update(1)
    finished_task_name_list.append(
        f"See `instance_instance_relationships` at: `{II_RELATIONSHIPS_CSV_FILE}`"
    )


def notify_done():
    for info in finished_task_name_list:
        print(info)


def exec():
    build_spo_table_and_inst_set()
    sample_the_type_dict()
    load_type_node_name_id_dict()
    build_instance_node_name_id_dict()

    i_nodes()
    # minimum_i_nodes()
    it_nodes()
    ii_relationships()
    it_relationships()

    notify_done()


if __name__ == "__main__":
    exec()
