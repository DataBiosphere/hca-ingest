from collections import defaultdict

from hca_orchestration.models.entities import MetadataEntity, Subgraph
from hca_orchestration.support.typing import MetadataType


def build_subgraph_nodes(links: list[Subgraph]) -> dict[MetadataType, list[MetadataEntity]]:
    nodes: dict[MetadataType, list[MetadataEntity]] = defaultdict(list)
    subgraphs = []
    for subgraph in links:
        subgraphs.append(subgraph.links)
        nodes[MetadataType("links")].append(MetadataEntity(MetadataType("link"), subgraph.links_id))

    print(f"Hydrating subgraphs [count={len(subgraphs)}]")
    for subgraph in subgraphs:   # type: ignore
        for link in subgraph:    # type: ignore
            link_type = link["link_type"]
            if link_type == 'process_link':
                process = MetadataEntity(link["process_type"], link["process_id"])
                nodes[process.entity_type].append(process)

                for input_link in link["inputs"]:
                    input_entity = MetadataEntity(input_link["input_type"], input_link["input_id"])
                    nodes[input_entity.entity_type].append(input_entity)

                for output_link in link["outputs"]:
                    output_entity = MetadataEntity(output_link["output_type"], output_link["output_id"])
                    nodes[output_entity.entity_type].append(output_entity)

                for protocol_link in link["protocols"]:
                    protocol_entity = MetadataEntity(protocol_link["protocol_type"], protocol_link["protocol_id"])
                    nodes[protocol_entity.entity_type].append(protocol_entity)

            elif link_type == 'supplementary_file_link':
                entity = MetadataEntity(link["entity"]["entity_type"], link["entity"]["entity_id"])
                nodes[entity.entity_type].append(entity)

                for file_link in link['files']:
                    file_entity = MetadataEntity(file_link["file_type"], file_link["file_id"])
                    nodes[file_entity.entity_type].append(file_entity)
            else:
                raise Exception(f"Unknown link type {link_type} encountered")

    return nodes
