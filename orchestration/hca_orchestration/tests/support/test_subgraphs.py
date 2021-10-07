import pdb

from hca_orchestration.support.subgraphs import build_subgraph_nodes
from hca_orchestration.models.entities import Subgraph, MetadataEntity
from hca_orchestration.support.typing import MetadataType


def test_build_subgraph_nodes():
    s1 = Subgraph(links_id='fake_id', content={
        'describedBy': 'https://schema.humancellatlas.org/system/2.1.1/links',
        'schema_type': 'links',
        'schema_version': '2.1.1',
        'links': [
            {
                'link_type': 'supplementary_file_link',
                'entity': {
                    'entity_type': 'project',
                    'entity_id': 'fake_project_id1'
                },
                'files': [
                    {
                        'file_id': 'fake_file_id1',
                        'file_type': 'supplementary_file'
                    },
                    {
                        'file_id': 'fake_file_id2', 'file_type': 'supplementary_file'
                    }
                ]
            }
        ]
    })
    s2 = Subgraph(
        links_id='fake_links_id2',
        content={
            'describedBy': 'https://schema.humancellatlas.org/system/2.1.1/links',
            'links': [
                {
                    'inputs': [{
                        'input_id': 'fake_input1',
                        'input_type': 'sequence_file'
                    }, {
                        'input_id': 'fake_input2',
                        'input_type': 'sequence_file'
                    }],
                    'link_type': 'process_link',
                    'outputs': [{'output_id': 'fake_output1', 'output_type': 'analysis_file'},
                                {'output_id': 'fake_output2', 'output_type': 'analysis_file'}],
                    'process_id': 'fake_process_id1', 'process_type': 'analysis_process',
                    'protocols': [{'protocol_id': 'fake_protocol_id1', 'protocol_type': 'analysis_protocol'}]
                }],
            'schema_type': 'links', 'schema_version': '2.1.1'}
    )

    nodes = build_subgraph_nodes([s1, s2])
    assert nodes[MetadataType('links')] == [MetadataEntity(entity_type=MetadataType(
        'link'), entity_id='fake_id'), MetadataEntity(entity_type=MetadataType('link'), entity_id='fake_links_id2')]
    assert nodes[MetadataType('project')] == [MetadataEntity(
        entity_type=MetadataType('project'), entity_id='fake_project_id1')]
    assert nodes[MetadataType('supplementary_file')] == [MetadataEntity(entity_type=MetadataType(
        'supplementary_file'), entity_id='fake_file_id1'), MetadataEntity(entity_type=MetadataType('supplementary_file'), entity_id='fake_file_id2')]
    assert nodes[MetadataType('analysis_file')] == [MetadataEntity(entity_type=MetadataType(
        'analysis_file'), entity_id='fake_output1'), MetadataEntity(entity_type=MetadataType('analysis_file'), entity_id='fake_output2')]
