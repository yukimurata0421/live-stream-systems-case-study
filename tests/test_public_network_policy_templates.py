"""Generic RFC1918 exclusions are public; deployed host addresses remain forbidden."""
import importlib.util
from pathlib import Path


def test_standard_cidr_exclusions_do_not_allow_private_host_addresses():
    path=Path(__file__).parents[1]/'ops/scripts/validate_public_port.py'
    spec=importlib.util.spec_from_file_location('public_port_validator',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    generic='10.0.0.0/8 172.16.0.0/12 192.168.0.0/16'
    assert not module.PRIVATE_ADDRESS.search(module.STANDARD_PRIVATE_CIDR.sub('',generic))
    hosts=[(10,23,45,67),(172,19,1,2),(192,168,4,5),(192,168,0,0)]
    cases=['.'.join(map(str,value)) for value in hosts]
    cases.append('.'.join(map(str,(10,0,0,0)))+'/80')
    for text in cases:
        assert module.PRIVATE_ADDRESS.search(module.STANDARD_PRIVATE_CIDR.sub('',text))
