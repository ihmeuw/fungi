""" Tests for CLI use of function to list time-lagged computational stages. """

import json
import elasticsearch_dsl

from conftest import \
    call_cli_func, code_stage_text, make_index_name, RawAndCliValidator
from esprov import DOCTYPE_KEY
from esprov.provda_record import ProvdaRecord

__author__ = "Vince Reuter"
__modified__ = "2016-11-19"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.tests.test_list_stages"

TEST_INDEX_NAME_SUFFICES = ["ti0", "t1", "ti1"]
INDEX_NAMES = [make_index_name(name) for name in TEST_INDEX_NAME_SUFFICES]
LAG_VALUES = ["-1M", "-2w", "-3d", "-12H", "-30m"]


CODE_LOGS = """
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:gbd/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:51.927Z", "instance": "code:tests/make_history.py", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:0a16324a-0017-47e9-a727-199d1f3e0fce", "@version": 1, "port": 56339}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:gbd/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:52.471Z", "instance": "code:tests/make_history.py", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:5a4e5132-cf82-4bb1-99b6-d4a673391512", "@version": 1, "port": 56340}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:gbd/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:53.582Z", "instance": "code:tests/make_history.py", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:10924aca-9781-4b51-8dd6-25a2344e2bcc", "@version": 1, "port": 56341}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:gbd/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:54.146Z", "instance": "code:tests/make_history.py", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:b0c477d3-94c1-47e2-9393-1251c6229596", "@version": 1, "port": 56342}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:scalar/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:54.933Z", "instance": "code:tests/make_history.py", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:180c4c18-9057-40a3-8f21-da089427905d", "@version": 1, "port": 56343}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:scalar/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:55.536Z", "instance": "code:tests/make_history.py", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:d294c6dd-502d-43d5-8130-01f929c420f3", "@version": 1, "port": 56344}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:scalar/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:56.263Z", "instance": "code:tests/make_history.py", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:7c75654a-d022-4ec2-88fd-ec89f8491539", "@version": 1, "port": 56345}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:scalar/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:56.871Z", "instance": "code:tests/make_history.py", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:21e652b2-6d1b-4bb6-8fb9-35e64fd22677", "@version": 1, "port": 56346}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:scalar/first_history_test0/cvd_ihd.hdf": {}, "doc:fbd-write/fbd/model_entity": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:combined/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:57.494Z", "instance": "code:tests/make_history.py", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:72af45f0-abd5-48b5-853b-ad71af9a8393", "@version": 1, "port": 56347}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:scalar/first_history_test0/cvd_ihd.hdf": {}, "doc:fbd-write/fbd/model_entity": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:combined/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:58.312Z", "instance": "code:tests/make_history.py", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:a78da002-0922-441a-802a-18a26c9e0b6a", "@version": 1, "port": 56348}
""".splitlines(True)


DOC_LOGS = """
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:gbd/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:51.927Z", "instance": "doc:gbd-read/schema/table", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:0a16324a-0017-47e9-a727-199d1f3e0fce", "@version": 1, "port": 56339}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:gbd/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:51.927Z", "instance": "doc:gbd/first_history_test0/cvd_ihd.hdf", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:0a16324a-0017-47e9-a727-199d1f3e0fce", "@version": 1, "port": 56339}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:gbd/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:51.927Z", "instance": "doc:paf/first_history_test0/cvd_ihd.hdf", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:0a16324a-0017-47e9-a727-199d1f3e0fce", "@version": 1, "port": 56339}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:gbd/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:52.471Z", "instance": "doc:gbd-read/schema/table", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:5a4e5132-cf82-4bb1-99b6-d4a673391512", "@version": 1, "port": 56340}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:gbd/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:52.471Z", "instance": "doc:gbd/first_history_test0/cvd_ihd.hdf", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:5a4e5132-cf82-4bb1-99b6-d4a673391512", "@version": 1, "port": 56340}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:gbd/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:52.471Z", "instance": "doc:paf/first_history_test0/cvd_ihd.hdf", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:5a4e5132-cf82-4bb1-99b6-d4a673391512", "@version": 1, "port": 56340}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:gbd/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:53.581Z", "instance": "doc:gbd-read/schema/table", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:10924aca-9781-4b51-8dd6-25a2344e2bcc", "@version": 1, "port": 56341}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:gbd/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:53.581Z", "instance": "doc:gbd/first_history_test0/cvd_ihd.hdf", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:10924aca-9781-4b51-8dd6-25a2344e2bcc", "@version": 1, "port": 56341}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:gbd/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:53.582Z", "instance": "doc:paf/first_history_test0/cvd_ihd.hdf", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:10924aca-9781-4b51-8dd6-25a2344e2bcc", "@version": 1, "port": 56341}
{"@fields": {"doc:gbd-read/schema/table": {}, "doc:gbd/first_history_test0/cvd_ihd.hdf": {}, "code:tests/make_history.py": {"unk:version_remote": "https://vr24@stash.ihme.washington.edu/scm/~adolgert/provda.git", "unk:version_branch_hash": "372d74f21713f47642fc424e7e3289f38b2ed5a0", "unk:script": "/Users/vr24/code/provda/tests/make_history.py", "unk:version_branch": "tinkering"}, "doc:paf/first_history_test0/cvd_ihd.hdf": {}}, "prov": "entity", "@timestamp": "2016-11-06T10:45:54.146Z", "instance": "doc:gbd-read/schema/table", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:b0c477d3-94c1-47e2-9393-1251c6229596", "@version": 1, "port": 56342}
""".splitlines(True)


ACTIVITY_LOGS = """
{"@fields": {"is:ee768fdd-106a-4416-b837-b82d2b1432df": {"unk:process_id": {"type": "xsd:int", "$": 42767}, "unk:command": "/Users/vr24/virtualenvs/general_personal_dev_env/bin/python", "unk:group_id": {"type": "xsd:int", "$": 42724}, "unk:platform": "Darwin-15.5.0-x86_64-i386-64bit", "unk:hostname": "Gladstone.domain", "unk:interpreter": "2.7.10 (default, Oct 23 2015, 19:19:21) ", "unk:args": "--child calculate_scalars --tag first_history_test0", "unk:date": "2016-11-06T03:00:24-08:00", "unk:sge_job_id": "112"}}, "prov": "activity", "@timestamp": "2016-11-06T11:00:24.199Z", "instance": "is:ee768fdd-106a-4416-b837-b82d2b1432df", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:ee768fdd-106a-4416-b837-b82d2b1432df", "@version": 1, "port": 56842}
{"@fields": {"is:c0f21855-abdc-4277-97dd-519c9775ba8a": {"unk:process_id": {"type": "xsd:int", "$": 42917}, "unk:command": "/Users/vr24/virtualenvs/general_personal_dev_env/bin/python", "unk:group_id": {"type": "xsd:int", "$": 42917}, "unk:platform": "Darwin-15.5.0-x86_64-i386-64bit", "unk:hostname": "Gladstone.domain", "unk:interpreter": "2.7.10 (default, Oct 23 2015, 19:19:21) ", "unk:args": "--tag first_history_test --start 100", "unk:date": "2016-11-06T03:01:30-08:00", "unk:sge_job_id": "221"}}, "prov": "activity", "@timestamp": "2016-11-06T11:02:00.333Z", "instance": "is:c0f21855-abdc-4277-97dd-519c9775ba8a", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:c0f21855-abdc-4277-97dd-519c9775ba8a", "@version": 1, "port": 56983}
{"@fields": {"is:bd69d7c4-0431-4538-8e99-16f186d43c0b": {"unk:process_id": {"type": "xsd:int", "$": 43296}, "unk:command": "/Users/vr24/virtualenvs/general_personal_dev_env/bin/python", "unk:group_id": {"type": "xsd:int", "$": 42917}, "unk:platform": "Darwin-15.5.0-x86_64-i386-64bit", "unk:hostname": "Gladstone.domain", "unk:interpreter": "2.7.10 (default, Oct 23 2015, 19:19:21) ", "unk:args": "--child calculate_pafs --tag first_history_test6", "unk:date": "2016-11-06T03:01:43-08:00", "unk:sge_job_id": "400"}}, "prov": "activity", "@timestamp": "2016-11-06T11:01:43.734Z", "instance": "is:bd69d7c4-0431-4538-8e99-16f186d43c0b", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:bd69d7c4-0431-4538-8e99-16f186d43c0b", "@version": 1, "port": 56935}
{"@fields": {"is:64c85242-aa65-4aa8-9c73-79764dec2682": {"unk:process_id": {"type": "xsd:int", "$": 44109}, "unk:command": "/Users/vr24/virtualenvs/general_personal_dev_env/bin/python", "unk:group_id": {"type": "xsd:int", "$": 43551}, "unk:platform": "Darwin-15.5.0-x86_64-i386-64bit", "unk:hostname": "Gladstone.domain", "unk:interpreter": "2.7.10 (default, Oct 23 2015, 19:19:21) ", "unk:args": "--child combine_scalars --tag first_history_test8", "unk:date": "2016-11-06T03:04:56-08:00", "unk:sge_job_id": "521"}}, "prov": "activity", "@timestamp": "2016-11-06T11:04:56.369Z", "instance": "is:64c85242-aa65-4aa8-9c73-79764dec2682", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:64c85242-aa65-4aa8-9c73-79764dec2682", "@version": 1, "port": 57089}
{"@fields": {"is:aac3c43b-2cd3-42bc-9be7-db00ce2851af": {"unk:process_id": {"type": "xsd:int", "$": 62174}, "unk:command": "/Users/vr24/virtualenvs/general_personal_dev_env/bin/python", "unk:group_id": {"type": "xsd:int", "$": 61695}, "unk:platform": "Darwin-15.5.0-x86_64-i386-64bit", "unk:hostname": "Gladstone.local", "unk:interpreter": "2.7.10 (default, Oct 23 2015, 19:19:21) ", "unk:args": "--child calculate_scalars --tag mh_duplicate_for_units7", "unk:date": "2016-11-16T15:11:01-08:00", "unk:sge_job_id": "462"}}, "prov": "activity", "@timestamp": "2016-11-16T23:11:01.564Z", "instance": "is:aac3c43b-2cd3-42bc-9be7-db00ce2851af", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:aac3c43b-2cd3-42bc-9be7-db00ce2851af", "@version": 1, "port": 51950}
{"@fields": {"is:f7b975a4-084b-4eed-aa53-f765b6932386": {"unk:process_id": {"type": "xsd:int", "$": 62280}, "unk:command": "/Users/vr24/virtualenvs/general_personal_dev_env/bin/python", "unk:group_id": {"type": "xsd:int", "$": 61695}, "unk:platform": "Darwin-15.5.0-x86_64-i386-64bit", "unk:hostname": "Gladstone.local", "unk:interpreter": "2.7.10 (default, Oct 23 2015, 19:19:21) ", "unk:args": "--child calculate_pafs --tag mh_duplicate_for_units9", "unk:date": "2016-11-16T15:11:09-08:00", "unk:sge_job_id": "553"}}, "prov": "activity", "@timestamp": "2016-11-16T23:11:09.905Z", "instance": "is:f7b975a4-084b-4eed-aa53-f765b6932386", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:f7b975a4-084b-4eed-aa53-f765b6932386", "@version": 1, "port": 51967}
{"@fields": {"is:07c2b9f3-e318-4344-9d61-7c47fcfd4cca": {"unk:process_id": {"type": "xsd:int", "$": 44129}, "unk:command": "/Users/vr24/virtualenvs/general_personal_dev_env/bin/python", "unk:group_id": {"type": "xsd:int", "$": 43551}, "unk:platform": "Darwin-15.5.0-x86_64-i386-64bit", "unk:hostname": "Gladstone.domain", "unk:interpreter": "2.7.10 (default, Oct 23 2015, 19:19:21) ", "unk:args": "--child calculate_pafs --tag first_history_test9", "unk:date": "2016-11-06T03:04:57-08:00", "unk:sge_job_id": "552"}}, "prov": "activity", "@timestamp": "2016-11-06T11:04:57.503Z", "instance": "is:07c2b9f3-e318-4344-9d61-7c47fcfd4cca", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:07c2b9f3-e318-4344-9d61-7c47fcfd4cca", "@version": 1, "port": 57092}
{"@fields": {"is:863087f6-1c94-41e8-8e9b-3487f9c2fca2": {"unk:process_id": {"type": "xsd:int", "$": 42676}, "unk:command": "/Users/vr24/virtualenvs/general_personal_dev_env/bin/python", "unk:group_id": {"type": "xsd:int", "$": 42057}, "unk:platform": "Darwin-15.5.0-x86_64-i386-64bit", "unk:hostname": "Gladstone.domain", "unk:interpreter": "2.7.10 (default, Oct 23 2015, 19:19:21) ", "unk:args": "--child combine_scalars --tag first_history_test9", "unk:date": "2016-11-06T02:46:42-08:00", "unk:sge_job_id": "571"}}, "prov": "activity", "@timestamp": "2016-11-06T10:46:42.957Z", "instance": "is:863087f6-1c94-41e8-8e9b-3487f9c2fca2", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:863087f6-1c94-41e8-8e9b-3487f9c2fca2", "@version": 1, "port": 56445}
{"@fields": {"is:8b20602f-c213-4e70-805b-73805800d769": {"unk:process_id": {"type": "xsd:int", "$": 43172}, "unk:command": "/Users/vr24/virtualenvs/general_personal_dev_env/bin/python", "unk:group_id": {"type": "xsd:int", "$": 42917}, "unk:platform": "Darwin-15.5.0-x86_64-i386-64bit", "unk:hostname": "Gladstone.domain", "unk:interpreter": "2.7.10 (default, Oct 23 2015, 19:19:21) ", "unk:args": "--child calculate_pafs --tag first_history_test4", "unk:date": "2016-11-06T03:01:34-08:00", "unk:sge_job_id": "300"}}, "prov": "activity", "@timestamp": "2016-11-06T11:01:35.183Z", "instance": "is:8b20602f-c213-4e70-805b-73805800d769", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:8b20602f-c213-4e70-805b-73805800d769", "@version": 1, "port": 56915}
{"@fields": {"is:c0f21855-abdc-4277-97dd-519c9775ba8a": {"unk:process_id": {"type": "xsd:int", "$": 42917}, "unk:command": "/Users/vr24/virtualenvs/general_personal_dev_env/bin/python", "unk:group_id": {"type": "xsd:int", "$": 42917}, "unk:platform": "Darwin-15.5.0-x86_64-i386-64bit", "unk:hostname": "Gladstone.domain", "unk:interpreter": "2.7.10 (default, Oct 23 2015, 19:19:21) ", "unk:args": "--tag first_history_test --start 100", "unk:date": "2016-11-06T03:01:30-08:00", "unk:sge_job_id": "221"}}, "prov": "activity", "@timestamp": "2016-11-06T11:02:00.333Z", "instance": "is:c0f21855-abdc-4277-97dd-519c9775ba8a", "host": "127.0.0.1", "@source_host": "withme", "@message": "create_file3", "document": "is:c0f21855-abdc-4277-97dd-519c9775ba8a", "@version": 1, "port": 56983}
""".splitlines(True)



class TestListStagesBasic(RawAndCliValidator):
    """ Tests for listing recent computational stages with ES records. """


    def format_output(self, results):
        """
        Format

        :param collections.abc.Iterable results: collection of
            results to reformat
        :return str: newline-joined results, such that each stage name
            would be printed on a separate line, with trailing newline
            to reset the command-line prompt
        """
        return "{}\n".format("\n".join(results))


    def test_raw_client(self, es_client, output_format):
        """ No Index --> no results. """
        expected = set()
        observed = call_cli_func("list_stages", client=es_client)
        super(TestListStagesBasic, self).validate(expected, set(observed),
                                                  output_format)


    def test_no_data(self, es_client,
                     inserted_index_and_response, output_format):
        """ Regardless of filter(s), no data --> no results. """

        # Insert and validate Index.
        index, response = inserted_index_and_response
        assert index in es_client.indices.get_alias().keys()

        expected = set()
        observed = call_cli_func("list_stages", client=es_client)
        super(TestListStagesBasic, self).validate(expected, set(observed),
                                                  output_format)


    def test_data_no_code_stages(self, es_client):
        """ No code stages --> no results. """

        index1 = make_index_name("index1")
        ProvdaRecord.init(index=index1, using=es_client)



        # Single-index


        # Multi-index


    def test_no_filters(self):
        """ Without filtering, ALL known stage names should be returned. """
        pass


    def test_single_period_lag(self):
        """ The most basic time-lag comp. stages query is single-span. """
        pass


    def test_multi_period_lag(self):
        """ Various lag spans can be chained together. """
        pass


    def test_multi_period_lag_span_collision(self):
        """ Multiple values for the same lag time span can't be given. """
        pass


    def test_specific_index(self):
        """ Specific index within which to search may be provided. """
        pass


    def test_limit_results(self):
        """ Full results may be too verbose; they can be limited. """
        pass


    def test_limit_results_single_period_lag(self):
        pass


    def test_limit_results_multi_period_lag(self):
        pass


    def test_specific_index_single_period_lag(self):
        pass


    def test_specific_index_multi_period_lag(self):
        pass


    def test_limit_results_specific_index_no_lag(self):
        pass


    def test_limit_results_specific_index_single_lag(self):
        pass


    def test_limit_results_specific_index_multi_lag(self):
        pass



class TestListStagesCustom:
    """ Tests for nice-to-have sort of features for stage run query. """
    pass
