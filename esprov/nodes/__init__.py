"""" Constants and setup for the nodes package """


__author__ = "Vince Reuter"
__modified__ = "2016-11-07"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.ed"
__modname__ = "provda_sandbox.esprov.nodes.__init__"


# Design decision: omit prefix on attribute names for now.
# This means that clients must specify or pre-clean.
# This makes more sense anyway with a view toward configurability.



AGENT_ATTRIBUTE_NAMES = {
    "fullname",
    "homedir"
}


ACTIVITY_ATTRIBUTE_NAMES = {
    "args",
    "command",
    "date",
    "group_id",
    "hostname",
    "interpreter",
    "platform",
    "process_id",
    "sge_job_id"
}



