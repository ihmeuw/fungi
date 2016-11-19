""" Definition of a provda-created provenance record w.r.t. Elasticsearch.

Provda is a package for provenance tracking of data and processes, with an
emphasis on process-oriented provenance. Created by Andrew Dolgert for the
Institute for Health Metrics and Evaluation (IHME) at the University of
Washington (UW), it is available on the web:
https://stash.ihme.washington.edu/users/adolgert/repos/provda/browse.

"""

from elasticsearch_dsl import \
    Date, DocType, Integer, \
    Keyword, Mapping, Object, \
    Text, MetaField, Short

__author__ = "Vince Reuter"
__modified__ = "2016-11-16"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.esprov.provda_record"


# TODO: may need to account for fact that in provda, we have '@' prefix for:
# 1 -- message
# 2 -- timestamp
# 3 -- version
# 4 -- source_host
# 5 -- fields

# TODO: also account for:
# 1 -- fields has "enabled": False.
# 2 -- keyword fields are not analyzed.

MAPPING = Mapping("ProvdaRecord")
MAPPING.field("prov", "keyword")
MAPPING.field("@message", "text")
MAPPING.field("@timestamp", "date")
MAPPING.field("@version", "short")
MAPPING.field("document", "keyword")
MAPPING.field("instance", "keyword")
MAPPING.field("@source_host", "keyword")
MAPPING.field("host", "keyword")
MAPPING.field("port", "integer")
MAPPING.field("@fields", "object", enabled=False)


class ProvdaRecord(DocType):
    """ Representation of a single provda-created provenance record. """


    @classmethod
    def init(cls, index, using):
        """
        Add provda mapping to ES index with ProvdaRecord.init(<index_name>);
        require provision of both index and client here, unlike superclass.

        :param str | int index: ES index
        :param elasticsearch.client.Elasticsearch | str using: ES client or
            alias for one to use
        """
        super(ProvdaRecord, cls).init(index=index, using=using)


    class Meta:
        """ Flexibility and metadata handling for records. """

        all = MetaField(enabled=False)  # We don't need "catch-all" '_all'
        dynamic = MetaField("strict")   # Raise error for non-standard record,

        # Critically, we fix the mapping for records based on provda format.
        mapping = MAPPING
