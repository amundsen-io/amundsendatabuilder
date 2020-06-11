class TableTagTransformer(Transformer):
    """Simple transformer that adds tags to all table nodes produced as part of a job."""
    # Config
    TAGS = "tags"
    DEFAULT_CONFIG = ConfigFactory.from_dict({TAGS: None})

    def init(self, conf):
        conf = conf.with_fallback(TableTagTransformer.DEFAULT_CONFIG)
        tags = conf.get_string(TableTagTransformer.TAGS)

        if isinstance(tags, str):
            tags = tags.split(",")
        self.tags = tags
        print("\n\n\n***** SETTING UP TABLES WITH TAGS {}".format(self.tags))

    def transform(self, record):
        if isinstance(record, TableMetadata):
            if record.tags:
                record.tags += self.tags
            else:
                record.tags = self.tags
            return record

    def get_scope(self):
        # type: () -> str
        return "transformer.tabletag"
